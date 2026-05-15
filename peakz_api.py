#!/usr/bin/env python3
"""PeakzAPI.py — Automatische zoek- en boekingstool voor Peakz Padel via de Foys API."""
from __future__ import annotations

import argparse
import base64
import getpass
import hashlib
import json
import logging
import math
import os
import re
import smtplib
import socket
import sys
import time
import unicodedata
import uuid
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests
from cryptography.fernet import Fernet, InvalidToken

try:
    from prefect import flow, task
    from prefect.artifacts import create_markdown_artifact
    from prefect.blocks.system import Secret
    _PREFECT = True
except ImportError:
    _PREFECT = False
    def flow(_fn=None, **kw):           # type: ignore[misc]
        def deco(fn): return fn
        return deco(_fn) if _fn else deco
    def task(_fn=None, **kw):           # type: ignore[misc]
        def deco(fn): return fn
        return deco(_fn) if _fn else deco
    def create_markdown_artifact(**kw): pass  # type: ignore[misc]
    class Secret:                       # type: ignore[misc]
        @staticmethod
        def load(name): raise RuntimeError("Prefect not available")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FOYS_BASE           = "https://api.foys.io"
LOCATION_ID         = "f37fb2ae-bf24-44f1-9b81-61e6c0784840"
RESERVATION_TYPE_ID = 6
FEDERATION_ID       = "df82f4dd-fd87-4af5-9c2f-656fe1a44357"
ORG_ID              = "df82f4dd-fd87-4af5-9c2f-656fe1a44357"
ORIGIN              = "https://www.peakzpadel.nl"
BROWSER_UA          = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)
TIER_DAYS: dict[int, int] = {1: 28, 2: 28, 3: 29, 4: 31, 5: 35}
MIN_DAYS_AHEAD      = 7
DEFAULT_USERNAME     = "j.welleweerd@gmail.com"
DEFAULT_SEARCH_DAYS = ["dinsdag", "woensdag", "donderdag"]
DUTCH_DAYS: dict[str, int] = {
    "maandag": 0, "dinsdag": 1, "woensdag": 2, "donderdag": 3,
    "vrijdag": 4, "zaterdag": 5, "zondag": 6,
}
ENC_FILE_NAME       = ".foys_password.enc"
EMAIL_SENDER        = "j.welleweerd@gmail.com"
EMAIL_RECIPIENTS    = ["j.welleweerd@gmail.com", "jos@well-it.nl"]
SMTP_SERVER         = "smtp.gmail.com"
SMTP_PORT           = 587

SCRIPT_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(safe_dag: str = "onbekend", safe_tijd: str = "0000") -> logging.Logger:
    log_dir = SCRIPT_DIR / "Log"
    log_dir.mkdir(exist_ok=True)
    log_date = datetime.now().strftime("%Y-%m-%d")
    log_path = log_dir / f"PeakzAPI_{log_date}_{safe_dag}_{safe_tijd}.log"

    logger = logging.getLogger("peakz")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [DEBUG] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.debug(f"Logbestand actief: {log_path}")
    return logger

# ---------------------------------------------------------------------------
# Password management (Linux-compatible — geen Windows DPAPI)
# ---------------------------------------------------------------------------

def _derive_fernet_key(username: str) -> bytes:
    seed = f"{os.environ.get('USER', '')}{username}{socket.gethostname()}"
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def save_local_foys_password(plain: str, username: str) -> None:
    key = _derive_fernet_key(username)
    token = Fernet(key).encrypt(plain.encode("utf-8"))
    enc_path = SCRIPT_DIR / ENC_FILE_NAME
    enc_path.write_bytes(token)
    print(f"Wachtwoord lokaal opgeslagen in {enc_path}")


def load_local_foys_password(username: str) -> str | None:
    enc_path = SCRIPT_DIR / ENC_FILE_NAME
    if not enc_path.exists():
        return None
    try:
        key = _derive_fernet_key(username)
        return Fernet(key).decrypt(enc_path.read_bytes()).decode("utf-8")
    except (InvalidToken, Exception):
        return None


def resolve_foys_password(password: str, username: str) -> str | None:
    if password:
        return password
    if _PREFECT:
        try:
            return Secret.load("foys-password").get()
        except Exception:
            pass
    env_pw = os.environ.get("FOYS_PASSWORD")
    if env_pw:
        return env_pw
    local_pw = load_local_foys_password(username)
    if local_pw:
        return local_pw
    try:
        plain = getpass.getpass("Voer je FOYS wachtwoord in: ")
        if plain:
            save_local_foys_password(plain, username)
            return plain
    except (KeyboardInterrupt, EOFError):
        pass
    return None

# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def get_doel_datum(dag_naam: str, max_days_ahead: int, logger: logging.Logger) -> date | None:
    target_wd = DUTCH_DAYS.get(dag_naam.lower())
    if target_wd is None:
        logger.error(f"Onbekende dag: {dag_naam}")
        return None
    candidate = date.today() + timedelta(days=max_days_ahead)
    while candidate.weekday() != target_wd:
        candidate -= timedelta(days=1)
    logger.debug(
        f"Get-DoelDatum return: {candidate.strftime('%d-%m-%Y')} 00:00:00 (ISO: {candidate.isoformat()})"
    )
    return candidate

# ---------------------------------------------------------------------------
# Slot filtering helpers
# ---------------------------------------------------------------------------

def _combined_text(item: dict) -> str:
    return " ".join(str(item.get(k, "")) for k in ("name", "type", "inventoryItemType")).lower()


def get_baan_type(item: dict) -> str:
    text = _combined_text(item)
    if re.search(r"outdoor|outside|buiten", text):
        return "buiten"
    if re.search(r"indoor|inside|binnen", text):
        return "binnen"
    return "onbekend"


def is_padel_baan(item: dict) -> bool:
    text = _combined_text(item)
    if re.search(r"beachvolley|volleybal", text):
        return False
    if "single court" in text:
        return False
    if "padel" in text or "double court" in text:
        return True
    return False


def get_plain_text(text: str) -> str:
    cleaned = "".join(c for c in text if unicodedata.category(c) not in ("So", "Cs"))
    return re.sub(r" +", " ", cleaned).strip()


def _parse_start(s: str) -> datetime:
    return datetime.strptime(s.rstrip("Z").split("+")[0], "%Y-%m-%dT%H:%M:%S")


def _baan_nr(naam: str) -> int:
    m = re.search(r"Baan\s+(\d+)", naam, re.IGNORECASE)
    return int(m.group(1)) if m else 0


def _fmt_dt(dt: datetime) -> str:
    return f"{dt.day}-{dt.month}-{dt.year} {dt.strftime('%H:%M:%S')}"

# ---------------------------------------------------------------------------
# Shared API headers
# ---------------------------------------------------------------------------

def _public_headers(referer_date: str = "") -> dict:
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "nl,en;q=0.9,nl-NL;q=0.8,en-NL;q=0.7,en-US;q=0.6",
        "User-Agent": BROWSER_UA,
        "Origin": ORIGIN,
        "Referer": (
            f"{ORIGIN}/reserveren/court-booking/reservation"
            f"?daypart=---&date={referer_date}&location=Zeehaenkade"
        ),
        "X-FederationId": FEDERATION_ID,
        "X-OrganisationId": ORG_ID,
    }


def _auth_headers(token: str, booking_id: str = "") -> dict:
    h = _public_headers()
    h["Authorization"] = f"Bearer {token}"
    if booking_id:
        h["Referer"] = f"{ORIGIN}/reserveren/court-booking/booking/{booking_id}"
    return h

# ---------------------------------------------------------------------------
# API tasks
# ---------------------------------------------------------------------------

@task(name="Zoek beschikbare slots", retries=2, retry_delay_seconds=30)
def search_available_slots(zoek_datum: date, speeltijd: int, baantype: str) -> list[dict] | None:
    logger = logging.getLogger("peakz")
    date_str = zoek_datum.strftime("%Y-%m-%dT00:00:00.000Z")
    logger.debug(f"LocationId: {LOCATION_ID}")
    logger.debug(f"ReservationTypeId: {RESERVATION_TYPE_ID}")
    logger.debug(f"dateStr: {date_str}")
    logger.debug(f"speeltijd: {speeltijd}")
    logger.debug(f"baantype: {baantype}")

    url = f"{FOYS_BASE}/court-booking/public/api/v1/locations/search"
    params = [
        ("reservationTypeId", RESERVATION_TYPE_ID),
        ("locationId", LOCATION_ID),
        ("playingTimes[]", 60),
        ("playingTimes[]", 90),
        ("playingTimes[]", 120),
        ("date", date_str),
    ]
    logger.debug(
        f"API URL: {url}?reservationTypeId={RESERVATION_TYPE_ID}"
        f"&locationId={LOCATION_ID}&playingTimes[]=60&playingTimes[]=90"
        f"&playingTimes[]=120&date={date_str}"
    )

    try:
        resp = requests.get(url, params=params, headers=_public_headers(zoek_datum.isoformat()), timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"[FOUT] Slots ophalen mislukt: {e}")
        return None

    items_list = data if isinstance(data, list) else [data]
    logger.debug(f"API response count: {len(items_list)}")

    slots: list[dict] = []
    for location in items_list:
        for item in location.get("inventoryItemsTimeSlots", []):
            if not is_padel_baan(item):
                continue
            baan_type = get_baan_type(item)
            baan_naam = get_plain_text(str(item.get("name", "")))
            inv_id = item.get("id") or item.get("inventoryItemId")
            for ts in item.get("timeSlots", []):
                slots.append({
                    "startTime":     ts.get("startTime", ""),
                    "endTime":       ts.get("endTime", ""),
                    "price":         ts.get("price", 0),
                    "isAvailable":   ts.get("isAvailable", False),
                    "duration":      ts.get("playingTime") or ts.get("duration") or ts.get("playingTimeMinutes"),
                    "baanType":      baan_type,
                    "baanNaam":      baan_naam,
                    "inventoryItemId": int(inv_id) if inv_id is not None else None,
                })

    logger.debug(f"Slot candidates gevonden: {len(slots)}")
    filtered = [
        s for s in slots
        if s["isAvailable"]
        and str(s["duration"]) == str(speeltijd)
        and (baantype == "alle" or s["baanType"] == baantype)
    ]
    logger.debug(f"Gefilterde slots (beschikbaar + duur): {len(filtered)}")
    return filtered


@task(name="Foys inloggen", retries=1, retry_delay_seconds=10)
def get_foys_token(username: str, password: str) -> str:
    logger = logging.getLogger("peakz")
    url = f"{FOYS_BASE}/foys/api/v1/token"
    resp = requests.post(
        url,
        data={"grant_type": "password", "username": username, "password": password, "federationId": FEDERATION_ID},
        timeout=30,
    )
    try:
        resp.raise_for_status()
    except Exception:
        logger.error(f"[FOUT] Inloggen mislukt: HTTP {resp.status_code} — {resp.text[:200]}")
        raise
    return resp.json()["access_token"]


@task(name="Boeking aanmaken", retries=1, retry_delay_seconds=10)
def create_foys_booking(token: str, inventory_item_id: int, start_local_str: str, speeltijd: int) -> dict:
    logger = logging.getLogger("peakz")
    url = f"{FOYS_BASE}/court-booking/members/api/v1/bookings"
    start_dt = datetime.fromisoformat(start_local_str)
    end_dt = start_dt + timedelta(minutes=speeltijd)
    body = {
        "reservationTypeId": RESERVATION_TYPE_ID,
        "startDateTime": start_dt.strftime("%Y-%m-%dT%H:%M"),
        "endDateTime":   end_dt.strftime("%Y-%m-%dT%H:%M"),
        "reservations":  [{"inventoryItemId": inventory_item_id}],
    }
    headers = _auth_headers(token)
    headers["X-Idempotency-Key"] = str(uuid.uuid4())
    headers["Content-Type"] = "application/json"
    resp = requests.post(url, json=body, headers=headers, timeout=30)
    try:
        resp.raise_for_status()
    except Exception:
        logger.error(f"[FOUT] Boeking aanmaken mislukt: HTTP {resp.status_code} — {resp.text[:200]}")
        raise
    return resp.json()


@task(name="Reservering type instellen")
def set_booking_reservation_type(token: str, booking_id: str, new_type: str) -> None:
    logger = logging.getLogger("peakz")
    url = f"{FOYS_BASE}/court-booking/members/api/v1/bookings/{booking_id}/reservation-type"
    headers = _auth_headers(token, booking_id)
    headers["Content-Type"] = "application/json"
    resp = requests.put(url, json={"newReservationType": new_type}, headers=headers, timeout=30)
    try:
        resp.raise_for_status()
    except Exception:
        logger.error(f"[FOUT] Set reservation type mislukt: HTTP {resp.status_code} — {resp.text[:200]}")
        raise


@task(name="Betalen met credits")
def pay_booking_with_credits(token: str, booking_id: str) -> dict:
    logger = logging.getLogger("peakz")
    url = f"{FOYS_BASE}/court-booking/members/api/v1/bookings/{booking_id}/pay/credits"
    headers = _auth_headers(token, booking_id)
    headers["Content-Type"] = "application/json"
    resp = requests.post(url, json={}, headers=headers, timeout=30)
    try:
        resp.raise_for_status()
    except Exception:
        logger.error(f"[FOUT] Betalen mislukt: HTTP {resp.status_code} — {resp.text[:200]}")
        raise
    pay_resp = resp.json() if resp.content else {}
    logger.debug(f"Pay response: {json.dumps(pay_resp, default=str)}")
    return pay_resp


@task(name="Fallback BookingId ophalen")
def get_booking_id_fallback(token: str, inventory_item_id: int, start_utc_iso: str) -> str | None:
    logger = logging.getLogger("peakz")
    paths = [
        "/court-booking/members/api/v1/bookings/future",
        "/court-booking/members/api/v1/bookings",
    ]
    headers = _auth_headers(token)
    start_utc = datetime.strptime(start_utc_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    for path in paths:
        try:
            resp = requests.get(f"{FOYS_BASE}{path}", headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            bookings = data if isinstance(data, list) else (
                data.get("items") or data.get("data") or []
            )
            for b in bookings:
                # Match inventoryItemId (either top-level or in reservations list)
                top_inv = str(b.get("inventoryItemId", ""))
                res_match = any(
                    str(r.get("inventoryItemId", "")) == str(inventory_item_id)
                    for r in b.get("reservations", [])
                )
                if top_inv != str(inventory_item_id) and not res_match:
                    continue
                raw_start = (
                    b.get("startTime") or b.get("beginTime") or b.get("startDateTime") or ""
                )
                if not raw_start:
                    continue
                try:
                    b_start = datetime.strptime(
                        raw_start.rstrip("Z").split("+")[0], "%Y-%m-%dT%H:%M:%S"
                    ).replace(tzinfo=timezone.utc)
                    if abs((b_start - start_utc).total_seconds()) <= 120:
                        bid = b.get("id") or b.get("bookingId") or b.get("guid")
                        if bid:
                            logger.debug(f"Fallback BookingId gevonden via {path}: {bid}")
                            return str(bid)
                except (ValueError, AttributeError):
                    continue
        except Exception as e:
            logger.debug(f"Fallback path {path} mislukt: {e}")
    return None

# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def send_error_mail(onderwerp: str, bericht: str, mail: bool, logger: logging.Logger) -> None:
    if not mail:
        return
    smtp_pw = os.environ.get("PEAKZ_SMTP_PASSWORD")
    if not smtp_pw and _PREFECT:
        try:
            smtp_pw = Secret.load("peakz-smtp-password").get()
        except Exception:
            pass
    if not smtp_pw:
        logger.warning("[WAARSCHUWING] PEAKZ_SMTP_PASSWORD niet ingesteld, mail niet verzonden.")
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = ", ".join(EMAIL_RECIPIENTS)
        msg["Subject"] = f"[FOUT] {onderwerp}"
        msg.attach(MIMEText(bericht, "plain", "utf-8"))
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.starttls()
            smtp.login(EMAIL_SENDER, smtp_pw)
            smtp.sendmail(EMAIL_SENDER, EMAIL_RECIPIENTS, msg.as_string())
    except Exception as e:
        logger.warning(f"[WAARSCHUWING] Foutmail verzenden mislukt: {e}")

# ---------------------------------------------------------------------------
# Midnight wait
# ---------------------------------------------------------------------------

def wait_for_midnight(logger: logging.Logger) -> None:
    now = datetime.now()
    midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
    wait_secs = math.ceil((midnight - now).total_seconds())
    print(f"Gestart om {now.strftime('%H:%M:%S')} — wacht tot middernacht ({wait_secs} seconden)...")
    time.sleep(wait_secs)
    print("Middernacht bereikt, zoeken begint nu.")

# ---------------------------------------------------------------------------
# Helper: book + pay flow (used twice: main path and fallback)
# ---------------------------------------------------------------------------

def _set_and_pay(token: str, booking_id: str, logger: logging.Logger) -> dict:
    set_booking_reservation_type(token, booking_id, "SplitReservation")
    logger.debug("Reservation type gezet naar SplitReservation")
    pay_resp = pay_booking_with_credits(token, booking_id)
    return pay_resp


def _print_betaald(booking_id: str, pay_resp: dict) -> None:
    betaald = (
        pay_resp.get("amountPaid") or pay_resp.get("amount") or
        pay_resp.get("creditsUsed") or pay_resp.get("totalAmount")
    )
    print(f"Boeking aangemaakt en betaald met credits. BookingId: {booking_id}")
    if betaald is not None:
        print(f"Betaald (jouw aandeel): {float(betaald):.2f}")
    else:
        print("Betaald bedrag: zie Peakz app (SplitReservation actief)")
    return betaald

# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="PeakzAPI Boeking", log_prints=True)
def peakz_boeking(
    speeltijd: str = "90",
    tijdstip: str = "19:00",
    invoer_dag: str = "",
    baantype: str = "alle",
    boek: bool = False,
    dryrun: bool = False,
    username: str = DEFAULT_USERNAME,
    password: str = "",
    mail: bool = False,
    tijdstip_alternatief: list[str] | None = None,
    tier: int = 5,
    wacht_middernacht: bool = False,
) -> None:

    safe_dag  = re.sub(r"[^a-zA-Z0-9_-]", "", invoer_dag).lower() or "onbekend"
    safe_tijd = re.sub(r"\D", "", tijdstip).zfill(4) or "0000"
    logger = setup_logging(safe_dag, safe_tijd)

    if not re.match(r"^\d+$", speeltijd):
        print(f"[FOUT] speeltijd moet een getal zijn: {speeltijd}")
        sys.exit(1)

    print("Peakz Padel - Automatische boeking v2.0")
    print("=" * 40)

    zoek_dagen = [invoer_dag] if invoer_dag else DEFAULT_SEARCH_DAYS
    logger.debug(f"Zoekdagen: {', '.join(zoek_dagen)}")

    if wacht_middernacht:
        wait_for_midnight(logger)

    # Time priority list
    base_time = datetime.strptime(tijdstip, "%H:%M")
    if tijdstip_alternatief:
        alts = tijdstip_alternatief
        if len(alts) == 1 and "," in alts[0]:
            alts = [t.strip() for t in alts[0].split(",")]
        tijd_prioriteit = [tijdstip] + alts
    else:
        m30 = (base_time - timedelta(minutes=30)).strftime("%H:%M")
        p30 = (base_time + timedelta(minutes=30)).strftime("%H:%M")
        tijd_prioriteit = [tijdstip, m30, p30]
    logger.debug(f"Tijdprioriteit: {' > '.join(tijd_prioriteit)}")

    tiers_fallback = [tier, 4] if tier > 4 else [tier]
    geboekt = False

    for active_tier in tiers_fallback:
        max_days_ahead = TIER_DAYS[active_tier]
        if active_tier != tier:
            print(
                f"Geen slots gevonden voor tier {tier} — "
                f"terugvallen op tier {active_tier} ({max_days_ahead} dagen vooruit)."
            )
        logger.debug(f"MaxDaysAhead: {max_days_ahead} (tier {active_tier})")

        slot_gevonden = False

        for dag in zoek_dagen:
            zoek_datum = get_doel_datum(dag, max_days_ahead, logger)
            if zoek_datum is None:
                continue

            print(f"Zoekdatum: {zoek_datum.strftime('%d-%m-%Y')} ({dag} binnen {max_days_ahead} dagen)")
            logger.debug(f"zoekDatum ISO: {zoek_datum.isoformat()}")

            days_until = (zoek_datum - date.today()).days
            if boek and not dryrun and days_until < MIN_DAYS_AHEAD:
                print(
                    f"[GEBLOKKEERD] Doeldatum {zoek_datum.strftime('%d-%m-%Y')} is slechts "
                    f"{days_until} dag(en) vooruit. Minimaal {MIN_DAYS_AHEAD} dagen vereist voor boeking."
                )
                continue

            alle_slots = search_available_slots(zoek_datum, int(speeltijd), baantype)
            if alle_slots is None:
                send_error_mail(f"API fout {dag}", f"Slots ophalen mislukt voor {dag} op {zoek_datum}", mail, logger)
                continue

            # Match against time priority
            beschikbaar: list[dict] = []
            gebruikt_tijdstip: str | None = None
            for t in tijd_prioriteit:
                matches = [s for s in alle_slots if _parse_start(s["startTime"]).strftime("%H:%M") == t]
                if matches:
                    beschikbaar = matches
                    gebruikt_tijdstip = t
                    break

            if not beschikbaar:
                print(f"Geen beschikbare slots gevonden voor {dag}.")
                continue

            if gebruikt_tijdstip != tijdstip:
                print(f"Voorkeurstijdstip {tijdstip} niet beschikbaar, alternatief gebruikt: {gebruikt_tijdstip}")

            slot_gevonden = True
            print(f"Beschikbare slots gevonden om {gebruikt_tijdstip} op {dag}.")
            for s in beschikbaar:
                start = _parse_start(s["startTime"])
                end   = _parse_start(s["endTime"])
                print(
                    f"- Slot: {_fmt_dt(start)} t/m {_fmt_dt(end)} "
                    f"({s['duration']} min, baanprijs totaal {s['price']}) "
                    f"[{s['baanType']}] {s['baanNaam']} |"
                )

            if not boek:
                continue

            try:
                doel_slot = sorted(
                    beschikbaar,
                    key=lambda s: (_parse_start(s["startTime"]), -_baan_nr(s["baanNaam"])),
                )[0]

                start_local_dt  = _parse_start(doel_slot["startTime"])
                start_local_str = start_local_dt.strftime("%Y-%m-%dT%H:%M")
                start_utc_iso   = (
                    start_local_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                )

                logger.debug(f"Gekozen slot voor boeking: {doel_slot['startTime']} op {doel_slot['baanNaam']}")

                if dryrun:
                    print("DRY-RUN: geen boeking uitgevoerd.")
                    logger.debug(
                        f"DRYRUN locationId={LOCATION_ID}; inventoryItemId={doel_slot['inventoryItemId']}; "
                        f"startTimeUtc={start_utc_iso}; startTimeLocal={start_local_str}; "
                        f"playingTime={speeltijd}; reservationTypeId={RESERVATION_TYPE_ID}"
                    )
                    geboekt = True
                    break

                resolved_pw = resolve_foys_password(password, username)
                if not resolved_pw:
                    print("[FOUT] Geen bruikbaar wachtwoord beschikbaar voor --boek.")
                    sys.exit(1)

                logger.debug("Start boeking via API...")
                token = get_foys_token(username, resolved_pw)

                booking_resp = create_foys_booking(
                    token, doel_slot["inventoryItemId"], start_local_str, int(speeltijd)
                )

                booking_id = (
                    booking_resp.get("id") or
                    booking_resp.get("bookingId") or
                    booking_resp.get("guid") or
                    (booking_resp.get("reservations") or [{}])[0].get("id")
                )

                if not booking_id:
                    logger.debug("Geen BookingId in create-response, probeer fallback lookup...")
                    booking_id = get_booking_id_fallback(
                        token, doel_slot["inventoryItemId"], start_utc_iso
                    )

                if booking_id:
                    logger.debug(f"Booking identifier gevonden: {booking_id}")
                    try:
                        pay_resp = _set_and_pay(token, booking_id, logger)
                        betaald  = _print_betaald(booking_id, pay_resp)

                        # Prefect artifact
                        end_dt = start_local_dt + timedelta(minutes=int(speeltijd))
                        md = (
                            "## Boeking bevestigd\n\n"
                            "| Veld | Waarde |\n|------|--------|\n"
                            f"| Datum | {start_local_dt.strftime('%d-%m-%Y')} |\n"
                            f"| Tijd | {start_local_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')} |\n"
                            f"| Baan | {doel_slot['baanNaam']} ({doel_slot['baanType']}) |\n"
                        )
                        if betaald is not None:
                            md += f"| Betaald | €{float(betaald):.2f} |\n"
                        md += f"| BookingId | {booking_id} |\n"
                        try:
                            create_markdown_artifact(
                                key="boeking-bevestigd",
                                markdown=md,
                                description="Padel boeking bevestiging",
                            )
                        except Exception:
                            pass

                    except Exception:
                        logger.debug("Eerste payment-call mislukt, probeer fallback lookup en retry...")
                        fallback_id = get_booking_id_fallback(
                            token, doel_slot["inventoryItemId"], start_utc_iso
                        )
                        if fallback_id and fallback_id != booking_id:
                            logger.debug(f"Retry met fallback booking identifier: {fallback_id}")
                            pay_resp = _set_and_pay(token, fallback_id, logger)
                            logger.debug("Reservation type gezet naar SplitReservation (fallbackId)")
                            _print_betaald(fallback_id, pay_resp)
                        else:
                            raise
                else:
                    msg = (
                        f"Boeking response ontvangen maar geen BookingId gevonden voor "
                        f"{doel_slot['baanNaam']} op {start_local_dt.strftime('%d-%m-%Y %H:%M')}."
                    )
                    print(f"Waarschuwing: {msg}")
                    print(json.dumps(booking_resp, indent=2, default=str))
                    send_error_mail("Geen BookingId na boeking", msg, mail, logger)

                geboekt = True
                break

            except Exception as e:
                msg = f"Boeking via API mislukt voor {dag} op {zoek_datum.strftime('%d-%m-%Y')}: {e}"
                print(f"[FOUT] {msg}")
                send_error_mail(f"Boekingsfout {dag}", msg, mail, logger)

        if geboekt or slot_gevonden:
            break

    print("Script voltooid!")

# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Peakz Padel - Automatische zoek- en boekingstool via de Foys API"
    )
    p.add_argument("--speeltijd", default="90")
    p.add_argument("--tijdstip", default="19:00")
    p.add_argument("--invoerDag", default="")
    p.add_argument("--baantype", default="alle", choices=["binnen", "buiten", "alle"])
    p.add_argument("--boek", action="store_true")
    p.add_argument("--dryrun", action="store_true")
    p.add_argument("--username", default=DEFAULT_USERNAME)
    p.add_argument("--password", default="")
    p.add_argument("--mail", action="store_true")
    p.add_argument("--tijdstipAlternatief", nargs="*", default=[])
    p.add_argument("--tier", type=int, default=5, choices=range(1, 6), metavar="TIER")
    p.add_argument(
        "--wachtMiddernacht", "--waitUntilMidnight",
        dest="wachtMiddernacht",
        action="store_true",
        help="Wacht tot middernacht (00:00:00) voordat het zoeken/boeken start.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    peakz_boeking(
        speeltijd=args.speeltijd,
        tijdstip=args.tijdstip,
        invoer_dag=args.invoerDag,
        baantype=args.baantype,
        boek=args.boek,
        dryrun=args.dryrun,
        username=args.username,
        password=args.password,
        mail=args.mail,
        tijdstip_alternatief=args.tijdstipAlternatief or [],
        tier=args.tier,
        wacht_middernacht=args.wachtMiddernacht,
    )
