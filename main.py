import os
import re
import json
import html as html_lib
import logging
import time
from datetime import datetime
from flask import Flask, request, abort
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

BOT_TOKEN               = os.environ["TELEGRAM_BOT_TOKEN"]
WEBHOOK_URL             = os.environ["WEBHOOK_URL"]
OWNER_ID                = int(os.environ["OWNER_TELEGRAM_ID"])
SHEETS_ID               = os.environ["GOOGLE_SHEETS_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

QUALIFY_USER_IDS        = [514275093, 5028786313]
NOTIFY_GROUP_ID         = -5160536788

API_BASE                = f"https://api.telegram.org/bot{BOT_TOKEN}"
SCOPES                  = ["https://www.googleapis.com/auth/spreadsheets"]
REMINDER_INTERVAL_MIN   = 30
DATA_START_ROW          = 4   # first data row in the sheet (1-indexed)

# chat_id -> {'lead_id': str, 'qualified': bool}
pending_reason: dict = {}


# ─── Google Sheets helpers ────────────────────────────────────────────────────

def get_sheets():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS_JSON), scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()


def sheets_call(fn, retries=3):
    """Exponential-backoff retry wrapper for any Sheets API call."""
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            logger.warning("[Sheets] Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                raise


# ─── Sheet schema
#   A  = lead_id  (YYYYMMDDHHMMSS, unique key)
#   B  = date
#   C  = name
#   D  = phone
#   E  = reason   (filled on qualification)
#   F  = status   ("PENDING|<iso_datetime>|<reminder_count>"  or  "DONE")


def sheet_insert_lead(lead_id, date_str, name, phone):
    """Insert a new row at position 4 with status=PENDING."""
    iso_now = datetime.now().isoformat()
    logger.info("[Sheets] INSERT lead_id=%s date=%s name=%s phone=%s", lead_id, date_str, name, phone)
    sheets = get_sheets()

    def _do():
        # 1. Push existing rows down
        sheets.batchUpdate(
            spreadsheetId=SHEETS_ID,
            body={"requests": [{
                "insertDimension": {
                    "range": {"sheetId": 0, "dimension": "ROWS",
                              "startIndex": DATA_START_ROW - 1,
                              "endIndex": DATA_START_ROW},
                    "inheritFromBefore": False,
                }
            }]},
        ).execute()
        logger.info("[Sheets] Row inserted at index %d", DATA_START_ROW - 1)

        # 2. Write data
        sheets.values().update(
            spreadsheetId=SHEETS_ID,
            range=f"A{DATA_START_ROW}:F{DATA_START_ROW}",
            valueInputOption="USER_ENTERED",
            body={"values": [[lead_id, date_str, name, phone, "", f"PENDING|{iso_now}|0"]]},
        ).execute()
        logger.info("[Sheets] Values written to A%d:F%d ✓", DATA_START_ROW, DATA_START_ROW)

    sheets_call(_do)
    logger.info("[Sheets] INSERT complete ✓")


def sheet_read_all():
    """Return all data rows as list of dicts (rows where col A is non-empty)."""
    sheets = get_sheets()
    def _do():
        resp = sheets.values().get(
            spreadsheetId=SHEETS_ID,
            range=f"A{DATA_START_ROW}:F",
        ).execute()
        return resp.get("values", [])

    rows = sheets_call(_do)
    leads = []
    for idx, row in enumerate(rows):
        if not row or not row[0]:
            continue
        leads.append({
            "lead_id":        row[0] if len(row) > 0 else "",
            "date_str":       row[1] if len(row) > 1 else "",
            "name":           row[2] if len(row) > 2 else "",
            "phone":          row[3] if len(row) > 3 else "",
            "reason":         row[4] if len(row) > 4 else "",
            "status":         row[5] if len(row) > 5 else "",
            "sheet_row":      DATA_START_ROW + idx,   # actual 1-indexed row number
        })
    return leads


def sheet_find_lead(lead_id):
    """Find a lead by its lead_id. Returns dict or None."""
    for lead in sheet_read_all():
        if lead["lead_id"] == lead_id:
            return lead
    return None


def sheet_update_status(sheet_row, status):
    """Overwrite column F for the given row."""
    sheets = get_sheets()
    def _do():
        sheets.values().update(
            spreadsheetId=SHEETS_ID,
            range=f"F{sheet_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [[status]]},
        ).execute()
        logger.info("[Sheets] Status F%d = %r ✓", sheet_row, status)
    sheets_call(_do)


def sheet_mark_processed(sheet_row, reason):
    """Write reason to col E and set status to DONE in col F."""
    sheets = get_sheets()
    def _do():
        sheets.values().update(
            spreadsheetId=SHEETS_ID,
            range=f"E{sheet_row}:F{sheet_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [[reason, "DONE"]]},
        ).execute()
        logger.info("[Sheets] Processed E%d:F%d ✓", sheet_row, sheet_row)
    sheets_call(_do)


def sheet_get_pending():
    """Return leads whose status starts with PENDING and elapsed >= 30 min."""
    pending = []
    for lead in sheet_read_all():
        if not lead["status"].startswith("PENDING"):
            continue
        parts = lead["status"].split("|")
        if len(parts) < 3:
            continue
        try:
            created_at = datetime.fromisoformat(parts[1])
            reminder_count = int(parts[2])
        except (ValueError, IndexError):
            continue
        elapsed_min = int((datetime.now() - created_at).total_seconds() / 60)
        lead["created_at"]      = parts[1]
        lead["reminder_count"]  = reminder_count
        lead["elapsed_min"]     = elapsed_min
        pending.append(lead)
    return pending


# ─── Multi-line lead parser ───────────────────────────────────────────────────

# Matches phone numbers in many formats:
#   +998 90 123 45 67 / 998901234567 / +7(999)123-45-67 / 8 800 123 45 67
PHONE_RE = re.compile(r'(?<!\d)(\+?[1-9][\d\s\-\.\(\)]{5,20}[\d])(?!\d)')

# Labels that precede phone/name values in structured bot messages
PHONE_LABEL_RE = re.compile(
    r'^(?:телефон|тел|phone|tel|моб|mob|номер|number|kontakt|contact)\s*[:\-]?\s*',
    re.IGNORECASE
)
NAME_LABEL_RE = re.compile(
    r'^(?:имя|name|клиент|client|ф\.?и\.?о\.?|фио|контакт|заявка от|от|from)\s*[:\-]?\s*',
    re.IGNORECASE
)


def extract_phone(raw: str) -> str | None:
    """Normalise a raw phone match — return cleaned string or None if too short."""
    cleaned = re.sub(r'[\s\-\.\(\)]', '', raw)
    # must have at least 7 digits (ignore country-code prefix)
    if len(re.sub(r'\D', '', cleaned)) >= 7:
        return cleaned
    return None


def parse_lead_text(text_body, entities):
    """
    Extracts (phone, name) from any free-form or structured message, e.g.:

    Free-form multi-line:          Structured (bot format):
        restoran                       Имя: Nazokat Latipova
        Nazokat Latipova               Телефон: +998 90 308 09 53
        +998 90 308 09 53

    Single line:                   Inline with label:
        +998903080953                  Тел: +998903080953 — Nazokat

    Returns (phone, name) or (None, None).
    """
    if not text_body:
        return None, None

    lines = [l.strip() for l in text_body.split('\n') if l.strip()]

    # ── 1. Telegram entity (most reliable) ───────────────────────────────────
    phone = None
    for ent in (entities or []):
        if ent.get("type") == "phone_number":
            phone = text_body[ent["offset"]: ent["offset"] + ent["length"]]
            break

    # ── 2. Structured "Label: value" scan ────────────────────────────────────
    structured_name  = None
    structured_phone = None
    for line in lines:
        if PHONE_LABEL_RE.match(line):
            val = PHONE_LABEL_RE.sub('', line).strip()
            m = PHONE_RE.search(val)
            if m:
                structured_phone = extract_phone(m.group(0))
        elif NAME_LABEL_RE.match(line):
            val = NAME_LABEL_RE.sub('', line).strip()
            if val and re.search(r'[a-zA-Zа-яА-ЯёЁ\u0400-\u04FF\u00C0-\u024F]', val):
                structured_name = val

    if structured_phone:
        return structured_phone, structured_name

    # ── 3. Free-form: find first line that contains a phone number ───────────
    phone_line_idx = None
    for i, line in enumerate(lines):
        m = PHONE_RE.search(line)
        if m:
            candidate = extract_phone(m.group(0))
            if candidate:
                if not phone:
                    phone = candidate
                phone_line_idx = i
                break

    if not phone:
        return None, None

    # ── 4. Name = line directly before the phone line ────────────────────────
    name = None
    if phone_line_idx is not None and phone_line_idx > 0:
        candidate = lines[phone_line_idx - 1]
        candidate = NAME_LABEL_RE.sub('', candidate).strip()
        if re.search(r'[a-zA-Zа-яА-ЯёЁ\u0400-\u04FF\u00C0-\u024F]', candidate):
            name = candidate

    # ── 5. Name = line directly after the phone line ─────────────────────────
    if not name and phone_line_idx is not None:
        for j in range(phone_line_idx + 1, min(phone_line_idx + 3, len(lines))):
            candidate = NAME_LABEL_RE.sub('', lines[j]).strip()
            if re.search(r'[a-zA-Zа-яА-ЯёЁ\u0400-\u04FF\u00C0-\u024F]', candidate):
                name = candidate
                break

    # ── 6. Fallback: first line with 2+ words that looks like a name ─────────
    if not name:
        for line in lines:
            cleaned = NAME_LABEL_RE.sub('', line).strip()
            if not cleaned or not re.search(r'[a-zA-Zа-яА-ЯёЁ\u0400-\u04FF\u00C0-\u024F]', cleaned):
                continue
            # skip if this line IS the phone itself or a short keyword
            if PHONE_RE.search(cleaned) and len(re.sub(r'\D', '', cleaned)) >= 7:
                continue
            if len(cleaned.split()) >= 2:
                name = cleaned
                break

    return phone, name


# ─── Telegram helpers ─────────────────────────────────────────────────────────

MAX_TEXT = 4096


def send_message(chat_id, text, reply_markup=None):
    """
    Send a Telegram message safely.
    - Validates chat_id and text before sending.
    - Truncates text to 4096 chars if needed.
    - Logs errors and returns None instead of raising, so callers never crash.
    """
    if not chat_id:
        logger.error("[TG] send_message skipped: chat_id is empty")
        return None
    if not text or not str(text).strip():
        logger.error("[TG] send_message skipped: text is empty (chat_id=%s)", chat_id)
        return None

    safe_text = str(text)
    if len(safe_text) > MAX_TEXT:
        logger.warning("[TG] Text truncated from %d to %d chars", len(safe_text), MAX_TEXT)
        safe_text = safe_text[:MAX_TEXT]

    payload = {"chat_id": chat_id, "text": safe_text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)

    try:
        resp = requests.post(f"{API_BASE}/sendMessage", json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as exc:
        # Log full response body so we can see Telegram's error description
        body = ""
        try:
            body = exc.response.text
        except Exception:
            pass
        logger.error("[TG] sendMessage HTTP %s for chat_id=%s: %s | body: %s",
                     exc.response.status_code if exc.response else "?",
                     chat_id, exc, body)
        return None
    except Exception as exc:
        logger.error("[TG] sendMessage failed for chat_id=%s: %s", chat_id, exc)
        return None


def answer_callback(cq_id, text=""):
    try:
        requests.post(f"{API_BASE}/answerCallbackQuery",
                      json={"callback_query_id": cq_id, "text": text}, timeout=10)
    except Exception as exc:
        logger.error("[TG] answerCallbackQuery failed: %s", exc)


def edit_message_reply_markup(chat_id, message_id):
    try:
        requests.post(f"{API_BASE}/editMessageReplyMarkup",
                      json={"chat_id": chat_id, "message_id": message_id,
                            "reply_markup": json.dumps({"inline_keyboard": []})},
                      timeout=10)
    except Exception as exc:
        logger.error("[TG] editMessageReplyMarkup failed: %s", exc)


# ─── Reminder scheduler ───────────────────────────────────────────────────────

def send_reminders():
    logger.info("[Reminder] Checking for unprocessed leads...")
    try:
        pending = sheet_get_pending()
    except Exception as exc:
        logger.error("[Reminder] Failed to read sheet: %s", exc)
        return

    if not pending:
        logger.info("[Reminder] No pending leads")
        return

    for lead in pending:
        if lead["elapsed_min"] < REMINDER_INTERVAL_MIN:
            logger.info("[Reminder] Lead %s too fresh (%d min)", lead["lead_id"], lead["elapsed_min"])
            continue

        n          = lead["reminder_count"] + 1
        lead_id    = lead["lead_id"]
        call_phone = lead["phone"] if lead["phone"].startswith("+") else f"+{lead['phone']}"

        text = (
            f"⏰ <b>Напоминание #{n} — необработанный лид!</b>\n\n"
            f"🆔 ID: <code>{lead_id}</code>\n"
            f"📞 Телефон: {html_lib.escape(call_phone)}\n"
            f"👤 Имя: {html_lib.escape(str(lead['name']))}\n"
            f"📅 Дата: {lead['date_str']}\n"
            f"🕐 Ожидает: {lead['elapsed_min']} мин.\n"
            f"🔁 Напоминаний: {lead['reminder_count']}\n\n"
            f"⚠️ Пожалуйста, обработайте лид!"
        )
        keyboard = {"inline_keyboard": [[
            {"text": "✅ Квалифицированный",    "callback_data": f"qual|yes|{lead_id}"},
            {"text": "❌ Не квалифицированный", "callback_data": f"qual|no|{lead_id}"},
        ]]}

        for uid in QUALIFY_USER_IDS:
            try:
                send_message(uid, text, reply_markup=keyboard)
                logger.info("[Reminder] #%d for %s → user %d", n, lead_id, uid)
            except Exception as exc:
                logger.error("[Reminder] Failed → user %d: %s", uid, exc)

        # Update counter in sheet
        new_status = f"PENDING|{lead['created_at']}|{n}"
        try:
            sheet_update_status(lead["sheet_row"], new_status)
        except Exception as exc:
            logger.error("[Reminder] Failed to update status: %s", exc)


# ─── Processing report ────────────────────────────────────────────────────────

def send_processing_report(lead, processed_by, qualified, reason, created_at_iso):
    try:
        created_at  = datetime.fromisoformat(created_at_iso)
        now         = datetime.now()
        total_min   = int((now - created_at).total_seconds() / 60)
        hours, mins = divmod(total_min, 60)

        call_phone     = lead["phone"] if lead["phone"].startswith("+") else f"+{lead['phone']}"
        reminder_count = lead.get("reminder_count", 0)
        qual_label     = "✅ Квалифицированный" if qualified else "❌ Не квалифицированный"

        report = (
            f"📊 <b>Отчёт по обработке лида</b>\n\n"
            f"🆔 ID: <code>{lead['lead_id']}</code>\n"
            f"📞 Телефон: {html_lib.escape(call_phone)}\n"
            f"👤 Имя: {html_lib.escape(str(lead['name']))}\n"
            f"📅 Дата: {lead['date_str']}\n\n"
            f"⏱ Время обработки: {hours}ч {mins}мин\n"
            f"👤 Обработал: <code>{processed_by}</code>\n"
            f"🔁 Напоминаний: {reminder_count}\n"
            f"📋 Статус: {qual_label}\n"
            f"💬 Причина: {html_lib.escape(str(reason))}"
        )
        send_message(OWNER_ID, report)
        logger.info("[Report] Sent for lead %s", lead["lead_id"])
    except Exception as exc:
        logger.error("[Report] Failed: %s", exc)


# ─── Webhook ──────────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    logger.info("POST /webhook received")
    try:
        data = request.get_json(force=True)
        if not data:
            logger.error("Empty JSON body")
            return "ok", 200

        logger.info("Update: %s", json.dumps(data, ensure_ascii=False)[:500])

        if "callback_query" in data:
            handle_callback(data["callback_query"])
        elif "message" in data:
            handle_message(data["message"])

    except Exception as exc:
        logger.error("Unhandled error in webhook: %s", exc, exc_info=True)

    # Always return 200 so Telegram does not retry the update
    return "ok", 200


def handle_message(msg):
    sender    = msg.get("from", {})
    chat_id   = msg.get("chat", {}).get("id") or sender.get("id")
    text_body = msg.get("text", "") or msg.get("caption", "")

    # ── Waiting for qualifier's reason text ──────────────────────────────────
    if chat_id in pending_reason and text_body:
        state     = pending_reason.pop(chat_id)
        lead_id   = state["lead_id"]
        qualified = state["qualified"]
        reason    = text_body.strip()

        lead = sheet_find_lead(lead_id)
        if not lead:
            send_message(chat_id, "❌ Лид не найден в таблице.")
            return

        if lead["status"] == "DONE":
            send_message(chat_id, "ℹ️ Этот лид уже обработан другим квалификатором.")
            return

        label       = "Квалифицированный ✅" if qualified else "Не квалифицированный ❌"
        full_reason = f"{label}: {reason}"

        try:
            sheet_mark_processed(lead["sheet_row"], full_reason)
            send_message(chat_id, f"✅ Причина сохранена!\n<i>{html_lib.escape(full_reason)}</i>")
            logger.info("[Lead] Processed %s by %d qualified=%s", lead_id, chat_id, qualified)

            # Parse created_at from status field
            parts      = lead["status"].split("|")
            created_at = parts[1] if len(parts) > 1 else datetime.now().isoformat()
            send_processing_report(lead, chat_id, qualified, full_reason, created_at)
        except Exception as exc:
            logger.error("Failed to process lead: %s", exc)
            send_message(chat_id, f"❌ Ошибка: {exc}")
        return

    # ── Regular contact detection ────────────────────────────────────────────
    username       = sender.get("username")
    sender_display = f"@{username}" if username else (
        f"{sender.get('first_name', '')} {sender.get('last_name', '')}".strip()
        or str(sender.get("id", "?"))
    )
    date_str = datetime.now().strftime("%d.%m.%Y")

    contact = msg.get("contact")
    if contact:
        phone      = contact.get("phone_number", "")
        first_name = contact.get("first_name", "")
        last_name  = contact.get("last_name", "")
        name       = f"{first_name} {last_name}".strip() or sender_display
    else:
        entities     = msg.get("entities", []) or msg.get("caption_entities", [])
        phone, parsed_name = parse_lead_text(text_body, entities)
        if not phone:
            return
        name = parsed_name or sender_display

    e_phone  = html_lib.escape(str(phone))
    e_name   = html_lib.escape(str(name))
    e_sender = html_lib.escape(str(sender_display))
    text = (
        f"📥 <b>Новый контакт</b>\n"
        f"📞 Телефон: <code>{e_phone}</code>\n"
        f"👤 Имя: {e_name}\n"
        f"👤 Кто скинул: {e_sender}\n"
        f"📅 Дата: {date_str}"
    )

    fixed        = f"save|{date_str}||{phone}"
    max_name_len = 64 - len(fixed.encode())
    name_safe    = name[:max_name_len] if len(name.encode()) > max_name_len else name
    cb_save      = f"save|{date_str}|{name_safe}|{phone}"

    keyboard = {"inline_keyboard": [[
        {"text": "✅ Записать",   "callback_data": cb_save},
        {"text": "❌ Пропустить", "callback_data": "skip"},
    ]]}

    send_message(OWNER_ID, text, reply_markup=keyboard)
    logger.info("Forwarded contact: phone=%s name=%s sender=%s", phone, name, sender_display)


def handle_callback(cb):
    cb_id      = cb["id"]
    cb_data    = cb.get("data", "")
    chat_id    = cb["message"]["chat"]["id"]
    message_id = cb["message"]["message_id"]

    answer_callback(cb_id)
    edit_message_reply_markup(chat_id, message_id)

    # ── Save lead ────────────────────────────────────────────────────────────
    if cb_data.startswith("save|"):
        parts = cb_data.split("|", 3)
        if len(parts) != 4:
            send_message(chat_id, "❌ Неверный формат данных.")
            return

        _, date_str, name, phone = parts
        lead_id = datetime.now().strftime("%Y%m%d%H%M%S")

        try:
            sheet_insert_lead(lead_id, date_str, name, phone)
            e_phone2 = html_lib.escape(str(phone))
            e_name2  = html_lib.escape(str(name))
            send_message(
                chat_id,
                f"✅ Записано!\n📞 {e_phone2} — {e_name2}\n🆔 ID: <code>{lead_id}</code>"
            )
            logger.info("Saved lead_id=%s phone=%s name=%s", lead_id, phone, name)

            call_phone = phone if phone.startswith("+") else f"+{phone}"
            e_call     = html_lib.escape(str(call_phone))
            qual_text  = (
                f"📋 <b>Новый лид на квалификацию</b>\n"
                f"🆔 ID: <code>{lead_id}</code>\n"
                f"📞 Телефон: {e_call}\n"
                f"👤 Имя: {e_name2}\n"
                f"📅 Дата: {date_str}\n\n"
                f"Квалифицированный?"
            )
            qual_keyboard = {"inline_keyboard": [[
                {"text": "✅ Квалифицированный",    "callback_data": f"qual|yes|{lead_id}"},
                {"text": "❌ Не квалифицированный", "callback_data": f"qual|no|{lead_id}"},
            ]]}

            failed_qual_uids = []
            for uid in QUALIFY_USER_IDS:
                try:
                    send_message(uid, qual_text, reply_markup=qual_keyboard)
                    logger.info("Qual message → %d lead_id=%s", uid, lead_id)
                except Exception as q_exc:
                    logger.error("Failed to send qual to %d: %s", uid, q_exc)
                    failed_qual_uids.append(uid)
            if failed_qual_uids:
                send_message(
                    chat_id,
                    f"⚠️ Не удалось отправить квалификаторам: {failed_qual_uids}\n"
                    f"Проверьте, что они начали диалог с ботом."
                )

            # Group notification
            try:
                group_text = (
                    f"📥 <b>Новый лид</b>\n"
                    f"📞 Телефон: {e_call}\n"
                    f"👤 Имя: {e_name2}\n"
                    f"📅 Дата: {date_str}\n\n"
                    f"Абдулла ака лид пришел и сейчас отправлен продажникам !"
                )
                send_message(NOTIFY_GROUP_ID, group_text)
                logger.info("Group notified for lead_id=%s", lead_id)
            except Exception as grp_exc:
                logger.error("Group notification failed: %s", grp_exc)

        except Exception as exc:
            logger.error("Failed to save lead: %s", exc)
            send_message(chat_id, f"❌ Ошибка записи: {exc}")

    # ── Qualification answer ─────────────────────────────────────────────────
    elif cb_data.startswith("qual|"):
        parts = cb_data.split("|")
        if len(parts) != 3:
            send_message(chat_id, "❌ Неверный формат данных квалификации.")
            return

        _, verdict, lead_id = parts
        lead = sheet_find_lead(lead_id)

        if not lead:
            send_message(chat_id, "❌ Лид не найден в таблице.")
            return

        if lead["status"] == "DONE":
            send_message(chat_id, "ℹ️ Этот лид уже обработан другим квалификатором.")
            return

        qualified = (verdict == "yes")
        pending_reason[chat_id] = {"lead_id": lead_id, "qualified": qualified}

        label = "✅ Квалифицированный" if qualified else "❌ Не квалифицированный"
        send_message(chat_id, f"{label}\n\nНапишите причину:")
        logger.info("Qual answer=%s lead_id=%s from %d", verdict, lead_id, chat_id)

    # ── Skip ─────────────────────────────────────────────────────────────────
    elif cb_data == "skip":
        send_message(chat_id, "Пропущено ❌")
        logger.info("Contact skipped")


# ─── Webhook registration ─────────────────────────────────────────────────────

def set_webhook():
    endpoint = f"{WEBHOOK_URL.rstrip('/')}/webhook"
    resp     = requests.post(
        f"{API_BASE}/setWebhook",
        json={"url": endpoint, "allowed_updates": ["message", "callback_query"]},
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        logger.info("Webhook set: %s", endpoint)
    else:
        logger.error("Failed to set webhook: %s", result)


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    set_webhook()

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(send_reminders, "interval", minutes=REMINDER_INTERVAL_MIN, id="reminders")
    scheduler.start()
    logger.info("[Scheduler] Reminders started (every %d min)", REMINDER_INTERVAL_MIN)

    port = int(os.environ.get("PORT", 18609))
    logger.info("Starting Flask on port %d", port)
    app.run(host="0.0.0.0", port=port)
