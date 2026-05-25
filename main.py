import os
import re
import json
import logging
import sqlite3
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

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
WEBHOOK_URL = os.environ["WEBHOOK_URL"]
OWNER_ID = int(os.environ["OWNER_TELEGRAM_ID"])
SHEETS_ID = os.environ["GOOGLE_SHEETS_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

QUALIFY_USER_IDS = [514275093, 5028786313]

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
DB_PATH = os.path.join(os.path.dirname(__file__), "leads.db")
REMINDER_INTERVAL_MINUTES = 30

# chat_id -> {'lead_id': int, 'qualified': bool}
pending_reason: dict = {}


# ─── SQLite ───────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            phone           TEXT NOT NULL,
            name            TEXT NOT NULL,
            date_str        TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            sheet_row       INTEGER DEFAULT 4,
            processed       INTEGER DEFAULT 0,
            processed_at    TEXT,
            processed_by    INTEGER,
            qualified       INTEGER,
            reason          TEXT,
            reminder_count  INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    logger.info("[DB] Initialized: %s", DB_PATH)


def db_create_lead(phone, name, date_str, sheet_row=4):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "INSERT INTO leads (phone, name, date_str, created_at, sheet_row) VALUES (?,?,?,?,?)",
        (phone, name, date_str, datetime.now().isoformat(), sheet_row),
    )
    lead_id = cur.lastrowid
    conn.commit()
    conn.close()
    logger.info("[DB] Lead created id=%d phone=%s name=%s", lead_id, phone, name)
    return lead_id


def db_get_lead(lead_id):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def db_mark_processed(lead_id, processed_by, qualified, reason):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """UPDATE leads
           SET processed=1, processed_at=?, processed_by=?, qualified=?, reason=?
           WHERE id=?""",
        (datetime.now().isoformat(), processed_by, 1 if qualified else 0, reason, lead_id),
    )
    conn.commit()
    conn.close()
    logger.info("[DB] Lead %d marked processed by %d", lead_id, processed_by)


def db_increment_reminder(lead_id):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE leads SET reminder_count = reminder_count + 1 WHERE id=?", (lead_id,)
    )
    conn.commit()
    conn.close()


def db_get_unprocessed():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM leads WHERE processed=0").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── Google Sheets (retry + detailed logging) ─────────────────────────────────

def get_sheets_service():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=credentials).spreadsheets()


def sheets_call(fn, retries=3):
    """Run a Sheets API call with exponential-backoff retry."""
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            logger.warning("[Sheets] Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                raise


def insert_row_to_sheet(date_str, name, phone):
    logger.info("[Sheets] INSERT row — date=%s name=%s phone=%s", date_str, name, phone)
    sheets = get_sheets_service()

    def _insert():
        sheets.batchUpdate(
            spreadsheetId=SHEETS_ID,
            body={"requests": [{
                "insertDimension": {
                    "range": {"sheetId": 0, "dimension": "ROWS",
                               "startIndex": 3, "endIndex": 4},
                    "inheritFromBefore": False,
                }
            }]},
        ).execute()
        logger.info("[Sheets] Dimension inserted at row index 3")

        sheets.values().update(
            spreadsheetId=SHEETS_ID,
            range="A4:E4",
            valueInputOption="USER_ENTERED",
            body={"values": [["", date_str, name, phone, ""]]},
        ).execute()
        logger.info("[Sheets] Values written to A4:E4 ✓")

    sheets_call(_insert)
    logger.info("[Sheets] INSERT complete ✓")


def update_reason_in_sheet(row_num, reason):
    logger.info("[Sheets] UPDATE E%d = %r", row_num, reason)
    sheets = get_sheets_service()

    def _update():
        sheets.values().update(
            spreadsheetId=SHEETS_ID,
            range=f"E{row_num}",
            valueInputOption="USER_ENTERED",
            body={"values": [[reason]]},
        ).execute()
        logger.info("[Sheets] E%d written ✓", row_num)

    sheets_call(_update)
    logger.info("[Sheets] UPDATE complete ✓")


# ─── Telegram helpers ─────────────────────────────────────────────────────────

def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    resp = requests.post(f"{API_BASE}/sendMessage", json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


def answer_callback(callback_query_id, text=""):
    requests.post(
        f"{API_BASE}/answerCallbackQuery",
        json={"callback_query_id": callback_query_id, "text": text},
        timeout=10,
    )


def edit_message_reply_markup(chat_id, message_id):
    requests.post(
        f"{API_BASE}/editMessageReplyMarkup",
        json={"chat_id": chat_id, "message_id": message_id,
              "reply_markup": json.dumps({"inline_keyboard": []})},
        timeout=10,
    )


# ─── Reminder scheduler ───────────────────────────────────────────────────────

def send_reminders():
    leads = db_get_unprocessed()
    if not leads:
        logger.info("[Reminder] No unprocessed leads")
        return

    for lead in leads:
        lead_id = lead["id"]
        created_at = datetime.fromisoformat(lead["created_at"])
        elapsed_sec = (datetime.now() - created_at).total_seconds()
        elapsed_min = int(elapsed_sec / 60)

        if elapsed_min < REMINDER_INTERVAL_MINUTES:
            logger.info("[Reminder] Lead %d too fresh (%d min), skipping", lead_id, elapsed_min)
            continue

        n = lead["reminder_count"] + 1
        call_phone = lead["phone"] if lead["phone"].startswith("+") else f"+{lead['phone']}"
        text = (
            f"⏰ <b>Напоминание #{n} — необработанный лид!</b>\n\n"
            f"🆔 ID: <code>{lead_id}</code>\n"
            f"📞 Телефон: {call_phone}\n"
            f"👤 Имя: {lead['name']}\n"
            f"📅 Дата: {lead['date_str']}\n"
            f"🕐 Ожидает: {elapsed_min} мин.\n"
            f"🔁 Напоминаний: {lead['reminder_count']}\n\n"
            f"⚠️ Пожалуйста, обработайте лид!"
        )
        keyboard = {"inline_keyboard": [[
            {"text": "✅ Квалифицированный", "callback_data": f"qual|yes|{lead_id}"},
            {"text": "❌ Не квалифицированный", "callback_data": f"qual|no|{lead_id}"},
        ]]}

        for uid in QUALIFY_USER_IDS:
            try:
                send_message(uid, text, reply_markup=keyboard)
                logger.info("[Reminder] #%d for lead %d → user %d", n, lead_id, uid)
            except Exception as exc:
                logger.error("[Reminder] Failed → user %d: %s", uid, exc)

        db_increment_reminder(lead_id)


# ─── Processing report ────────────────────────────────────────────────────────

def send_processing_report(lead_id, processed_by_chat_id):
    lead = db_get_lead(lead_id)
    if not lead:
        return

    created_at = datetime.fromisoformat(lead["created_at"])
    processed_at = datetime.fromisoformat(lead["processed_at"])
    total_min = int((processed_at - created_at).total_seconds() / 60)
    hours, mins = divmod(total_min, 60)

    qualified_label = "✅ Квалифицированный" if lead["qualified"] else "❌ Не квалифицированный"
    call_phone = lead["phone"] if lead["phone"].startswith("+") else f"+{lead['phone']}"

    report = (
        f"📊 <b>Отчёт по обработке лида</b>\n\n"
        f"🆔 ID лида: <code>{lead_id}</code>\n"
        f"📞 Телефон: {call_phone}\n"
        f"👤 Имя: {lead['name']}\n"
        f"📅 Дата поступления: {lead['date_str']}\n\n"
        f"⏱ Время обработки: {hours}ч {mins}мин\n"
        f"👤 Обработал: <code>{processed_by_chat_id}</code>\n"
        f"🔁 Напоминаний отправлено: {lead['reminder_count']}\n"
        f"📋 Статус: {qualified_label}\n"
        f"💬 Причина: {lead['reason']}"
    )

    try:
        send_message(OWNER_ID, report)
        logger.info("[Report] Sent for lead %d", lead_id)
    except Exception as exc:
        logger.error("[Report] Failed: %s", exc)


# ─── Webhook ──────────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    logger.info("POST /webhook received")
    try:
        data = request.get_json(force=True)
    except Exception as exc:
        logger.error("Failed to parse JSON: %s", exc)
        abort(400)

    logger.info("Update: %s", json.dumps(data, ensure_ascii=False))

    if "callback_query" in data:
        handle_callback(data["callback_query"])
    elif "message" in data:
        handle_message(data["message"])

    return "ok", 200


def handle_message(msg):
    sender = msg.get("from", {})
    chat_id = msg.get("chat", {}).get("id") or sender.get("id")
    text_body = msg.get("text", "")

    # Waiting for qualifier's reason text
    if chat_id in pending_reason and text_body:
        state = pending_reason.pop(chat_id)
        lead_id = state["lead_id"]
        qualified = state["qualified"]
        reason = text_body.strip()

        lead = db_get_lead(lead_id)
        if not lead:
            send_message(chat_id, "❌ Лид не найден в базе.")
            return

        if lead["processed"]:
            send_message(chat_id, "ℹ️ Этот лид уже был обработан другим квалификатором.")
            return

        label = "Квалифицированный ✅" if qualified else "Не квалифицированный ❌"
        full_reason = f"{label}: {reason}"

        try:
            update_reason_in_sheet(lead["sheet_row"], full_reason)
            db_mark_processed(lead_id, chat_id, qualified, full_reason)
            send_message(chat_id, f"✅ Причина сохранена!\n<i>{full_reason}</i>")
            logger.info("[Lead] Processed lead_id=%d by=%d qualified=%s", lead_id, chat_id, qualified)
            send_processing_report(lead_id, chat_id)
        except Exception as exc:
            logger.error("Failed to save reason: %s", exc)
            send_message(chat_id, f"❌ Ошибка сохранения причины: {exc}")
        return

    # Regular contact detection
    username = sender.get("username")
    sender_display = f"@{username}" if username else (
        f"{sender.get('first_name', '')} {sender.get('last_name', '')}".strip()
        or str(sender.get("id", "?"))
    )
    date_str = datetime.now().strftime("%d.%m.%Y")

    contact = msg.get("contact")
    if contact:
        phone = contact.get("phone_number", "")
        first_name = contact.get("first_name", "")
        last_name = contact.get("last_name", "")
        name = f"{first_name} {last_name}".strip() or sender_display
    else:
        entities = msg.get("entities", [])
        phone = None
        for ent in entities:
            if ent.get("type") == "phone_number":
                offset, length = ent["offset"], ent["length"]
                phone = text_body[offset:offset + length]
                break
        if not phone and text_body:
            match = re.search(r'\+?[\d][\d\s\-\(\)]{5,17}[\d]', text_body)
            if match:
                raw = match.group(0)
                digits_only = re.sub(r'[\s\-\(\)]', '', raw)
                if len(digits_only) >= 7:
                    phone = digits_only
        if not phone:
            return
        name = sender_display

    text = (
        f"📥 <b>Новый контакт</b>\n"
        f"📞 Телефон: <code>{phone}</code>\n"
        f"👤 Имя: {name}\n"
        f"👤 Кто скинул: {sender_display}\n"
        f"📅 Дата: {date_str}"
    )

    fixed = f"save|{date_str}||{phone}"
    max_name_len = 64 - len(fixed.encode())
    name_safe = name[:max_name_len] if len(name.encode()) > max_name_len else name
    cb_save = f"save|{date_str}|{name_safe}|{phone}"

    keyboard = {"inline_keyboard": [[
        {"text": "✅ Записать", "callback_data": cb_save},
        {"text": "❌ Пропустить", "callback_data": "skip"},
    ]]}

    result = send_message(OWNER_ID, text, reply_markup=keyboard)
    logger.info("Forwarded contact to owner: phone=%s name=%s sender=%s", phone, name, sender_display)
    return result


def handle_callback(cb):
    cb_id = cb["id"]
    cb_data = cb.get("data", "")
    chat_id = cb["message"]["chat"]["id"]
    message_id = cb["message"]["message_id"]

    answer_callback(cb_id)
    edit_message_reply_markup(chat_id, message_id)

    if cb_data.startswith("save|"):
        parts = cb_data.split("|", 3)
        if len(parts) == 4:
            _, date_str, name, phone = parts
            try:
                insert_row_to_sheet(date_str, name, phone)
                lead_id = db_create_lead(phone, name, date_str, sheet_row=4)

                send_message(
                    chat_id,
                    f"✅ Записано в таблицу!\n📞 {phone} — {name}\n🆔 ID лида: <code>{lead_id}</code>"
                )
                logger.info("Saved: %s %s %s lead_id=%d", date_str, name, phone, lead_id)

                call_phone = phone if phone.startswith("+") else f"+{phone}"
                qual_text = (
                    f"📋 <b>Новый лид на квалификацию</b>\n"
                    f"🆔 ID: <code>{lead_id}</code>\n"
                    f"📞 Телефон: {call_phone}\n"
                    f"👤 Имя: {name}\n"
                    f"📅 Дата: {date_str}\n\n"
                    f"Квалифицированный?"
                )
                qual_keyboard = {"inline_keyboard": [[
                    {"text": "✅ Квалифицированный", "callback_data": f"qual|yes|{lead_id}"},
                    {"text": "❌ Не квалифицированный", "callback_data": f"qual|no|{lead_id}"},
                ]]}

                for uid in QUALIFY_USER_IDS:
                    send_message(uid, qual_text, reply_markup=qual_keyboard)
                    logger.info("Qual message → %d for lead_id=%d", uid, lead_id)

            except Exception as exc:
                logger.error("Failed to insert row: %s", exc)
                send_message(chat_id, f"❌ Ошибка записи в таблицу: {exc}")
        else:
            send_message(chat_id, "❌ Ошибка: неверный формат данных.")

    elif cb_data.startswith("qual|"):
        parts = cb_data.split("|")
        if len(parts) == 3:
            _, verdict, lead_id_str = parts
            try:
                lead_id = int(lead_id_str)
            except ValueError:
                send_message(chat_id, "❌ Ошибка: неверный ID лида.")
                return

            lead = db_get_lead(lead_id)
            if not lead:
                send_message(chat_id, "❌ Лид не найден в базе.")
                return

            if lead["processed"]:
                send_message(chat_id, "ℹ️ Этот лид уже обработан другим квалификатором.")
                return

            qualified = (verdict == "yes")
            pending_reason[chat_id] = {"lead_id": lead_id, "qualified": qualified}

            label = "✅ Квалифицированный" if qualified else "❌ Не квалифицированный"
            send_message(chat_id, f"{label}\n\nНапишите причину:")
            logger.info("Qual answer=%s lead_id=%d from chat=%d", verdict, lead_id, chat_id)
        else:
            send_message(chat_id, "❌ Ошибка: неверный формат данных квалификации.")

    elif cb_data == "skip":
        send_message(chat_id, "Пропущено ❌")
        logger.info("Contact skipped by owner")


# ─── Webhook registration ─────────────────────────────────────────────────────

def set_webhook():
    webhook_endpoint = f"{WEBHOOK_URL.rstrip('/')}/webhook"
    resp = requests.post(
        f"{API_BASE}/setWebhook",
        json={"url": webhook_endpoint, "allowed_updates": ["message", "callback_query"]},
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        logger.info("Webhook set: %s", webhook_endpoint)
    else:
        logger.error("Failed to set webhook: %s", result)


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    set_webhook()

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(send_reminders, "interval", minutes=REMINDER_INTERVAL_MINUTES, id="reminders")
    scheduler.start()
    logger.info("[Scheduler] Reminder job started (every %d min)", REMINDER_INTERVAL_MINUTES)

    port = int(os.environ.get("PORT", 18609))
    logger.info("Starting Flask on port %d", port)
    app.run(host="0.0.0.0", port=port)
