import os
import re
import json
import html as html_lib
import logging
import time
import pg8000.native
from datetime import datetime, timedelta
from contextlib import contextmanager
from flask import Flask, request
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

BOT_TOKEN               = os.environ["TELEGRAM_BOT_TOKEN"]
WEBHOOK_URL             = os.environ["WEBHOOK_URL"]
OWNER_ID                = int(os.environ["OWNER_TELEGRAM_ID"])
SHEETS_ID               = os.environ["GOOGLE_SHEETS_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]
DATABASE_URL            = os.environ["DATABASE_URL"]

MAIN_QUALIFIER_ID       = 514275093
SECOND_QUALIFIER_ID     = 5028786313
NOTIFY_GROUP_ID         = -5160536788

API_BASE                = f"https://api.telegram.org/bot{BOT_TOKEN}"
SCOPES                  = ["https://www.googleapis.com/auth/spreadsheets"]
REMINDER_INTERVAL_MIN   = 30
DATA_START_ROW          = 4

# ─── DB ───────────────────────────────────────────────────────────────────────

def parse_db_url(url):
    """Parse postgres://user:pass@host:port/dbname"""
    import urllib.parse
    r = urllib.parse.urlparse(url)
    return {
        "host": r.hostname,
        "port": r.port or 5432,
        "database": r.path.lstrip("/"),
        "user": r.username,
        "password": r.password,
    }

@contextmanager
def get_db():
    params = parse_db_url(DATABASE_URL)
    conn = pg8000.native.Connection(
        host=params["host"],
        port=params["port"],
        database=params["database"],
        user=params["user"],
        password=params["password"],
        ssl_context=True,
    )
    try:
        yield conn
        conn.run("COMMIT")
    except Exception:
        conn.run("ROLLBACK")
        raise
    finally:
        conn.close()


def row_to_dict(columns, row):
    return dict(zip(columns, row))


def fetchall_dict(conn, query, params=None):
    if params:
        rows = conn.run(query, *params)
    else:
        rows = conn.run(query)
    cols = [c["name"] for c in conn.columns]
    return [row_to_dict(cols, r) for r in rows]


def fetchone_dict(conn, query, params=None):
    results = fetchall_dict(conn, query, params)
    return results[0] if results else None


def init_db():
    with get_db() as conn:
        conn.run("""
            CREATE TABLE IF NOT EXISTS leads (
                lead_id        TEXT PRIMARY KEY,
                date_str       TEXT NOT NULL,
                name           TEXT DEFAULT '',
                phone          TEXT NOT NULL,
                reason         TEXT DEFAULT '',
                status         TEXT DEFAULT 'PENDING',
                assigned_to    BIGINT DEFAULT 0,
                processed_by   TEXT DEFAULT '',
                created_at     TIMESTAMP DEFAULT NOW(),
                updated_at     TIMESTAMP DEFAULT NOW(),
                reminder_count INT DEFAULT 0,
                sheet_row      INT DEFAULT 0
            )
        """)
        conn.run("""
            CREATE TABLE IF NOT EXISTS pending_states (
                chat_id        BIGINT PRIMARY KEY,
                state_type     TEXT NOT NULL,
                lead_id        TEXT NOT NULL,
                qualified      BOOLEAN DEFAULT FALSE,
                user_label     TEXT DEFAULT '',
                followup_round INT DEFAULT 0,
                created_at     TIMESTAMP DEFAULT NOW()
            )
        """)
    logger.info("[DB] Tables ready")


# ─── DB helpers ───────────────────────────────────────────────────────────────

def db_insert_lead(lead_id, date_str, name, phone):
    with get_db() as conn:
        conn.run("""
            INSERT INTO leads (lead_id, date_str, name, phone, status, created_at, updated_at)
            VALUES (:1, :2, :3, :4, 'PENDING', NOW(), NOW())
            ON CONFLICT (lead_id) DO NOTHING
        """, lead_id, date_str, name or '', phone)


def db_find_lead(lead_id):
    with get_db() as conn:
        return fetchone_dict(conn, "SELECT * FROM leads WHERE lead_id = :1", [lead_id])


def db_assign_lead(lead_id, qualifier_id):
    with get_db() as conn:
        conn.run("""
            UPDATE leads SET assigned_to = :1, status = 'ASSIGNED', updated_at = NOW()
            WHERE lead_id = :2
        """, qualifier_id, lead_id)


def db_claim_lead(lead_id, user_label, qualifier_id):
    with get_db() as conn:
        conn.run("""
            UPDATE leads
            SET status = 'PROCESSING', processed_by = :1, assigned_to = :2, updated_at = NOW()
            WHERE lead_id = :3 AND status IN ('PENDING', 'ASSIGNED')
        """, user_label, qualifier_id, lead_id)
        return conn.row_count > 0


def db_mark_processed(lead_id, reason, qualified):
    status = 'QUALIFIED' if qualified else 'DONE'
    with get_db() as conn:
        conn.run("""
            UPDATE leads SET status = :1, reason = :2, updated_at = NOW(), reminder_count = 0
            WHERE lead_id = :3
        """, status, reason, lead_id)


def db_advance_followup(lead_id, new_round):
    with get_db() as conn:
        conn.run("UPDATE leads SET reminder_count = :1, updated_at = NOW() WHERE lead_id = :2",
                 new_round, lead_id)


def db_get_pending_leads():
    cutoff = datetime.now() - timedelta(minutes=REMINDER_INTERVAL_MIN)
    with get_db() as conn:
        return fetchall_dict(conn, """
            SELECT * FROM leads
            WHERE status IN ('PENDING', 'ASSIGNED') AND updated_at <= :1
        """, [cutoff])


def db_get_qualified_for_followup(days):
    expected_round = 0 if days == 1 else 1
    cutoff = datetime.now() - timedelta(days=days)
    with get_db() as conn:
        return fetchall_dict(conn, """
            SELECT * FROM leads
            WHERE status = 'QUALIFIED' AND reminder_count = :1 AND updated_at <= :2
        """, [expected_round, cutoff])


def db_set_sheet_row(lead_id, sheet_row):
    with get_db() as conn:
        conn.run("UPDATE leads SET sheet_row = :1 WHERE lead_id = :2", sheet_row, lead_id)


def db_increment_reminder(lead_id):
    with get_db() as conn:
        conn.run("""
            UPDATE leads SET reminder_count = reminder_count + 1, updated_at = NOW()
            WHERE lead_id = :1
        """, lead_id)


# ─── Pending state ────────────────────────────────────────────────────────────

def db_set_pending_reason(chat_id, lead_id, qualified, user_label):
    with get_db() as conn:
        conn.run("""
            INSERT INTO pending_states (chat_id, state_type, lead_id, qualified, user_label, created_at)
            VALUES (:1, 'reason', :2, :3, :4, NOW())
            ON CONFLICT (chat_id) DO UPDATE
            SET state_type='reason', lead_id=:2, qualified=:3, user_label=:4, created_at=NOW()
        """, chat_id, lead_id, qualified, user_label)


def db_set_pending_followup(chat_id, lead_id, followup_round, user_label):
    with get_db() as conn:
        conn.run("""
            INSERT INTO pending_states (chat_id, state_type, lead_id, followup_round, user_label, created_at)
            VALUES (:1, 'followup', :2, :3, :4, NOW())
            ON CONFLICT (chat_id) DO UPDATE
            SET state_type='followup', lead_id=:2, followup_round=:3, user_label=:4, created_at=NOW()
        """, chat_id, lead_id, followup_round, user_label)


def db_get_pending_state(chat_id):
    with get_db() as conn:
        return fetchone_dict(conn, "SELECT * FROM pending_states WHERE chat_id = :1", [chat_id])


def db_clear_pending_state(chat_id):
    with get_db() as conn:
        conn.run("DELETE FROM pending_states WHERE chat_id = :1", chat_id)


# ─── Keyboards ────────────────────────────────────────────────────────────────

def keyboard_main_qualifier(lead_id):
    return {"inline_keyboard": [[
        {"text": "✅ Принять",  "callback_data": f"accept|{lead_id}"},
        {"text": "❌ Отказать", "callback_data": f"reject|{lead_id}"},
    ]]}

def keyboard_second_qualifier(lead_id):
    return {"inline_keyboard": [[
        {"text": "✅ Принять", "callback_data": f"accept|{lead_id}"}
    ]]}

def keyboard_qualification(lead_id):
    return {"inline_keyboard": [[
        {"text": "✅ Квалифицированный",    "callback_data": f"qual|yes|{lead_id}"},
        {"text": "❌ Не квалифицированный", "callback_data": f"qual|no|{lead_id}"},
    ]]}


# ─── Google Sheets ────────────────────────────────────────────────────────────

def get_sheets():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS_JSON), scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()


def sheets_call(fn, retries=3):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            logger.warning("[Sheets] Attempt %d/%d: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                raise


def sheet_insert_lead(lead_id, date_str, name, phone):
    sheets = get_sheets()
    def _do():
        result = sheets.values().get(
            spreadsheetId=SHEETS_ID, range="A:A"
        ).execute()
        existing_rows = len(result.get("values", []))
        insert_row = max(existing_rows + 1, DATA_START_ROW)
        sheets.values().append(
            spreadsheetId=SHEETS_ID,
            range=f"A{insert_row}",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [[lead_id, date_str, name, phone, "", "PENDING"]]},
        ).execute()
        return insert_row
    return sheets_call(_do)


def sheet_update_row(sheet_row, reason, status):
    if not sheet_row:
        return
    sheets = get_sheets()
    def _do():
        sheets.values().update(
            spreadsheetId=SHEETS_ID,
            range=f"E{sheet_row}:F{sheet_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [[reason, status]]},
        ).execute()
    sheets_call(_do)


# ─── Phone & parser ───────────────────────────────────────────────────────────

def normalize_phone(raw):
    digits = re.sub(r'\D', '', str(raw))
    if str(raw).strip().startswith('+'):
        return f"+{digits}"
    if digits.startswith('998') and len(digits) == 12:
        return f"+{digits}"
    if len(digits) == 9:
        return f"+998{digits}"
    if digits.startswith('8') and len(digits) == 11:
        return f"+7{digits[1:]}"
    return f"+{digits}"


PHONE_RE       = re.compile(r'(?<!\d)(\+?[1-9][\d\s\-\.\(\)]{5,20}[\d])(?!\d)')
PHONE_LABEL_RE = re.compile(r'^(?:телефон|тел|phone|tel|моб|mob|номер|number)\s*[:\-]?\s*', re.IGNORECASE)
NAME_LABEL_RE  = re.compile(r'^(?:имя|name|клиент|client|фио|от|from)\s*[:\-]?\s*', re.IGNORECASE)


def extract_phone(raw):
    cleaned = re.sub(r'[\s\-\.\(\)]', '', raw)
    return cleaned if len(re.sub(r'\D', '', cleaned)) >= 7 else None


def parse_lead_text(text_body, entities):
    if not text_body:
        return None, None
    lines = [l.strip() for l in text_body.split('\n') if l.strip()]
    phone = None

    for ent in (entities or []):
        if ent.get("type") == "phone_number":
            phone = text_body[ent["offset"]: ent["offset"] + ent["length"]]
            break

    structured_name = structured_phone = None
    for line in lines:
        if PHONE_LABEL_RE.match(line):
            m = PHONE_RE.search(PHONE_LABEL_RE.sub('', line))
            if m:
                structured_phone = extract_phone(m.group(0))
        elif NAME_LABEL_RE.match(line):
            val = NAME_LABEL_RE.sub('', line).strip()
            if val and re.search(r'[a-zA-Zа-яА-ЯёЁ]', val):
                structured_name = val

    if structured_phone:
        return structured_phone, structured_name

    phone_line_idx = None
    for i, line in enumerate(lines):
        m = PHONE_RE.search(line)
        if m:
            c = extract_phone(m.group(0))
            if c:
                phone = phone or c
                phone_line_idx = i
                break

    if not phone:
        return None, None

    name = None
    if phone_line_idx and phone_line_idx > 0:
        c = NAME_LABEL_RE.sub('', lines[phone_line_idx - 1]).strip()
        if re.search(r'[a-zA-Zа-яА-ЯёЁ]', c):
            name = c

    if not name and phone_line_idx is not None:
        for j in range(phone_line_idx + 1, min(phone_line_idx + 3, len(lines))):
            c = NAME_LABEL_RE.sub('', lines[j]).strip()
            if re.search(r'[a-zA-Zа-яА-ЯёЁ]', c):
                name = c
                break

    if not name:
        for line in lines:
            c = NAME_LABEL_RE.sub('', line).strip()
            if not c or not re.search(r'[a-zA-Zа-яА-ЯёЁ]', c):
                continue
            if PHONE_RE.search(c) and len(re.sub(r'\D', '', c)) >= 7:
                continue
            if len(c.split()) >= 2:
                name = c
                break

    return phone, name


# ─── Telegram helpers ─────────────────────────────────────────────────────────

def send_message(chat_id, text, reply_markup=None):
    if not chat_id or not text:
        return None
    payload = {"chat_id": chat_id, "text": str(text)[:4096], "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        r = requests.post(f"{API_BASE}/sendMessage", json=payload, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.error("[TG] sendMessage failed chat_id=%s: %s", chat_id, exc)
        return None


def answer_callback(cq_id, text=""):
    try:
        requests.post(f"{API_BASE}/answerCallbackQuery",
                      json={"callback_query_id": cq_id, "text": text}, timeout=10)
    except Exception as exc:
        logger.error("[TG] answerCallback failed: %s", exc)


def edit_markup(chat_id, message_id, markup=None):
    try:
        requests.post(f"{API_BASE}/editMessageReplyMarkup",
                      json={"chat_id": chat_id, "message_id": message_id,
                            "reply_markup": json.dumps(markup or {"inline_keyboard": []})},
                      timeout=10)
    except Exception as exc:
        logger.error("[TG] editMarkup failed: %s", exc)


def get_user_label(user_dict):
    uname = user_dict.get("username")
    fname = user_dict.get("first_name", "")
    lname = user_dict.get("last_name", "")
    return f"@{uname}" if uname else (f"{fname} {lname}".strip() or str(user_dict.get("id", "?")))


def lead_info(lead_id, e_call, e_name, date_str):
    return (
        f"🆔 ID: <code>{lead_id}</code>\n"
        f"📞 Телефон: {e_call}\n"
        f"👤 Имя: {e_name}\n"
        f"📅 Дата: {date_str}"
    )


# ─── Schedulers ───────────────────────────────────────────────────────────────

def send_reminders():
    logger.info("[Reminder] Checking...")
    try:
        leads = db_get_pending_leads()
    except Exception as exc:
        logger.error("[Reminder] DB error: %s", exc)
        return

    for lead in leads:
        lead_id    = lead["lead_id"]
        call_phone = normalize_phone(lead["phone"])
        n          = lead["reminder_count"] + 1
        elapsed    = int((datetime.now() - lead["created_at"]).total_seconds() / 60)

        text = (
            f"⏰ <b>Напоминание #{n} — необработанный лид!</b>\n\n"
            f"{lead_info(lead_id, html_lib.escape(call_phone), html_lib.escape(str(lead['name'])), lead['date_str'])}\n"
            f"🕐 Ожидает: {elapsed} мин.\n\nПримите или отклоните лид:"
        )
        send_message(MAIN_QUALIFIER_ID, text, reply_markup=keyboard_main_qualifier(lead_id))
        if lead["status"] == "ASSIGNED" and lead["assigned_to"] == SECOND_QUALIFIER_ID:
            send_message(SECOND_QUALIFIER_ID, text, reply_markup=keyboard_second_qualifier(lead_id))
        send_message(NOTIFY_GROUP_ID,
            f"⏰ Необработанный лид #{n}\n📞 {html_lib.escape(call_phone)} | ожидает {elapsed} мин.")
        try:
            db_increment_reminder(lead_id)
        except Exception as exc:
            logger.error("[Reminder] increment failed: %s", exc)


def send_followups():
    logger.info("[FollowUp] Checking...")
    for days, label in [(1, "1-дневный"), (3, "3-дневный")]:
        try:
            leads = db_get_qualified_for_followup(days)
        except Exception as exc:
            logger.error("[FollowUp] DB error: %s", exc)
            continue
        for lead in leads:
            lead_id     = lead["lead_id"]
            assigned_to = lead.get("assigned_to") or MAIN_QUALIFIER_ID
            e_phone     = html_lib.escape(normalize_phone(lead["phone"]))
            e_name      = html_lib.escape(str(lead["name"]))
            round_num   = lead["reminder_count"] + 1
            fu_text = (
                f"📋 <b>{label} follow-up</b>\n\n"
                f"{lead_info(lead_id, e_phone, e_name, lead['date_str'])}\n\n"
                f"Что сейчас происходит с этим лидом? На каком этапе?\n"
                f"<i>Напишите ответ текстом</i>"
            )
            send_message(assigned_to, fu_text)
            db_set_pending_followup(assigned_to, lead_id, round_num, str(assigned_to))
            db_advance_followup(lead_id, round_num)
            logger.info("[FollowUp] %s → uid=%d lead=%s", label, assigned_to, lead_id)


# ─── Webhook ──────────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        if not data:
            return "ok", 200
        logger.info("Update: %s", json.dumps(data, ensure_ascii=False)[:500])
        if "callback_query" in data:
            handle_callback(data["callback_query"])
        elif "message" in data:
            handle_message(data["message"])
    except Exception as exc:
        logger.error("Webhook error: %s", exc, exc_info=True)
    return "ok", 200


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}, 200


def handle_message(msg):
    sender    = msg.get("from", {})
    chat_id   = msg.get("chat", {}).get("id") or sender.get("id")
    text_body = msg.get("text", "") or msg.get("caption", "")

    if text_body and text_body.startswith("/stats") and chat_id in (OWNER_ID, MAIN_QUALIFIER_ID, SECOND_QUALIFIER_ID):
        today       = datetime.now().strftime("%d.%m.%Y")
        month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        with get_db() as conn:
            total         = fetchone_dict(conn, "SELECT COUNT(*) as cnt FROM leads")["cnt"]
            today_count   = fetchone_dict(conn, "SELECT COUNT(*) as cnt FROM leads WHERE date_str = :1", [today])["cnt"]
            month_count   = fetchone_dict(conn, "SELECT COUNT(*) as cnt FROM leads WHERE created_at >= :1", [month_start])["cnt"]
            qual_count    = fetchone_dict(conn, "SELECT COUNT(*) as cnt FROM leads WHERE status IN ('QUALIFIED','FOLLOWUP_DONE')")["cnt"]
            done_count    = fetchone_dict(conn, "SELECT COUNT(*) as cnt FROM leads WHERE status = 'DONE'")["cnt"]
            pending_count = fetchone_dict(conn, "SELECT COUNT(*) as cnt FROM leads WHERE status IN ('PENDING','ASSIGNED')")["cnt"]
            by_user       = fetchall_dict(conn, "SELECT processed_by, COUNT(*) as cnt FROM leads WHERE processed_by != '' GROUP BY processed_by ORDER BY cnt DESC")

        qual_rate  = round(qual_count / max(qual_count + done_count, 1) * 100)
        user_lines = "".join(f"  👤 {html_lib.escape(r['processed_by'])}: {r['cnt']}\n" for r in by_user if r['processed_by'])
        stats_text = (
            f"📊 <b>Статистика лидов</b>\n\n"
            f"📅 Сегодня: <b>{today_count}</b>\n"
            f"📆 За месяц: <b>{month_count}</b>\n"
            f"📦 Всего: <b>{total}</b>\n\n"
            f"✅ Квалифицированных: {qual_count}\n"
            f"❌ Не квалифицированных: {done_count}\n"
            f"⏳ Ожидают: {pending_count}\n"
            f"📈 Конверсия: {qual_rate}%\n"
        )
        if user_lines:
            stats_text += f"\n<b>По квалификаторам:</b>\n{user_lines}"
        send_message(chat_id, stats_text)
        return

    if text_body and text_body.startswith("/resend_today") and chat_id == OWNER_ID:
        today = datetime.now().strftime("%d.%m.%Y")
        with get_db() as conn:
            leads = fetchall_dict(conn, "SELECT * FROM leads WHERE date_str = :1", [today])
        if not leads:
            send_message(OWNER_ID, f"ℹ️ Сегодня ({today}) лидов нет.")
            return
        send_message(OWNER_ID, f"📤 Отправляю {len(leads)} лидов...")
        for lead in leads:
            lid    = lead["lead_id"]
            e_call = html_lib.escape(normalize_phone(lead["phone"]))
            e_name = html_lib.escape(str(lead["name"]))
            send_message(MAIN_QUALIFIER_ID,
                f"📋 <b>Лид за {today} (повтор)</b>\n\n{lead_info(lid, e_call, e_name, today)}\n\nПримите или отклоните:",
                reply_markup=keyboard_main_qualifier(lid))
        send_message(OWNER_ID, f"✅ Отправлено {len(leads)} лидов.")
        return

    if chat_id and text_body:
        state = db_get_pending_state(chat_id)

        if state and state["state_type"] == "reason":
            db_clear_pending_state(chat_id)
            lead_id    = state["lead_id"]
            qualified  = state["qualified"]
            user_label = state["user_label"]
            reason     = text_body.strip()
            lead = db_find_lead(lead_id)
            if not lead:
                send_message(chat_id, "❌ Лид не найден.")
                return
            if lead["status"] != "PROCESSING":
                send_message(chat_id, "ℹ️ Лид уже обработан.")
                return
            label       = "Квалифицированный ✅" if qualified else "Не квалифицированный ❌"
            full_reason = f"{label} | {user_label}: {reason}"
            db_mark_processed(lead_id, full_reason, qualified=qualified)
            send_message(chat_id, f"✅ Сохранено!\n<i>{html_lib.escape(full_reason)}</i>")
            if lead.get("sheet_row"):
                try:
                    sheet_update_row(lead["sheet_row"], full_reason, "QUALIFIED" if qualified else "DONE")
                except Exception as exc:
                    logger.error("[Sheets] update failed: %s", exc)
            _send_report(lead, user_label, qualified, full_reason)
            e_call = html_lib.escape(normalize_phone(lead["phone"]))
            e_name = html_lib.escape(str(lead["name"]))
            send_message(NOTIFY_GROUP_ID,
                f"{'✅' if qualified else '❌'} Лид обработан\n📞 {e_call} | {e_name}\n"
                f"👤 {html_lib.escape(user_label)}\n💬 {html_lib.escape(reason)}")
            if qualified:
                processor_id = lead.get("assigned_to") or MAIN_QUALIFIER_ID
                with get_db() as conn:
                    conn.run("UPDATE leads SET updated_at = NOW(), reminder_count = 0 WHERE lead_id = :1", lead_id)
                send_message(processor_id,
                    "📋 Follow-up напоминания запланированы:\n• Через 1 день\n• Через 3 дня\n\nБот напомнит автоматически.")
            return

        if state and state["state_type"] == "followup":
            db_clear_pending_state(chat_id)
            lead_id        = state["lead_id"]
            followup_round = state["followup_round"]
            user_label     = get_user_label(sender)
            stage_text     = text_body.strip()
            lead = db_find_lead(lead_id)
            if not lead:
                send_message(chat_id, "❌ Лид не найден.")
                return
            e_phone  = html_lib.escape(normalize_phone(lead["phone"]))
            e_name   = html_lib.escape(str(lead["name"]))
            e_stage  = html_lib.escape(stage_text)
            e_by     = html_lib.escape(user_label)
            e_reason = html_lib.escape(str(lead.get("reason", "")))
            send_message(chat_id, f"✅ Ответ сохранён!\n<i>{e_stage}</i>")
            if followup_round == 1:
                db_advance_followup(lead_id, 1)
                send_message(OWNER_ID,
                    f"📊 <b>Follow-up (1 день)</b>\n\n"
                    f"{lead_info(lead_id, e_phone, e_name, lead['date_str'])}\n\n"
                    f"📋 Этап: {e_stage}\n👤 Ответил: <b>{e_by}</b>")
            else:
                db_advance_followup(lead_id, followup_round)
                with get_db() as conn:
                    conn.run("UPDATE leads SET status='FOLLOWUP_DONE' WHERE lead_id=:1", lead_id)
                send_message(OWNER_ID,
                    f"📊 <b>Финальный отчёт (3 дня)</b>\n\n"
                    f"{lead_info(lead_id, e_phone, e_name, lead['date_str'])}\n\n"
                    f"📋 Состояние: {e_stage}\n💬 Причина: {e_reason}\n👤 Кто обработал: <b>{e_by}</b>")
            return

    username       = sender.get("username")
    sender_display = f"@{username}" if username else (
        f"{sender.get('first_name', '')} {sender.get('last_name', '')}".strip() or str(sender.get("id", "?")))
    date_str = datetime.now().strftime("%d.%m.%Y")

    contact = msg.get("contact")
    if contact:
        phone = contact.get("phone_number", "")
        name  = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip() or sender_display
    else:
        entities = msg.get("entities", []) or msg.get("caption_entities", [])
        phone, parsed_name = parse_lead_text(text_body, entities)
        if not phone:
            return
        name = parsed_name or sender_display

    _now       = datetime.now()
    lead_id    = _now.strftime("%Y%m%d%H%M%S") + str(_now.microsecond // 1000).zfill(3)
    call_phone = normalize_phone(phone)
    e_call     = html_lib.escape(call_phone)
    e_name2    = html_lib.escape(str(name))
    e_sender   = html_lib.escape(str(sender_display))

    try:
        db_insert_lead(lead_id, date_str, name, phone)
        try:
            sheet_row = sheet_insert_lead(lead_id, date_str, name, phone)
            db_set_sheet_row(lead_id, sheet_row)
        except Exception as exc:
            logger.error("[Sheets] insert failed: %s", exc)
        send_message(OWNER_ID,
            f"📥 <b>Новый лид получен</b>\n\n{lead_info(lead_id, e_call, e_name2, date_str)}\n👤 Кто скинул: {e_sender}")
        send_message(MAIN_QUALIFIER_ID,
            f"📋 <b>Новый лид</b>\n\n{lead_info(lead_id, e_call, e_name2, date_str)}\n👤 Кто скинул: {e_sender}\n\nПримите или отклоните:",
            reply_markup=keyboard_main_qualifier(lead_id))
        send_message(NOTIFY_GROUP_ID, f"📥 Новый лид\n📞 {e_call} | {e_name2}\n👤 От: {e_sender}")
        logger.info("[AutoSave] lead_id=%s phone=%s name=%s", lead_id, phone, name)
    except Exception as exc:
        logger.error("[AutoSave] Failed: %s", exc)
        send_message(OWNER_ID, f"❌ Ошибка сохранения лида: {html_lib.escape(str(exc))}")


def handle_callback(cb):
    cb_id      = cb["id"]
    cb_data    = cb.get("data", "")
    chat_id    = cb["message"]["chat"]["id"]
    message_id = cb["message"]["message_id"]
    from_user  = cb.get("from", {})
    user_label = get_user_label(from_user)

    answer_callback(cb_id)
    edit_markup(chat_id, message_id)

    if cb_data.startswith("accept|"):
        lead_id = cb_data.split("|", 1)[1]
        lead    = db_find_lead(lead_id)
        if not lead:
            send_message(chat_id, "❌ Лид не найден."); return
        if lead["status"] not in ("PENDING", "ASSIGNED"):
            send_message(chat_id, "ℹ️ Лид уже обработан."); return
        if not db_claim_lead(lead_id, user_label, chat_id):
            send_message(chat_id, "ℹ️ Лид уже взят."); return
        e_call = html_lib.escape(normalize_phone(lead["phone"]))
        e_name = html_lib.escape(str(lead["name"]))
        send_message(chat_id,
            f"📋 <b>Этап квалификации</b>\n\n{lead_info(lead_id, e_call, e_name, lead['date_str'])}\n\nЛид квалифицированный?",
            reply_markup=keyboard_qualification(lead_id))

    elif cb_data.startswith("reject|"):
        if chat_id != MAIN_QUALIFIER_ID:
            send_message(chat_id, "⛔ У вас нет прав на это действие."); return
        lead_id = cb_data.split("|", 1)[1]
        lead    = db_find_lead(lead_id)
        if not lead:
            send_message(chat_id, "❌ Лид не найден."); return
        if lead["status"] not in ("PENDING", "ASSIGNED"):
            send_message(chat_id, "ℹ️ Лид уже обработан."); return
        db_assign_lead(lead_id, SECOND_QUALIFIER_ID)
        e_call = html_lib.escape(normalize_phone(lead["phone"]))
        e_name = html_lib.escape(str(lead["name"]))
        send_message(chat_id, "↩️ Лид передан второму квалификатору.")
        send_message(SECOND_QUALIFIER_ID,
            f"📋 <b>Новый лид для вас</b>\n\n{lead_info(lead_id, e_call, e_name, lead['date_str'])}\n\nПримите лид:",
            reply_markup=keyboard_second_qualifier(lead_id))
        send_message(OWNER_ID, f"↩️ Лид <code>{lead_id}</code> отклонён главным, передан второму квалификатору.")
        send_message(NOTIFY_GROUP_ID, f"↩️ Лид {lead_id} отклонён → передан второму квалификатору")

    elif cb_data.startswith("qual|"):
        parts = cb_data.split("|")
        if len(parts) != 3: return
        _, verdict, lead_id = parts
        lead = db_find_lead(lead_id)
        if not lead:
            send_message(chat_id, "❌ Лид не найден."); return
        if lead["status"] != "PROCESSING":
            send_message(chat_id, "ℹ️ Лид уже обработан."); return
        qualified = (verdict == "yes")
        db_set_pending_reason(chat_id, lead_id, qualified, user_label)
        if not qualified:
            send_message(chat_id, "❌ <b>Не квалифицированный</b>\n\nНапишите причину:")
        else:
            send_message(chat_id, "✅ <b>Квалифицированный</b>\n\nНапишите причину / комментарий:")


def _send_report(lead, processed_by, qualified, reason):
    try:
        created_at  = lead["created_at"]
        total_min   = int((datetime.now() - created_at).total_seconds() / 60)
        hours, mins = divmod(total_min, 60)
        qual_label  = "✅ Квалифицированный" if qualified else "❌ Не квалифицированный"
        send_message(OWNER_ID,
            f"📊 <b>Отчёт по лиду</b>\n\n"
            f"{lead_info(lead['lead_id'], html_lib.escape(normalize_phone(lead['phone'])), html_lib.escape(str(lead['name'])), lead['date_str'])}\n\n"
            f"⏱ Время обработки: {hours}ч {mins}мин\n"
            f"👤 Обработал: <b>{html_lib.escape(str(processed_by))}</b>\n"
            f"📋 Статус: {qual_label}\n"
            f"💬 Причина: {html_lib.escape(str(reason))}")
    except Exception as exc:
        logger.error("[Report] Failed: %s", exc)


def set_webhook():
    endpoint = f"{WEBHOOK_URL.rstrip('/')}/webhook"
    resp = requests.post(f"{API_BASE}/setWebhook",
        json={"url": endpoint, "allowed_updates": ["message", "callback_query"]}, timeout=10)
    result = resp.json()
    if result.get("ok"):
        logger.info("Webhook set: %s", endpoint)
    else:
        logger.error("Webhook failed: %s", result)


def main():
    init_db()
    set_webhook()
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(send_reminders, "interval", minutes=REMINDER_INTERVAL_MIN, id="reminders")
    scheduler.add_job(send_followups,  "interval", hours=1, id="followups")
    scheduler.start()
    logger.info("[Scheduler] Started")
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


# Init on gunicorn startup too
try:
    init_db()
    set_webhook()
    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_job(send_reminders, "interval", minutes=REMINDER_INTERVAL_MIN, id="reminders")
    _scheduler.add_job(send_followups,  "interval", hours=1, id="followups")
    _scheduler.start()
    logger.info("[Scheduler] Started via gunicorn")
except Exception as e:
    logger.error("Startup error: %s", e)


if __name__ == "__main__":
    main()
