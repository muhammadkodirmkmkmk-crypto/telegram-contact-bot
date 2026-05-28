import os, re, json, html as html_lib, logging, time
import pg8000
import urllib.parse
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

BOT_TOKEN               = os.environ["TELEGRAM_BOT_TOKEN"]
WEBHOOK_URL             = os.environ["WEBHOOK_URL"]
OWNER_ID                = int(os.environ["OWNER_TELEGRAM_ID"])
SHEETS_ID               = os.environ["GOOGLE_SHEETS_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]
DATABASE_URL            = os.environ["DATABASE_URL"]

MAIN_QUALIFIER_ID  = 514275093
SECOND_QUALIFIER_ID = 5028786313
NOTIFY_GROUP_ID    = -5160536788
API_BASE           = f"https://api.telegram.org/bot{BOT_TOKEN}"
SCOPES             = ["https://www.googleapis.com/auth/spreadsheets"]
REMINDER_INTERVAL_MIN = 30
DATA_START_ROW     = 4

# ─── DB ───────────────────────────────────────────────────────────────────────

def parse_db_url(url):
    r = urllib.parse.urlparse(url)
    return dict(host=r.hostname, port=r.port or 5432,
                database=r.path.lstrip("/"), user=r.username, password=r.password)

@contextmanager
def get_db():
    p = parse_db_url(DATABASE_URL)
    conn = pg8000.connect(host=p["host"], port=p["port"], database=p["database"],
                          user=p["user"], password=p["password"], ssl_context=True)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def qall(conn, sql, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or [])
    if cur.description:
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    return []

def qone(conn, sql, params=None):
    rows = qall(conn, sql, params)
    return rows[0] if rows else None

def qrun(conn, sql, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or [])
    return cur.rowcount

def init_db():
    with get_db() as conn:
        qrun(conn, """CREATE TABLE IF NOT EXISTS leads (
            lead_id TEXT PRIMARY KEY, date_str TEXT NOT NULL, name TEXT DEFAULT '',
            phone TEXT NOT NULL, reason TEXT DEFAULT '', status TEXT DEFAULT 'PENDING',
            assigned_to BIGINT DEFAULT 0, processed_by TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW(),
            reminder_count INT DEFAULT 0, sheet_row INT DEFAULT 0)""")
        qrun(conn, """CREATE TABLE IF NOT EXISTS pending_states (
            chat_id BIGINT PRIMARY KEY, state_type TEXT NOT NULL, lead_id TEXT NOT NULL,
            qualified BOOLEAN DEFAULT FALSE, user_label TEXT DEFAULT '',
            followup_round INT DEFAULT 0, created_at TIMESTAMP DEFAULT NOW())""")
    logger.info("[DB] Tables ready")

# ─── DB helpers ───────────────────────────────────────────────────────────────

def db_insert_lead(lead_id, date_str, name, phone):
    with get_db() as conn:
        qrun(conn, """INSERT INTO leads (lead_id,date_str,name,phone,status,created_at,updated_at)
            VALUES (%s,%s,%s,%s,'PENDING',NOW(),NOW()) ON CONFLICT (lead_id) DO NOTHING""",
            [lead_id, date_str, name or '', phone])

def db_find_lead(lead_id):
    with get_db() as conn:
        return qone(conn, "SELECT * FROM leads WHERE lead_id=%s", [lead_id])

def db_assign_lead(lead_id, qualifier_id):
    with get_db() as conn:
        qrun(conn, "UPDATE leads SET assigned_to=%s,status='ASSIGNED',updated_at=NOW() WHERE lead_id=%s",
             [qualifier_id, lead_id])

def db_claim_lead(lead_id, user_label, qualifier_id):
    with get_db() as conn:
        n = qrun(conn, """UPDATE leads SET status='PROCESSING',processed_by=%s,assigned_to=%s,updated_at=NOW()
            WHERE lead_id=%s AND status IN ('PENDING','ASSIGNED')""",
            [user_label, qualifier_id, lead_id])
        return n > 0

def db_mark_processed(lead_id, reason, qualified):
    status = 'QUALIFIED' if qualified else 'DONE'
    with get_db() as conn:
        qrun(conn, "UPDATE leads SET status=%s,reason=%s,updated_at=NOW(),reminder_count=0 WHERE lead_id=%s",
             [status, reason, lead_id])

def db_advance_followup(lead_id, new_round):
    with get_db() as conn:
        qrun(conn, "UPDATE leads SET reminder_count=%s,updated_at=NOW() WHERE lead_id=%s", [new_round, lead_id])

def db_get_pending_leads():
    cutoff = datetime.now() - timedelta(minutes=REMINDER_INTERVAL_MIN)
    with get_db() as conn:
        return qall(conn, "SELECT * FROM leads WHERE status IN ('PENDING','ASSIGNED') AND updated_at<=%s", [cutoff])

def db_get_qualified_for_followup(days):
    expected_round = 0 if days == 1 else 1
    cutoff = datetime.now() - timedelta(days=days)
    with get_db() as conn:
        return qall(conn, "SELECT * FROM leads WHERE status='QUALIFIED' AND reminder_count=%s AND updated_at<=%s",
                    [expected_round, cutoff])

def db_set_sheet_row(lead_id, sheet_row):
    with get_db() as conn:
        qrun(conn, "UPDATE leads SET sheet_row=%s WHERE lead_id=%s", [sheet_row, lead_id])

def db_increment_reminder(lead_id):
    with get_db() as conn:
        qrun(conn, "UPDATE leads SET reminder_count=reminder_count+1,updated_at=NOW() WHERE lead_id=%s", [lead_id])

def db_set_pending_reason(chat_id, lead_id, qualified, user_label):
    with get_db() as conn:
        qrun(conn, """INSERT INTO pending_states (chat_id,state_type,lead_id,qualified,user_label,created_at)
            VALUES (%s,'reason',%s,%s,%s,NOW())
            ON CONFLICT (chat_id) DO UPDATE SET state_type='reason',lead_id=%s,qualified=%s,user_label=%s,created_at=NOW()""",
            [chat_id, lead_id, qualified, user_label, lead_id, qualified, user_label])

def db_set_pending_followup(chat_id, lead_id, followup_round, user_label):
    with get_db() as conn:
        qrun(conn, """INSERT INTO pending_states (chat_id,state_type,lead_id,followup_round,user_label,created_at)
            VALUES (%s,'followup',%s,%s,%s,NOW())
            ON CONFLICT (chat_id) DO UPDATE SET state_type='followup',lead_id=%s,followup_round=%s,user_label=%s,created_at=NOW()""",
            [chat_id, lead_id, followup_round, user_label, lead_id, followup_round, user_label])

def db_get_pending_state(chat_id):
    with get_db() as conn:
        return qone(conn, "SELECT * FROM pending_states WHERE chat_id=%s", [chat_id])

def db_clear_pending_state(chat_id):
    with get_db() as conn:
        qrun(conn, "DELETE FROM pending_states WHERE chat_id=%s", [chat_id])

# ─── Keyboards ────────────────────────────────────────────────────────────────

def keyboard_main(lead_id):
    return {"inline_keyboard": [[
        {"text": "✅ Принять",  "callback_data": f"accept|{lead_id}"},
        {"text": "❌ Отказать", "callback_data": f"reject|{lead_id}"},
    ]]}

def keyboard_second(lead_id):
    return {"inline_keyboard": [[{"text": "✅ Принять", "callback_data": f"accept|{lead_id}"}]]}

def keyboard_qual(lead_id):
    return {"inline_keyboard": [[
        {"text": "✅ Квалифицированный",    "callback_data": f"qual|yes|{lead_id}"},
        {"text": "❌ Не квалифицированный", "callback_data": f"qual|no|{lead_id}"},
    ]]}

# ─── Google Sheets ────────────────────────────────────────────────────────────

def get_sheets():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS_JSON), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds).spreadsheets()

def sheets_call(fn, retries=3):
    for i in range(1, retries+1):
        try:
            return fn()
        except Exception as e:
            logger.warning("[Sheets] %d/%d: %s", i, retries, e)
            if i < retries: time.sleep(2**i)
            else: raise

def sheet_insert_lead(lead_id, date_str, name, phone):
    sheets = get_sheets()
    def _do():
        # Вставляем новую строку сразу после заголовка (строка 4)
        sheets.batchUpdate(
            spreadsheetId=SHEETS_ID,
            body={"requests": [{"insertDimension": {
                "range": {"sheetId": 0, "dimension": "ROWS",
                          "startIndex": DATA_START_ROW - 1, "endIndex": DATA_START_ROW},
                "inheritFromBefore": False
            }}]}
        ).execute()
        sheets.values().update(
            spreadsheetId=SHEETS_ID,
            range=f"A{DATA_START_ROW}:F{DATA_START_ROW}",
            valueInputOption="USER_ENTERED",
            body={"values": [[lead_id, date_str, name, phone, "", "PENDING"]]}
        ).execute()
        # Узнаём реальную строку этого лида (ищем по lead_id)
        res = sheets.values().get(spreadsheetId=SHEETS_ID, range="A:A").execute()
        values = res.get("values", [])
        for idx, row_val in enumerate(values):
            if row_val and row_val[0] == lead_id:
                return idx + 1  # 1-based
        return DATA_START_ROW
    return sheets_call(_do)

def sheet_find_row(lead_id):
    """Найти реальную строку лида по ID."""
    sheets = get_sheets()
    def _do():
        res = sheets.values().get(spreadsheetId=SHEETS_ID, range="A:A").execute()
        values = res.get("values", [])
        for idx, row_val in enumerate(values):
            if row_val and row_val[0] == lead_id:
                return idx + 1
        return None
    return sheets_call(_do)

def sheet_update_row(sheet_row, reason, status):
    if not sheet_row: return
    sheets = get_sheets()
    sheets_call(lambda: sheets.values().update(
        spreadsheetId=SHEETS_ID, range=f"E{sheet_row}:F{sheet_row}",
        valueInputOption="USER_ENTERED", body={"values": [[reason, status]]}).execute())

# ─── Phone & parser ───────────────────────────────────────────────────────────

def normalize_phone(raw):
    digits = re.sub(r'\D', '', str(raw))
    if str(raw).strip().startswith('+'): return f"+{digits}"
    if digits.startswith('998') and len(digits) == 12: return f"+{digits}"
    if len(digits) == 9: return f"+998{digits}"
    if digits.startswith('8') and len(digits) == 11: return f"+7{digits[1:]}"
    return f"+{digits}"

# Принимаем ЛЮБОЙ номер — минимум 5 цифр подряд
PHONE_RE = re.compile(r'(\+?[\d][\d\s\-\.\(\)]{3,25}[\d])')
NAME_LABEL_RE = re.compile(r'^(?:имя|name|клиент|client|фио|от|from)\s*[:\-]?\s*', re.IGNORECASE)
PHONE_LABEL_RE = re.compile(r'^(?:телефон|тел|phone|tel|моб|mob|номер|number)\s*[:\-]?\s*', re.IGNORECASE)

def extract_phone(raw):
    """Извлекает номер — принимает всё у чего >= 5 цифр."""
    cleaned = re.sub(r'[\s\-\.\(\)]', '', raw)
    digits = re.sub(r'\D', '', cleaned)
    return cleaned if len(digits) >= 5 else None

def parse_lead_text(text_body, entities):
    if not text_body: return None, None
    lines = [l.strip() for l in text_body.split('\n') if l.strip()]

    # 1. Telegram entity phone_number (самый надёжный)
    for ent in (entities or []):
        if ent.get("type") == "phone_number":
            phone = text_body[ent["offset"]: ent["offset"] + ent["length"]]
            name = None
            for line in lines:
                c = NAME_LABEL_RE.sub('', line).strip()
                if c and re.search(r'[a-zA-Zа-яА-ЯёЁ]', c) and not re.search(r'\d{5}', c):
                    name = c; break
            return phone, name

    # 2. Ищем любую строку с цифрами >= 5
    phone = None
    phone_line_idx = None
    for i, line in enumerate(lines):
        digits_in_line = re.sub(r'\D', '', line)
        if len(digits_in_line) >= 5:
            m = PHONE_RE.search(line)
            if m:
                c = extract_phone(m.group(0))
                if c:
                    phone = c
                    phone_line_idx = i
                    break

    if not phone: return None, None

    # 3. Ищем имя рядом с номером
    name = None
    for i, line in enumerate(lines):
        if i == phone_line_idx: continue
        c = NAME_LABEL_RE.sub('', line).strip()
        if not c: continue
        if not re.search(r'[a-zA-Zа-яА-ЯёЁ]', c): continue
        if len(re.sub(r'\D', '', c)) >= 5: continue  # пропускаем строки с номерами
        name = c
        break

    return phone, name

# ─── Telegram ─────────────────────────────────────────────────────────────────

def send_message(chat_id, text, reply_markup=None):
    if not chat_id or not text: return None
    payload = {"chat_id": chat_id, "text": str(text)[:4096], "parse_mode": "HTML"}
    if reply_markup: payload["reply_markup"] = json.dumps(reply_markup)
    try:
        r = requests.post(f"{API_BASE}/sendMessage", json=payload, timeout=10)
        return r.json()
    except Exception as e:
        logger.error("[TG] sendMessage %s: %s", chat_id, e)
        return None

def answer_callback(cq_id, text=""):
    try: requests.post(f"{API_BASE}/answerCallbackQuery",
                       json={"callback_query_id": cq_id, "text": text}, timeout=10)
    except Exception as e: logger.error("[TG] answerCB: %s", e)

def edit_markup(chat_id, message_id, markup=None):
    try: requests.post(f"{API_BASE}/editMessageReplyMarkup",
                       json={"chat_id": chat_id, "message_id": message_id,
                             "reply_markup": json.dumps(markup or {"inline_keyboard": []})}, timeout=10)
    except Exception as e: logger.error("[TG] editMarkup: %s", e)

def get_user_label(u):
    uname = u.get("username")
    return f"@{uname}" if uname else (f"{u.get('first_name','')} {u.get('last_name','')}".strip() or str(u.get("id","?")))

def lead_info(lead_id, e_call, e_name, date_str):
    return (f"🆔 ID: <code>{lead_id}</code>\n📞 Телефон: {e_call}\n👤 Имя: {e_name}\n📅 Дата: {date_str}")

# ─── Schedulers ───────────────────────────────────────────────────────────────

def send_reminders():
    logger.info("[Reminder] Checking...")
    try: leads = db_get_pending_leads()
    except Exception as e: logger.error("[Reminder] DB: %s", e); return
    for lead in leads:
        lid = lead["lead_id"]
        phone = normalize_phone(lead["phone"])
        n = lead["reminder_count"] + 1
        elapsed = int((datetime.now() - lead["created_at"]).total_seconds() / 60)
        text = (f"⏰ <b>Напоминание #{n} — необработанный лид!</b>\n\n"
                f"{lead_info(lid, html_lib.escape(phone), html_lib.escape(str(lead['name'])), lead['date_str'])}\n"
                f"🕐 Ожидает: {elapsed} мин.\n\nПримите или отклоните:")
        send_message(OWNER_ID, text, reply_markup=keyboard_main(lid))
        send_message(MAIN_QUALIFIER_ID, text, reply_markup=keyboard_main(lid))
        if lead["status"] == "ASSIGNED" and lead["assigned_to"] == SECOND_QUALIFIER_ID:
            send_message(SECOND_QUALIFIER_ID, text, reply_markup=keyboard_second(lid))
        try: db_increment_reminder(lid)
        except Exception as e: logger.error("[Reminder] inc: %s", e)

def send_followups():
    logger.info("[FollowUp] Checking...")
    for days, label in [(1, "1-дневный"), (3, "3-дневный")]:
        try: leads = db_get_qualified_for_followup(days)
        except Exception as e: logger.error("[FollowUp] DB: %s", e); continue
        for lead in leads:
            lid = lead["lead_id"]
            to  = lead.get("assigned_to") or MAIN_QUALIFIER_ID
            ep  = html_lib.escape(normalize_phone(lead["phone"]))
            en  = html_lib.escape(str(lead["name"]))
            rnd = lead["reminder_count"] + 1
            send_message(to, f"📋 <b>{label} follow-up</b>\n\n{lead_info(lid,ep,en,lead['date_str'])}\n\nЧто сейчас? На каком этапе?\n<i>Напишите ответ текстом</i>")
            db_set_pending_followup(to, lid, rnd, str(to))
            db_advance_followup(lid, rnd)

# ─── Webhook ──────────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        if not data: return "ok", 200
        if "callback_query" in data: handle_callback(data["callback_query"])
        elif "message" in data: handle_message(data["message"])
    except Exception as e:
        logger.error("Webhook error: %s", e, exc_info=True)
    return "ok", 200

@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200

def handle_message(msg):
    sender    = msg.get("from", {})
    chat_id   = msg.get("chat", {}).get("id") or sender.get("id")
    text_body = msg.get("text", "") or msg.get("caption", "")

    # /stats
    if text_body and text_body.startswith("/stats") and chat_id in (OWNER_ID, MAIN_QUALIFIER_ID, SECOND_QUALIFIER_ID):
        today = datetime.now().strftime("%d.%m.%Y")
        month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        with get_db() as conn:
            total         = qone(conn, "SELECT COUNT(*) as cnt FROM leads")["cnt"]
            today_count   = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE date_str=%s", [today])["cnt"]
            month_count   = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE created_at>=%s", [month_start])["cnt"]
            qual_count    = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE status IN ('QUALIFIED','FOLLOWUP_DONE')")["cnt"]
            done_count    = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE status='DONE'")["cnt"]
            pending_count = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE status IN ('PENDING','ASSIGNED')")["cnt"]
            by_user       = qall(conn, "SELECT processed_by, COUNT(*) as cnt FROM leads WHERE processed_by!='' GROUP BY processed_by ORDER BY cnt DESC")
        qual_rate = round(qual_count / max(qual_count + done_count, 1) * 100)
        user_lines = "".join(f"  👤 {html_lib.escape(r['processed_by'])}: {r['cnt']}\n" for r in by_user if r['processed_by'])
        stats = (f"📊 <b>Статистика лидов</b>\n\n📅 Сегодня: <b>{today_count}</b>\n📆 За месяц: <b>{month_count}</b>\n"
                 f"📦 Всего: <b>{total}</b>\n\n✅ Квалифицированных: {qual_count}\n❌ Не квалифицированных: {done_count}\n"
                 f"⏳ Ожидают: {pending_count}\n📈 Конверсия: {qual_rate}%\n")
        if user_lines: stats += f"\n<b>По квалификаторам:</b>\n{user_lines}"
        send_message(chat_id, stats); return

    # /today — статистика за последние 24 часа
    if text_body and text_body.startswith("/today") and chat_id in (OWNER_ID, MAIN_QUALIFIER_ID, SECOND_QUALIFIER_ID):
        since = datetime.now() - timedelta(hours=24)
        with get_db() as conn:
            total_24    = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE created_at >= %s", [since])["cnt"]
            qual_24     = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE created_at >= %s AND status IN ('QUALIFIED','FOLLOWUP_DONE')", [since])["cnt"]
            not_qual_24 = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE created_at >= %s AND status = 'DONE'", [since])["cnt"]
            pending_24  = qone(conn, "SELECT COUNT(*) as cnt FROM leads WHERE created_at >= %s AND status IN ('PENDING','ASSIGNED','PROCESSING')", [since])["cnt"]
        rate = round(qual_24 / max(qual_24 + not_qual_24, 1) * 100)
        send_message(chat_id,
            f"📊 <b>Статистика за последние 24 часа</b>\n\n"
            f"📦 Всего лидов: <b>{total_24}</b>\n"
            f"✅ Квалифицированных: <b>{qual_24}</b>\n"
            f"❌ Не квалифицированных: <b>{not_qual_24}</b>\n"
            f"⏳ Ожидают обработки: <b>{pending_24}</b>\n"
            f"📈 Конверсия: <b>{rate}%</b>")
        return

    # /resend_today
    if text_body and text_body.startswith("/resend_today") and chat_id == OWNER_ID:
        today = datetime.now().strftime("%d.%m.%Y")
        with get_db() as conn:
            leads = qall(conn, "SELECT * FROM leads WHERE date_str=%s", [today])
        if not leads: send_message(OWNER_ID, f"ℹ️ Сегодня ({today}) лидов нет."); return
        send_message(OWNER_ID, f"📤 Отправляю {len(leads)} лидов...")
        for lead in leads:
            lid = lead["lead_id"]
            send_message(MAIN_QUALIFIER_ID,
                f"📋 <b>Лид за {today} (повтор)</b>\n\n{lead_info(lid, html_lib.escape(normalize_phone(lead['phone'])), html_lib.escape(str(lead['name'])), today)}\n\nПримите или отклоните:",
                reply_markup=keyboard_main(lid))
        send_message(OWNER_ID, f"✅ Отправлено {len(leads)} лидов."); return

    # Pending state
    if chat_id and text_body:
        state = db_get_pending_state(chat_id)

        if state and state["state_type"] == "reason":
            db_clear_pending_state(chat_id)
            lead_id = state["lead_id"]; qualified = state["qualified"]; user_label = state["user_label"]
            reason = text_body.strip()
            lead = db_find_lead(lead_id)
            if not lead: send_message(chat_id, "❌ Лид не найден."); return
            if lead["status"] != "PROCESSING": send_message(chat_id, "ℹ️ Лид уже обработан."); return
            label = "Квалифицированный ✅" if qualified else "Не квалифицированный ❌"
            full_reason = f"{label} | {user_label}: {reason}"
            db_mark_processed(lead_id, full_reason, qualified=qualified)
            send_message(chat_id, f"✅ Сохранено!\n<i>{html_lib.escape(full_reason)}</i>")
            if lead.get("sheet_row"):
                try:
                    # Если sheet_row=4 для всех (старый баг), ищем реальную строку
                    sheet_row = lead["sheet_row"]
                    try:
                        real_row = sheet_find_row(lead["lead_id"])
                        if real_row: sheet_row = real_row
                    except Exception: pass
                    sheet_update_row(sheet_row, full_reason, "QUALIFIED" if qualified else "DONE")
                except Exception as e: logger.error("[Sheets] %s", e)
            _send_report(lead, user_label, qualified, full_reason)
            ep = html_lib.escape(normalize_phone(lead["phone"])); en = html_lib.escape(str(lead["name"]))
            # В группу — итог + причина
            send_message(NOTIFY_GROUP_ID,
                f"{'✅' if qualified else '❌'} <b>Лид обработан</b>\n"
                f"📞 {ep} | 👤 {en}\n"
                f"💬 Причина: {html_lib.escape(reason)}")
            if qualified:
                proc_id = lead.get("assigned_to") or MAIN_QUALIFIER_ID
                with get_db() as conn:
                    qrun(conn, "UPDATE leads SET updated_at=NOW(),reminder_count=0 WHERE lead_id=%s", [lead_id])
                send_message(proc_id, "📋 Follow-up запланирован:\n• Через 1 день\n• Через 3 дня")
            return

        if state and state["state_type"] == "followup":
            db_clear_pending_state(chat_id)
            lead_id = state["lead_id"]; followup_round = state["followup_round"]
            user_label = get_user_label(sender); stage_text = text_body.strip()
            lead = db_find_lead(lead_id)
            if not lead: send_message(chat_id, "❌ Лид не найден."); return
            ep = html_lib.escape(normalize_phone(lead["phone"])); en = html_lib.escape(str(lead["name"]))
            send_message(chat_id, f"✅ Ответ сохранён!\n<i>{html_lib.escape(stage_text)}</i>")
            if followup_round == 1:
                db_advance_followup(lead_id, 1)
                send_message(OWNER_ID, f"📊 <b>Follow-up (1 день)</b>\n\n{lead_info(lead_id,ep,en,lead['date_str'])}\n\n📋 Этап: {html_lib.escape(stage_text)}\n👤 Ответил: <b>{html_lib.escape(user_label)}</b>")
            else:
                db_advance_followup(lead_id, followup_round)
                with get_db() as conn:
                    qrun(conn, "UPDATE leads SET status='FOLLOWUP_DONE' WHERE lead_id=%s", [lead_id])
                send_message(OWNER_ID,
                    f"📊 <b>Финальный отчёт (3 дня)</b>\n\n{lead_info(lead_id,ep,en,lead['date_str'])}\n\n"
                    f"📋 Состояние: {html_lib.escape(stage_text)}\n💬 Причина: {html_lib.escape(str(lead.get('reason','')))}\n"
                    f"👤 Кто обработал: <b>{html_lib.escape(user_label)}</b>")
            return

    # Parse lead
    username = sender.get("username")
    sender_display = f"@{username}" if username else (
        f"{sender.get('first_name','')} {sender.get('last_name','')}".strip() or str(sender.get("id","?")))
    date_str = datetime.now().strftime("%d.%m.%Y")
    contact = msg.get("contact")
    if contact:
        phone = contact.get("phone_number", "")
        name  = f"{contact.get('first_name','')} {contact.get('last_name','')}".strip() or sender_display
    else:
        entities = msg.get("entities", []) or msg.get("caption_entities", [])
        phone, parsed_name = parse_lead_text(text_body, entities)
        if not phone: return
        name = parsed_name or sender_display

    _now = datetime.now()
    lead_id = _now.strftime("%Y%m%d%H%M%S") + str(_now.microsecond // 1000).zfill(3)
    call_phone = normalize_phone(phone)
    e_call = html_lib.escape(call_phone); e_name = html_lib.escape(str(name)); e_sender = html_lib.escape(str(sender_display))

    try:
        db_insert_lead(lead_id, date_str, name, phone)
        try:
            sheet_row = sheet_insert_lead(lead_id, date_str, name, phone)
            db_set_sheet_row(lead_id, sheet_row)
        except Exception as e: logger.error("[Sheets] %s", e)
        # Владельцу — уведомление + кнопки
        send_message(OWNER_ID, f"📥 <b>Новый лид получен</b>\n\n{lead_info(lead_id,e_call,e_name,date_str)}\n👤 Кто скинул: {e_sender}\n\nПримите или отклоните:",
                     reply_markup=keyboard_main(lead_id))
        # Главному квалификатору — с кнопками
        send_message(MAIN_QUALIFIER_ID, f"📋 <b>Новый лид</b>\n\n{lead_info(lead_id,e_call,e_name,date_str)}\n👤 Кто скинул: {e_sender}\n\nПримите или отклоните:",
                     reply_markup=keyboard_main(lead_id))
        # В группу — только краткое уведомление
        send_message(NOTIFY_GROUP_ID, f"📥 Новый лид\n📞 {e_call} | {e_name}\n👤 От: {e_sender}")
    except Exception as e:
        logger.error("[AutoSave] %s", e)
        send_message(OWNER_ID, f"❌ Ошибка: {html_lib.escape(str(e))}")

def handle_callback(cb):
    cb_id = cb["id"]; cb_data = cb.get("data","")
    chat_id = cb["message"]["chat"]["id"]; message_id = cb["message"]["message_id"]
    user_label = get_user_label(cb.get("from",{}))
    answer_callback(cb_id); edit_markup(chat_id, message_id)

    if cb_data.startswith("accept|"):
        lead_id = cb_data.split("|",1)[1]
        lead = db_find_lead(lead_id)
        if not lead: send_message(chat_id, "❌ Лид не найден."); return
        if lead["status"] not in ("PENDING","ASSIGNED"): send_message(chat_id, "ℹ️ Лид уже обработан."); return
        if not db_claim_lead(lead_id, user_label, chat_id): send_message(chat_id, "ℹ️ Лид уже взят."); return
        ep = html_lib.escape(normalize_phone(lead["phone"])); en = html_lib.escape(str(lead["name"]))
        send_message(chat_id, f"📋 <b>Этап квалификации</b>\n\n{lead_info(lead_id,ep,en,lead['date_str'])}\n\nЛид квалифицированный?",
                     reply_markup=keyboard_qual(lead_id))

    elif cb_data.startswith("reject|"):
        if chat_id not in (MAIN_QUALIFIER_ID, OWNER_ID): send_message(chat_id, "⛔ Нет прав."); return
        lead_id = cb_data.split("|",1)[1]
        lead = db_find_lead(lead_id)
        if not lead: send_message(chat_id, "❌ Лид не найден."); return
        if lead["status"] not in ("PENDING","ASSIGNED"): send_message(chat_id, "ℹ️ Лид уже обработан."); return
        db_assign_lead(lead_id, SECOND_QUALIFIER_ID)
        ep = html_lib.escape(normalize_phone(lead["phone"])); en = html_lib.escape(str(lead["name"]))
        send_message(chat_id, "↩️ Лид передан второму квалификатору.")
        send_message(SECOND_QUALIFIER_ID, f"📋 <b>Новый лид для вас</b>\n\n{lead_info(lead_id,ep,en,lead['date_str'])}\n\nПримите лид:",
                     reply_markup=keyboard_second(lead_id))
        send_message(OWNER_ID, f"↩️ Лид <code>{lead_id}</code> отклонён главным, передан второму квалификатору.")

    elif cb_data.startswith("qual|"):
        parts = cb_data.split("|")
        if len(parts) != 3: return
        _, verdict, lead_id = parts
        lead = db_find_lead(lead_id)
        if not lead: send_message(chat_id, "❌ Лид не найден."); return
        if lead["status"] != "PROCESSING": send_message(chat_id, "ℹ️ Лид уже обработан."); return
        qualified = (verdict == "yes")
        db_set_pending_reason(chat_id, lead_id, qualified, user_label)
        send_message(chat_id, "✅ <b>Квалифицированный</b>\n\nНапишите комментарий:" if qualified
                     else "❌ <b>Не квалифицированный</b>\n\nНапишите причину:")

def _send_report(lead, processed_by, qualified, reason):
    try:
        total_min = int((datetime.now() - lead["created_at"]).total_seconds() / 60)
        h, m = divmod(total_min, 60)
        send_message(OWNER_ID,
            f"📊 <b>Отчёт по лиду</b>\n\n"
            f"{lead_info(lead['lead_id'], html_lib.escape(normalize_phone(lead['phone'])), html_lib.escape(str(lead['name'])), lead['date_str'])}\n\n"
            f"⏱ Время: {h}ч {m}мин\n👤 Обработал: <b>{html_lib.escape(str(processed_by))}</b>\n"
            f"📋 {'✅ Квалифицированный' if qualified else '❌ Не квалифицированный'}\n💬 {html_lib.escape(str(reason))}")
    except Exception as e: logger.error("[Report] %s", e)

def set_webhook():
    endpoint = f"{WEBHOOK_URL.rstrip('/')}/webhook"
    resp = requests.post(f"{API_BASE}/setWebhook",
        json={"url": endpoint, "allowed_updates": ["message","callback_query"]}, timeout=10)
    result = resp.json()
    logger.info("Webhook %s: %s", "set" if result.get("ok") else "FAILED", endpoint)

def main():
    init_db(); set_webhook()
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(send_reminders, "interval", minutes=REMINDER_INTERVAL_MIN, id="reminders")
    scheduler.add_job(send_followups,  "interval", hours=1, id="followups")
    scheduler.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

try:
    init_db(); set_webhook()
    _sched = BackgroundScheduler(timezone="UTC")
    _sched.add_job(send_reminders, "interval", minutes=REMINDER_INTERVAL_MIN, id="reminders")
    _sched.add_job(send_followups,  "interval", hours=1, id="followups")
    _sched.start()
    logger.info("[Scheduler] Started via gunicorn")
except Exception as e:
    logger.error("Startup error: %s", e)

if __name__ == "__main__":
    main()
