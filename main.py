import os
import re
import json
import logging
from datetime import datetime
from flask import Flask, request, abort
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

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

# Состояние ожидания причины: chat_id -> {'row': int, 'qualified': bool}
pending_reason: dict = {}


def get_sheets_service():
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)
    return service.spreadsheets()


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
        json={
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": json.dumps({"inline_keyboard": []}),
        },
        timeout=10,
    )


def insert_row_to_sheet(date_str, name, phone):
    sheets = get_sheets_service()
    body = {"requests": [
        {
            "insertDimension": {
                "range": {
                    "sheetId": 0,
                    "dimension": "ROWS",
                    "startIndex": 3,
                    "endIndex": 4,
                },
                "inheritFromBefore": False,
            }
        }
    ]}
    sheets.batchUpdate(spreadsheetId=SHEETS_ID, body=body).execute()

    values = [["", date_str, name, phone, ""]]
    sheets.values().update(
        spreadsheetId=SHEETS_ID,
        range="A4:E4",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()
    logger.info("Row inserted: date=%s name=%s phone=%s", date_str, name, phone)


def update_reason_in_sheet(row_num, reason):
    sheets = get_sheets_service()
    sheets.values().update(
        spreadsheetId=SHEETS_ID,
        range=f"E{row_num}",
        valueInputOption="USER_ENTERED",
        body={"values": [[reason]]},
    ).execute()
    logger.info("Reason saved to E%d: %s", row_num, reason)


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

    # Если ждём причину от пользователя-квалификатора
    if chat_id in pending_reason and text_body:
        state = pending_reason.pop(chat_id)
        row_num = state["row"]
        qualified = state["qualified"]
        reason = text_body.strip()

        label = "Квалифицированный ✅" if qualified else "Не квалифицированный ❌"
        full_reason = f"{label}: {reason}"

        try:
            update_reason_in_sheet(row_num, full_reason)
            send_message(chat_id, f"✅ Причина сохранена в таблицу!\n<i>{full_reason}</i>")
            logger.info("Reason saved row=%d qualified=%s reason=%s", row_num, qualified, reason)
        except Exception as exc:
            logger.error("Failed to save reason: %s", exc)
            send_message(chat_id, f"❌ Ошибка сохранения причины: {exc}")
        return

    # Иначе — обычная обработка контакта
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
        # Сначала ищем через Telegram entity (точное распознавание)
        for ent in entities:
            if ent.get("type") == "phone_number":
                offset = ent["offset"]
                length = ent["length"]
                phone = text_body[offset:offset + length]
                break
        # Запасной вариант — regex для любых номеров (7-15 цифр, с +, пробелами, дефисами, скобками)
        if not phone and text_body:
            match = re.search(r'\+?[\d][\d\s\-\(\)]{5,17}[\d]', text_body)
            if match:
                # Оставляем только цифры и ведущий +
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

    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ Записать", "callback_data": cb_save},
            {"text": "❌ Пропустить", "callback_data": "skip"},
        ]]
    }

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
                send_message(chat_id, f"✅ Записано в таблицу!\n📞 {phone} — {name}")
                logger.info("Saved to sheet: %s %s %s", date_str, name, phone)

                # Отправляем лид на квалификацию всем квалификаторам
                qual_text = (
                    f"📋 <b>Новый лид на квалификацию</b>\n"
                    f"📞 Телефон: <code>{phone}</code>\n"
                    f"👤 Имя: {name}\n"
                    f"📅 Дата: {date_str}\n\n"
                    f"Квалифицированный?"
                )
                # callback_data: qual|yes|4 или qual|no|4  (row=4)
                qual_keyboard = {
                    "inline_keyboard": [[
                        {"text": "✅ Квалифицированный", "callback_data": "qual|yes|4"},
                        {"text": "❌ Не квалифицированный", "callback_data": "qual|no|4"},
                    ]]
                }
                for uid in QUALIFY_USER_IDS:
                    send_message(uid, qual_text, reply_markup=qual_keyboard)
                    logger.info("Qual message sent to %d for phone=%s", uid, phone)

            except Exception as exc:
                logger.error("Failed to insert row: %s", exc)
                send_message(chat_id, f"❌ Ошибка записи в таблицу: {exc}")
        else:
            send_message(chat_id, "❌ Ошибка: неверный формат данных.")

    elif cb_data.startswith("qual|"):
        # qual|yes|4 или qual|no|4
        parts = cb_data.split("|")
        if len(parts) == 3:
            _, verdict, row_str = parts
            try:
                row_num = int(row_str)
            except ValueError:
                row_num = 4
            qualified = (verdict == "yes")

            # Запоминаем состояние ожидания причины
            pending_reason[chat_id] = {"row": row_num, "qualified": qualified}

            label = "✅ Квалифицированный" if qualified else "❌ Не квалифицированный"
            send_message(chat_id, f"{label}\n\nНапишите причину:")
            logger.info("Qual answer=%s row=%d from chat=%d, waiting for reason", verdict, row_num, chat_id)
        else:
            send_message(chat_id, "❌ Ошибка: неверный формат данных квалификации.")

    elif cb_data == "skip":
        send_message(chat_id, "Пропущено ❌")
        logger.info("Contact skipped by owner")


def set_webhook():
    webhook_endpoint = f"{WEBHOOK_URL.rstrip('/')}/webhook"
    resp = requests.post(
        f"{API_BASE}/setWebhook",
        json={
            "url": webhook_endpoint,
            "allowed_updates": ["message", "callback_query"],
        },
        timeout=10,
    )
    result = resp.json()
    if result.get("ok"):
        logger.info("Webhook set successfully: %s", webhook_endpoint)
    else:
        logger.error("Failed to set webhook: %s", result)


if __name__ == "__main__":
    set_webhook()
    port = int(os.environ.get("PORT", 18609))
    logger.info("Starting Flask on port %d", port)
    app.run(host="0.0.0.0", port=port)
