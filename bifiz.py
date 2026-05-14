import asyncio
import logging
import re
import os
from datetime import datetime
from decimal import Decimal

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (InlineKeyboardButton, InlineKeyboardMarkup,
                           InputMediaPhoto, ParseMode, CallbackQuery,
                           Message, LabeledPrice, ContentTypes)
from aiogram.utils import executor
from aiohttp import web
from telethon import TelegramClient, events
from telethon.errors import (SessionPasswordNeededError,
                             PhoneCodeExpiredError,
                             PhoneCodeInvalidError)
from telethon.sessions import StringSession
from tinydb import TinyDB, Query
from tinydb.storages import JSONStorage

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
SECOND_BOT_TOKEN = os.getenv("SECOND_BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
USERBOT_PHONE = os.getenv("USERBOT_PHONE")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
CRYPTO_BOT_TOKEN = os.getenv("CRYPTO_BOT_TOKEN")
API_SECRET = os.getenv("API_SECRET", "super_secret_key_123")

CHANNEL_ID = -1002308793392
CHANNEL_LINK = "https://t.me/+Y88DiqFlqBRiNjIy"
SUPPORT_USERNAME = "swordSar"
RULES_LINK = "https://telegra.ph/PhysicHub--Pravila-polzovaniya-servisom-05-06"
REVIEWS_LINK = "https://t.me/c/2308793392/8548"
BOT_USERNAME = "PhysicHubFiz_Bot"
STARS_BOT_USERNAME = "StarsPaPhuchic_Bot"

STARS_RATE = Decimal('2')

# Базы данных
db = TinyDB("bot_data.json", storage=JSONStorage, indent=4, ensure_ascii=False)
users_table = db.table("users")
accounts_table = db.table("accounts")
orders_table = db.table("orders")
pending_payments = db.table("pending_payments")
sold_accounts_table = db.table("sold_accounts")
referrals_table = db.table("referrals")
banned_table = db.table("banned")

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
second_bot = Bot(token=SECOND_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp_second = Dispatcher(second_bot, storage=storage)

# ==================== HTTP СЕРВЕР ====================
app = web.Application()

async def handle_topup(request):
    try:
        data = await request.json()
        if data.get("secret") != API_SECRET:
            return web.json_response({"status": "error", "message": "Неверный ключ"})
        user_id = int(data["user_id"])
        amount = float(data["amount"])
        user = get_user(user_id)
        if user:
            new_balance = user["balance"] + amount
            users_table.update({"balance": new_balance}, Query().user_id == user_id)
            try:
                await bot.send_message(user_id, f"✅ Баланс пополнен на {format_price(amount)} через Stars!")
            except:
                pass
            return web.json_response({"status": "ok"})
        return web.json_response({"status": "error", "message": "Юзер не найден"})
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)})

app.router.add_post("/topup", handle_topup)

# ==================== FSM ====================
class AddAccount(StatesGroup):
    waiting_country = State()
    waiting_type = State()
    waiting_year = State()
    waiting_price = State()
    waiting_phone = State()
    waiting_code = State()

class Broadcast(StatesGroup):
    waiting_text = State()

class TopUp(StatesGroup):
    waiting_crypto_amount = State()
    waiting_stars_amount = State()

class BanUser(StatesGroup):
    waiting_ban_id = State()

# ==================== ПРОВЕРКА БАНА ====================
def is_banned(user_id):
    banned = banned_table.get(Query().user_id == user_id)
    return banned is not None

async def check_ban_and_answer(message: Message):
    if is_banned(message.from_user.id):
        await message.answer("🚫 Ваш тикет закрыт.")
        return True
    return False

# ==================== ФУНКЦИИ ====================
def is_admin(user_id):
    return user_id in ADMIN_IDS

def get_user(user_id):
    return users_table.get(Query().user_id == user_id)

def create_user_if_not(user_id, username=None, referrer_id=None):
    user = get_user(user_id)
    if not user:
        users_table.insert({
            "user_id": user_id,
            "username": username or "Неизвестный",
            "balance": 0.0,
            "purchases": 0,
            "referrer_id": referrer_id,
            "created_at": datetime.now().isoformat()
        })
        if referrer_id and referrer_id != user_id:
            existing = referrals_table.get(
                (Query().user_id == referrer_id) & (Query().invited_user_id == user_id)
            )
            if not existing:
                referrals_table.insert({
                    "user_id": referrer_id,
                    "invited_user_id": user_id,
                    "has_purchased": False,
                    "reward_claimed": False
                })
    else:
        users_table.update({"username": username or user.get("username", "Неизвестный")}, Query().user_id == user_id)

def format_price(price):
    return f"{Decimal(str(price)).quantize(Decimal('0.01'))} ₽"

def get_referral_count(user_id):
    return len(referrals_table.search(Query().user_id == user_id))

def admin_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("➕ Добавить аккаунт", callback_data="admin_add"),
        InlineKeyboardButton("📊 Остатки", callback_data="admin_stock")
    )
    keyboard.add(
        InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast"),
        InlineKeyboardButton("💳 Пополнить юзеру", callback_data="admin_topup_user")
    )
    keyboard.add(
        InlineKeyboardButton("📋 Рефералы", callback_data="admin_referrals"),
        InlineKeyboardButton("🚫 Бан юзера", callback_data="admin_ban")
    )
    keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="main_menu"))
    return keyboard

def main_menu_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.row(
        InlineKeyboardButton("[👛] Купить аккаунт", callback_data="buy_regular"),
        InlineKeyboardButton("[👝] Аккаунт с отлегой", callback_data="buy_aged")
    )
    keyboard.row(InlineKeyboardButton("[🎩] Мой профиль", callback_data="profile"))
    keyboard.row(
        InlineKeyboardButton("[🗞] Правила", url=RULES_LINK),
        InlineKeyboardButton("[📓] Отзывы", url=REVIEWS_LINK)
    )
    if is_admin(user_id):
        keyboard.row(InlineKeyboardButton("🛠 Админ-панель", callback_data="admin_panel"))
    return keyboard

async def show_main_menu(user_id):
    caption = (
        "Добро пожаловать ✈️\n\n"
        "<i>Чем мы лучше других сервисов</i>\n"
        "<blockquote>Моментальная выдача аккаунта.\n"
        "Большой ассортимент аккаунтов.\n"
        "Лучшее качество аккаунтов.</blockquote>"
    )
    await bot.send_photo(user_id, "https://iili.io/BQyyE22.jpg", caption=caption, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard(user_id))

def get_country_keyboard(acc_type):
    keyboard = InlineKeyboardMarkup(row_width=2)
    accounts = accounts_table.search(Query().acc_type == acc_type)
    unique_countries = list(set(a["country_code"] for a in accounts))
    if not unique_countries:
        keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="main_menu"))
        return keyboard
    for country in unique_countries:
        account = accounts_table.get(Query().country_code == country)
        if account:
            keyboard.insert(InlineKeyboardButton(f"{account['country_flag']} {account['country_name']}", callback_data=f"country_{acc_type}_{country}"))
    keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="main_menu"))
    return keyboard

def get_years_keyboard(country_code):
    keyboard = InlineKeyboardMarkup(row_width=3)
    accounts = accounts_table.search((Query().country_code == country_code) & (Query().acc_type == "отлега"))
    years = sorted(list(set(a["year"] for a in accounts)), reverse=True)
    if not years:
        keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="buy_aged"))
        return keyboard
    for year in years:
        keyboard.insert(InlineKeyboardButton(str(year), callback_data=f"year_{country_code}_{year}"))
    keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="buy_aged"))
    return keyboard

def get_available_account(acc_type, country_code, year=None):
    search_q = (Query().acc_type == acc_type) & (Query().country_code == country_code)
    if year:
        search_q &= (Query().year == int(year))
    accounts = accounts_table.search(search_q)
    return accounts[0] if accounts else None

# ==================== CRYPTO BOT ====================
async def get_usdt_rate_from_crypto_bot():
    try:
        url = "https://pay.crypt.bot/api/getExchangeRates"
        headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                result = await resp.json()
                if result.get("ok"):
                    for rate in result["result"]:
                        if rate["source"] == "USDT" and rate["target"] == "RUB" and rate["is_valid"]:
                            return Decimal('1') / Decimal(rate["rate"])
        return Decimal('0.011')
    except:
        return Decimal('0.011')

async def create_crypto_invoice(amount_rub):
    try:
        rub_to_usdt = await get_usdt_rate_from_crypto_bot()
        amount_usd = Decimal(str(amount_rub)) * rub_to_usdt
        amount_usd = amount_usd.quantize(Decimal('0.01'))
        if amount_usd < Decimal('0.1'):
            amount_usd = Decimal('0.1')
        url = "https://pay.crypt.bot/api/createInvoice"
        headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
        data = {"asset": "USDT", "amount": str(amount_usd), "description": "Пополнение баланса", "expires_in": 1800}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as resp:
                result = await resp.json()
                if result.get("ok"):
                    return result["result"]["bot_invoice_url"], result["result"]["invoice_id"]
        return None, None
    except:
        return None, None

async def check_crypto_payment(invoice_id):
    try:
        url = "https://pay.crypt.bot/api/getInvoices"
        headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
        data = {"invoice_ids": invoice_id}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as resp:
                result = await resp.json()
                if result.get("ok") and result["result"].get("items"):
                    return result["result"]["items"][0]
    except:
        pass
    return None

# ==================== ЮЗЕРБОТ ====================
main_userbot = None
userbot_clients = {}

async def start_main_userbot():
    global main_userbot
    try:
        main_userbot = TelegramClient(StringSession(), API_ID, API_HASH)
        await main_userbot.connect()
        if not await main_userbot.is_user_authorized():
            await main_userbot.send_code_request(USERBOT_PHONE)
            for admin_id in ADMIN_IDS:
                await bot.send_message(admin_id, f"🔐 <b>Регистрация юзербота</b>\n\nНомер: {USERBOT_PHONE}\nОтправь код подтверждения.", parse_mode=ParseMode.HTML)
            return False
        else:
            logging.info("Юзербот уже авторизован")
            return True
    except Exception as e:
        logging.error(f"Main userbot error: {e}")
        return False

async def login_userbot_with_code(code):
    global main_userbot
    try:
        await main_userbot.sign_in(USERBOT_PHONE, code)
        for admin_id in ADMIN_IDS:
            await bot.send_message(admin_id, "✅ Юзербот авторизован!")
        return True
    except SessionPasswordNeededError:
        for admin_id in ADMIN_IDS:
            await bot.send_message(admin_id, "⚠️ Нужен облачный пароль!")
        return False
    except Exception as e:
        logging.error(f"Login error: {e}")
        return False

async def create_userbot_for_account(phone, account_id):
    try:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            await client.send_code_request(phone)
        userbot_clients[phone] = {"client": client, "account_id": account_id, "awaiting_code": True}
        
        @client.on(events.NewMessage(from_users=777000))
        async def code_handler(event):
            message_text = event.message.message
            code_match = re.search(r'\b(\d{5})\b', message_text)
            if code_match:
                code = code_match.group(1)
                orders = orders_table.search((Query().phone == phone) & (Query().status == "ожидает_код"))
                if orders:
                    order = orders[0]
                    await bot.send_message(order["buyer_id"], f"🔑 Код: <code>{code}</code>", parse_mode=ParseMode.HTML)
                    orders_table.update({"status": "завершен", "code": code}, Query().doc_id == order.doc_id)
        return client
    except Exception as e:
        logging.error(f"Create userbot error: {e}")
        return None

async def login_account_userbot(phone, code):
    if phone not in userbot_clients:
        return False
    client = userbot_clients[phone]["client"]
    account_id = userbot_clients[phone]["account_id"]
    try:
        await client.sign_in(phone, code)
        accounts_table.update({"has_session": True}, Query().doc_id == account_id)
        userbot_clients[phone]["awaiting_code"] = False
        return True
    except Exception as e:
        logging.error(f"Account login error: {e}")
        return False

# ==================== ОБРАБОТЧИК КОДОВ ====================
@dp.message_handler(lambda msg: is_admin(msg.from_user.id) and msg.text and len(msg.text) == 5 and msg.text.isdigit())
async def catch_code(message: Message):
    if await check_ban_and_answer(message):
        return
    code = message.text
    if main_userbot and not await main_userbot.is_user_authorized():
        success = await login_userbot_with_code(code)
        if success:
            await message.answer("✅ Юзербот авторизован!")
        return
    for phone, data in userbot_clients.items():
        if data.get("awaiting_code"):
            success = await login_account_userbot(phone, code)
            if success:
                await message.answer(f"✅ Вход для {phone} выполнен!")
                return

# ==================== /start ====================
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    if await check_ban_and_answer(message):
        return
    
    args = message.get_args()
    referrer_id = None
    if args.startswith("ref_"):
        try:
            referrer_id = int(args.replace("ref_", ""))
        except:
            pass
    
    create_user_if_not(message.from_user.id, message.from_user.username, referrer_id)
    
    try:
        member = await bot.get_chat_member(CHANNEL_ID, message.from_user.id)
        if member.status in ['creator', 'administrator', 'member']:
            await show_main_menu(message.from_user.id)
        else:
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(InlineKeyboardButton("🔔 Подписаться", url=CHANNEL_LINK))
            keyboard.add(InlineKeyboardButton("♻️ Проверить подписку", callback_data="check_sub"))
            await bot.send_photo(message.from_user.id, "https://iili.io/BQyyE22.jpg", caption="<b>Для использования бота подпишитесь на канал!</b>", parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except:
        await message.answer("❌ Ошибка")

# ==================== CALLBACK: ПОДПИСКА ====================
@dp.callback_query_handler(text="check_sub")
async def check_subscription(call: CallbackQuery):
    try:
        member = await bot.get_chat_member(CHANNEL_ID, call.from_user.id)
        if member.status in ['creator', 'administrator', 'member']:
            await call.message.delete()
            await show_main_menu(call.from_user.id)
        else:
            await call.answer("❌ Вы не подписаны!", show_alert=True)
    except:
        await call.answer("❌ Ошибка!")
    await call.answer()

# ==================== CALLBACK: ГЛАВНОЕ МЕНЮ ====================
@dp.callback_query_handler(text="main_menu")
async def back_to_main(call: CallbackQuery):
    await call.message.delete()
    await show_main_menu(call.from_user.id)
    await call.answer()

# ==================== CALLBACK: ПРОФИЛЬ ====================
@dp.callback_query_handler(text="profile")
async def show_profile(call: CallbackQuery):
    user = get_user(call.from_user.id)
    if not user:
        await call.answer("Ошибка!")
        return
    text = (
        "Профиль\n"
        "——————————————————\n"
        f"Имя пользователя: @{user.get('username', 'Неизвестный')}\n"
        f"Идентификатор: {call.from_user.id}\n"
        "——————————————————\n"
        f"👛 Баланс: {format_price(user.get('balance', 0))}\n"
        f"Покупок: {user.get('purchases', 0)}"
    )
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("[👜] Пополнить баланс", callback_data="top_up"))
    keyboard.add(InlineKeyboardButton("[⛓️] Реф программа", callback_data="referral"))
    keyboard.add(InlineKeyboardButton("[🦺] Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"))
    keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="main_menu"))
    await call.message.delete()
    await bot.send_photo(call.from_user.id, "https://iili.io/BZzNhN9.jpg", caption=text, reply_markup=keyboard)
    await call.answer()

# ==================== РЕФЕРАЛЬНАЯ ПРОГРАММА ====================
@dp.callback_query_handler(text="referral")
async def show_referral(call: CallbackQuery):
    ref_count = get_referral_count(call.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{call.from_user.id}"
    
    text = (
        "<b>⛓️ Реферальная программа</b>\n\n"
        "<blockquote>Условия получения аккаунта: необходимо пригласить 15 уникальных пользователей. "
        "При условии, что один из приглашённых совершит покупку любого товара в боте, "
        "вы имеете право на получение любого аккаунта из ассортимента.</blockquote>\n\n"
        f"Ваша ссылка:\n<code>{ref_link}</code>\n\n"
        f"📪 Вы пригласили: <b>{ref_count}</b> чел."
    )
    
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="profile"))
    
    await call.message.delete()
    await bot.send_message(call.from_user.id, text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

# ==================== ПОПОЛНЕНИЕ ====================
@dp.callback_query_handler(text="top_up")
async def top_up_menu(call: CallbackQuery):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.row(
        InlineKeyboardButton("[💳] СБП", callback_data="topup_sbp"),
        InlineKeyboardButton("[🔏] Crypto Bot", callback_data="topup_crypto")
    )
    keyboard.row(InlineKeyboardButton("[🪄] Звезды", callback_data="topup_stars"))
    keyboard.row(InlineKeyboardButton("[ ← Назад ]", callback_data="profile"))
    await call.message.delete()
    await bot.send_message(call.from_user.id, "<b>Выберите способ пополнения:</b>", parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(text="topup_sbp")
async def sbp_info(call: CallbackQuery):
    text = (
        "<b>Пополнение через СБП:</b>\n\n"
        "1. <b>Перевод по СБП</b>\n"
        "2. В поиске <b>ЮMoney</b>\n"
        "3. Номер: <code>+79646603227</code>\n"
        f"4. Комментарий: <code>{call.from_user.id}</code>\n"
        "5. Скрин чека в поддержку"
    )
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("[ ← Назад ]", callback_data="top_up"))
    await call.message.delete()
    await bot.send_message(call.from_user.id, text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(text="topup_crypto")
async def crypto_amount(call: CallbackQuery):
    await call.message.delete()
    await bot.send_message(call.from_user.id, "Введите сумму в рублях:")
    await TopUp.waiting_crypto_amount.set()
    await call.answer()

@dp.message_handler(state=TopUp.waiting_crypto_amount)
async def crypto_invoice(message: Message, state: FSMContext):
    if await check_ban_and_answer(message):
        await state.finish()
        return
    try:
        amount = Decimal(message.text.replace(",", "."))
        if amount < 10:
            await message.answer("Минимальная сумма: 10 ₽")
            return
        await message.answer("⏳ Создаю счёт...")
        invoice_url, invoice_id = await create_crypto_invoice(amount)
        if invoice_url:
            pending_payments.insert({"user_id": message.from_user.id, "amount": float(amount), "invoice_id": invoice_id, "status": "pending", "created_at": datetime.now().isoformat()})
            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("💳 Оплатить", url=invoice_url))
            keyboard.add(InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"check_crypto_{invoice_id}"))
            keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="top_up"))
            await message.answer(f"<b>Счёт на {format_price(amount)} создан!</b>", parse_mode=ParseMode.HTML, reply_markup=keyboard)
        else:
            await message.answer("❌ Не удалось создать счёт.")
    except:
        await message.answer("❌ Введите корректную сумму!")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda call: call.data.startswith("check_crypto_"))
async def check_crypto_payment_handler(call: CallbackQuery):
    invoice_id = call.data.replace("check_crypto_", "")
    invoice_data = await check_crypto_payment(invoice_id)
    if invoice_data and invoice_data["status"] == "paid":
        payment = pending_payments.get(Query().invoice_id == invoice_id)
        if payment:
            user = get_user(payment["user_id"])
            if user:
                new_balance = user["balance"] + payment["amount"]
                users_table.update({"balance": new_balance}, Query().user_id == payment["user_id"])
                await call.message.delete()
                await bot.send_message(call.from_user.id, f"✅ Баланс пополнен на {format_price(payment['amount'])}!")
                await call.answer("✅ Оплата прошла!")
                return
    await call.answer("❌ Оплата не прошла!", show_alert=True)

# ==================== STARS ====================
@dp.callback_query_handler(text="topup_stars")
async def stars_amount(call: CallbackQuery):
    await call.message.delete()
    await bot.send_message(call.from_user.id, "Введите сумму в рублях для пополнения через Stars:")
    await TopUp.waiting_stars_amount.set()
    await call.answer()

@dp.message_handler(state=TopUp.waiting_stars_amount)
async def stars_invoice(message: Message, state: FSMContext):
    if await check_ban_and_answer(message):
        await state.finish()
        return
    try:
        amount_rub = float(message.text)
        if amount_rub < 10:
            await message.answer("Минимальная сумма: 10 руб.")
            return
        amount_stars = int(amount_rub * 2)
        
        # Инициализируем чат со вторым ботом
        try:
            init_msg = await second_bot.send_message(
                message.from_user.id,
                f"💫 Счёт на {amount_stars} Stars"
            )
            await asyncio.sleep(0.5)
            await second_bot.delete_message(message.from_user.id, init_msg.message_id)
        except:
            pass
        
        # Выставляем счёт
        await second_bot.send_invoice(
            chat_id=message.from_user.id,
            title="Пополнение баланса",
            description=f"Пополнение на {format_price(amount_rub)}",
            payload=f"topup_{message.from_user.id}_{amount_rub}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label="Пополнение", amount=amount_stars)]
        )
        
        # Кнопка на второго бота
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("💳 Оплатить", url=f"https://t.me/{STARS_BOT_USERNAME}"))
        
        await message.answer(
            f"⭐ Счёт на {amount_stars} Stars выставлен. Оплатите его по кнопке ниже:",
            reply_markup=keyboard
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
    finally:
        await state.finish()

# ==================== ОБРАБОТЧИКИ ДЛЯ ВТОРОГО БОТА ====================
@dp_second.pre_checkout_query_handler(lambda query: True)
async def process_pre_checkout(pre_checkout_query: types.PreCheckoutQuery):
    await second_bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp_second.message_handler(content_types=ContentTypes.SUCCESSFUL_PAYMENT)
async def process_successful_payment(message: Message):
    payload = message.successful_payment.invoice_payload
    parts = payload.split('_')
    user_id = int(parts[1])
    amount_rub = float(parts[2])
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                "http://localhost:8080/topup",
                json={"user_id": user_id, "amount": amount_rub, "secret": API_SECRET}
            ) as resp:
                result = await resp.json()
                if result["status"] == "ok":
                    await message.answer("✅ Оплата получена! Баланс пополнен.")
                else:
                    await message.answer(f"❌ Ошибка: {result.get('message')}")
        except Exception as e:
            await message.answer(f"❌ Ошибка связи с ботом: {e}")

# ==================== ПОКУПКА ====================
@dp.callback_query_handler(text="buy_regular")
async def buy_regular(call: CallbackQuery):
    keyboard = get_country_keyboard("обычный")
    await call.message.delete()
    await bot.send_message(call.from_user.id, "<b>Покупка аккаунта</b>\n\nВыберите страну:", parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(text="buy_aged")
async def buy_aged(call: CallbackQuery):
    accounts = accounts_table.search(Query().acc_type == "отлега")
    if not accounts:
        await call.answer("❌ Нет доступных!", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(row_width=2)
    unique_countries = list(set(a["country_code"] for a in accounts))
    for country in unique_countries:
        country_data = accounts_table.get(Query().country_code == country)
        if country_data:
            keyboard.insert(InlineKeyboardButton(f"{country_data['country_flag']} {country_data['country_name']}", callback_data=f"aged_country_{country}"))
    keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data="main_menu"))
    await call.message.delete()
    await bot.send_message(call.from_user.id, "<b>Аккаунты с отлегой</b>\n\nВыберите страну:", parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(lambda call: call.data.startswith("aged_country_"))
async def aged_country_select(call: CallbackQuery):
    country_code = call.data.replace("aged_country_", "")
    keyboard = get_years_keyboard(country_code)
    await call.message.delete()
    await bot.send_message(call.from_user.id, "<b>Выберите год:</b>", parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(lambda call: call.data.startswith("country_"))
async def country_select(call: CallbackQuery):
    parts = call.data.split("_")
    acc_type = parts[1]
    country_code = parts[2]
    account = get_available_account(acc_type, country_code)
    if not account:
        await call.answer("❌ Нет доступных!", show_alert=True)
        return
    text = f"<b>{'Обычный аккаунт' if acc_type == 'обычный' else 'Аккаунт с отлегой'}</b>\n\n📍 Страна: {account['country_flag']} {account['country_name']}\n💵 Цена: {format_price(account['price'])}"
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("💰 Купить", callback_data=f"purchase_{account.doc_id}"))
    keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data=f"buy_{acc_type}"))
    await call.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(lambda call: call.data.startswith("year_"))
async def year_select(call: CallbackQuery):
    parts = call.data.split("_")
    country_code = parts[1]
    year = parts[2]
    account = get_available_account("отлега", country_code, year)
    if not account:
        await call.answer("❌ Нет доступных!", show_alert=True)
        return
    text = f"<b>Аккаунт с отлегой {year} года</b>\n\n📍 Страна: {account['country_flag']} {account['country_name']}\n📅 Год: {year}\n💵 Цена: {format_price(account['price'])}"
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("💰 Купить", callback_data=f"purchase_{account.doc_id}"))
    keyboard.add(InlineKeyboardButton("[ ← Назад ]", callback_data=f"aged_country_{country_code}"))
    await call.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(lambda call: call.data.startswith("purchase_"))
async def purchase_account(call: CallbackQuery):
    account_id = int(call.data.replace("purchase_", ""))
    account = accounts_table.get(doc_id=account_id)
    user = get_user(call.from_user.id)
    if not account:
        await call.answer("❌ Не найден!", show_alert=True)
        return
    if user["balance"] < account["price"]:
        await call.answer("❌ Недостаточно средств!", show_alert=True)
        return
    new_balance = user["balance"] - account["price"]
    users_table.update({"balance": new_balance, "purchases": user["purchases"] + 1}, Query().user_id == call.from_user.id)
    phone = account["phone"]
    price = account["price"]
    country_flag = account["country_flag"]
    country_name = account["country_name"]
    
    user_data = users_table.get(Query().user_id == call.from_user.id)
    if user_data and user_data.get("referrer_id"):
        referrals_table.update(
            {"has_purchased": True},
            (Query().user_id == user_data["referrer_id"]) & (Query().invited_user_id == call.from_user.id)
        )
    
    orders_table.insert({"phone": phone, "buyer_id": call.from_user.id, "status": "ожидает_код", "created_at": datetime.now().isoformat()})
    accounts_table.remove(doc_ids=[account_id])
    sold_accounts_table.insert({**account, "buyer_id": call.from_user.id, "sold_at": datetime.now().isoformat()})
    await call.message.delete()
    result_text = f"✅ <b>Аккаунт куплен!</b>\n\n📱 Номер: <code>{phone}</code>\n🌍 Страна: {country_flag} {country_name}\n💰 Цена: {format_price(price)}\n\n🔑 <b>Введите номер в Telegram.</b>\nКод придёт автоматически."
    await bot.send_message(call.from_user.id, result_text, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard(call.from_user.id))
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"💰 Продан {phone}\n👤 @{call.from_user.username}\n💵 {format_price(price)}")
        except:
            pass
    await call.answer("✅ Куплен!", show_alert=True)

# ==================== АДМИН-ПАНЕЛЬ ====================
@dp.callback_query_handler(text="admin_panel")
async def admin_panel(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Нет доступа!", show_alert=True)
        return
    await call.message.delete()
    await bot.send_message(call.from_user.id, "<b>🛠 Админ-панель</b>", parse_mode=ParseMode.HTML, reply_markup=admin_keyboard())
    await call.answer()

@dp.callback_query_handler(text="admin_add")
async def admin_add_start(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.message.delete()
    await bot.send_message(call.from_user.id, "<b>➕ Добавление аккаунта</b>\n\nВведите страну с флагом (например: 🇺🇸США):", parse_mode=ParseMode.HTML)
    await AddAccount.waiting_country.set()
    await call.answer()

@dp.message_handler(state=AddAccount.waiting_country)
async def admin_add_country(message: Message, state: FSMContext):
    await state.update_data(country=message.text)
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(InlineKeyboardButton("Обычный", callback_data="type_обычный"), InlineKeyboardButton("Отлега", callback_data="type_отлега"))
    await message.answer("Выберите тип:", reply_markup=keyboard)
    await AddAccount.waiting_type.set()

@dp.callback_query_handler(lambda call: call.data.startswith("type_"), state=AddAccount.waiting_type)
async def admin_add_type(call: CallbackQuery, state: FSMContext):
    acc_type = call.data.replace("type_", "")
    await state.update_data(acc_type=acc_type)
    if acc_type == "отлега":
        await call.message.delete()
        await bot.send_message(call.from_user.id, "Введите год регистрации:")
        await AddAccount.waiting_year.set()
    else:
        await call.message.delete()
        await bot.send_message(call.from_user.id, "Введите цену в рублях:")
        await AddAccount.waiting_price.set()
    await call.answer()

@dp.message_handler(state=AddAccount.waiting_year)
async def admin_add_year(message: Message, state: FSMContext):
    try:
        year = int(message.text)
        await state.update_data(year=year)
        await message.answer("Введите цену в рублях:")
        await AddAccount.waiting_price.set()
    except:
        await message.answer("❌ Введите год цифрами!")

@dp.message_handler(state=AddAccount.waiting_price)
async def admin_add_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.replace(",", "."))
        await state.update_data(price=price)
        await message.answer("Введите номер телефона:")
        await AddAccount.waiting_phone.set()
    except:
        await message.answer("❌ Введите цену цифрами!")

@dp.message_handler(state=AddAccount.waiting_phone)
async def admin_add_phone(message: Message, state: FSMContext):
    data = await state.get_data()
    phone = message.text
    country_input = data["country"]
    if len(country_input) >= 2 and ord(country_input[0]) > 127:
        flag = country_input[:2]
        name = country_input[2:].strip()
    else:
        flag = ""
        name = country_input
    account_data = {"country_name": name, "country_flag": flag, "country_code": name, "acc_type": data["acc_type"], "price": data["price"], "phone": phone, "has_session": False, "added_at": datetime.now().isoformat(), "year": data.get("year")}
    acc_id = accounts_table.insert(account_data)
    client = await create_userbot_for_account(phone, acc_id)
    if client:
        await message.answer(f"📱 Номер: {phone}\n🔐 <b>Введи код подтверждения:</b>", parse_mode=ParseMode.HTML)
        await state.update_data(phone=phone, acc_id=acc_id)
        await AddAccount.waiting_code.set()
    else:
        await message.answer("❌ Ошибка создания юзербота")
        await state.finish()

@dp.message_handler(state=AddAccount.waiting_code)
async def admin_add_code(message: Message, state: FSMContext):
    data = await state.get_data()
    phone = data["phone"]
    code = message.text
    success = await login_account_userbot(phone, code)
    if success:
        await message.answer(f"✅ Аккаунт {phone} готов!\n🏳️ Страна: {data.get('country_flag', '')} {data.get('country', '')}\n📦 Тип: {data.get('acc_type', '')}\n💵 Цена: {format_price(data.get('price', 0))}")
    else:
        await message.answer("❌ Неверный код!")
        return
    await state.finish()

@dp.callback_query_handler(text="admin_stock")
async def admin_stock(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    accounts = accounts_table.all()
    text = "<b>📊 Остатки:</b>\n\n"
    if not accounts:
        text += "Нет доступных."
    else:
        for country in set(a["country_name"] for a in accounts):
            flag_data = accounts_table.get(Query().country_name == country)
            flag = flag_data["country_flag"] if flag_data else ""
            regular = len(accounts_table.search((Query().country_name == country) & (Query().acc_type == "обычный")))
            aged = len(accounts_table.search((Query().country_name == country) & (Query().acc_type == "отлега")))
            text += f"{flag} {country}: {regular} обычных, {aged} с отлегой\n"
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("[ ← Назад ]", callback_data="admin_panel"))
    await call.message.delete()
    await bot.send_message(call.from_user.id, text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(text="admin_broadcast")
async def admin_broadcast(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.message.delete()
    await bot.send_message(call.from_user.id, "Введите текст для рассылки:")
    await Broadcast.waiting_text.set()
    await call.answer()

@dp.message_handler(state=Broadcast.waiting_text, content_types=ContentTypes.ANY)
async def broadcast_send(message: Message, state: FSMContext):
    users = users_table.all()
    success = 0
    failed = 0
    for user in users:
        try:
            await message.copy_to(user["user_id"])
            success += 1
        except:
            failed += 1
        await asyncio.sleep(0.05)
    await message.answer(f"✅ Рассылка завершена!\nУспешно: {success}\nНеудачно: {failed}")
    await state.finish()

@dp.callback_query_handler(text="admin_topup_user")
async def admin_topup_user(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.message.delete()
    await bot.send_message(call.from_user.id, "Введите ID и сумму:\n<code>123456789 100</code>", parse_mode=ParseMode.HTML)
    
    @dp.message_handler(lambda msg: is_admin(msg.from_user.id) and len(msg.text.split()) == 2)
    async def process_topup(msg: Message):
        try:
            parts = msg.text.split()
            user_id = int(parts[0])
            amount = float(parts[1])
            user = get_user(user_id)
            if user:
                new_balance = user["balance"] + amount
                users_table.update({"balance": new_balance}, Query().user_id == user_id)
                await msg.answer(f"✅ Баланс {user_id} пополнен на {format_price(amount)}")
            else:
                await msg.answer("❌ Не найден!")
        except:
            await msg.answer("❌ Неверный формат!")
    await call.answer()

# ==================== АДМИН: РЕФЕРАЛЫ ====================
@dp.callback_query_handler(text="admin_referrals")
async def admin_referrals(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    
    all_refs = referrals_table.all()
    if not all_refs:
        await call.answer("Нет рефералов", show_alert=True)
        return
    
    stats = {}
    for ref in all_refs:
        uid = ref["user_id"]
        if uid not in stats:
            stats[uid] = {"total": 0, "purchased": 0}
        stats[uid]["total"] += 1
        if ref.get("has_purchased"):
            stats[uid]["purchased"] += 1
    
    text = "<b>📋 Статистика рефералов:</b>\n\n"
    for uid, data in sorted(stats.items(), key=lambda x: x[1]["total"], reverse=True):
        user = get_user(uid)
        username = user["username"] if user else "Неизвестный"
        text += f"@{username} (ID: {uid})\nПригласил: {data['total']} | Купили: {data['purchased']}\n\n"
    
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("[ ← Назад ]", callback_data="admin_panel"))
    await call.message.delete()
    await bot.send_message(call.from_user.id, text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    await call.answer()

# ==================== АДМИН: БАН ====================
@dp.callback_query_handler(text="admin_ban")
async def admin_ban_start(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    
    await call.message.delete()
    await bot.send_message(call.from_user.id, "Введите ID или @username для бана:")
    await BanUser.waiting_ban_id.set()
    await call.answer()

@dp.message_handler(state=BanUser.waiting_ban_id)
async def admin_ban_user(message: Message, state: FSMContext):
    target = message.text.strip()
    
    user = None
    if target.startswith("@"):
        user = users_table.get(Query().username == target[1:])
    else:
        try:
            uid = int(target)
            user = get_user(uid)
        except:
            pass
    
    if not user:
        await message.answer("❌ Пользователь не найден в базе.")
        await state.finish()
        return
    
    uid = user["user_id"]
    
    if not is_banned(uid):
        banned_table.insert({"user_id": uid, "banned_at": datetime.now().isoformat()})
        await message.answer(f"🚫 Пользователь {uid} забанен.")
        try:
            await bot.send_message(uid, "🚫 Ваш тикет закрыт.")
        except:
            pass
    else:
        banned_table.remove(Query().user_id == uid)
        await message.answer(f"✅ Пользователь {uid} разбанен.")
    
    await state.finish()

# ==================== ЗАПУСК ====================
async def on_startup(dp):
    logging.info("Запуск...")
    success = await start_main_userbot()
    if success:
        logging.info("Юзербот авторизован")
    else:
        logging.info("Ожидание кода...")
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()
    logging.info("HTTP сервер запущен на порту 8080")

async def main():
    await on_startup(dp)
    await asyncio.gather(
        dp.start_polling(),
        dp_second.start_polling()
    )

if __name__ == '__main__':
    asyncio.run(main())