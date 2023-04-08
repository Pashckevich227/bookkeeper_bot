import os
import json
import logging
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ParseMode
from aiogram import executor
import main

load_dotenv()
token = os.environ.get('TOKEN_BOT')

logging.basicConfig(level=logging.INFO)

bot = Bot(token=token, parse_mode=ParseMode.HTML)

storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

class JsonState(StatesGroup):
    waiting_for_json = State()

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    await message.answer("Привет!")


@dp.message_handler(content_types=['text'])
async def process_json(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data['dt_from']
        dt_upto = data['dt_upto']
        group_type = data['group_type']
        correct_dt_from = main.correct_time(dt_from, dt_upto)[0]
        correct_dt_upto = main.correct_time(dt_from, dt_upto)[1]
        await message.reply(main.dataset(correct_dt_from, correct_dt_upto, group_type))
    except:
        await message.reply('Не понимаю эту команду.')


@dp.message_handler()
async def unknown_command(message: types.Message):
    await message.answer("Не понимаю эту команду.")

if __name__ == '__main__':
    logging.info('Starting bot...')
    executor.start_polling(dp, skip_updates=True)