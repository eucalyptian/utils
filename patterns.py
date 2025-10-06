
# minimal code to start a python-telegram-bot[socks] using v2rayN proxy settings. This is for a bot that accepts updates.
import os
from telegram.ext import ApplicationBuilder

os.environ['http_PROXY'] = '127.0.0.1:10808' # sample proxy setting for v2rayN
os.environ['https_PROXY'] = '127.0.0.1:10808' # sample proxy setting for v2rayN
bot_token = 'YOUR_BOT_TOKEN' # bot token
app = ApplicationBuilder().token(bot_token).build() # build app
print('Bot is running...')
app.run_polling() # start bot

# minimal code to start a python-telegram-bot[socks] using v2rayN proxy settings. This is for a bot that sends messages to channels or groups and doesn't require any updates from telegram.
import asyncio
from telegram import Bot

BOT_TOKEN = "YOUR_BOT_TOKEN"
CHANNEL_ID = "YOUR_CHANNEL_ID"   # your channel id or @channelusername

bot = Bot(token=BOT_TOKEN)

# --- Windows fix for asyncio ---
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def send_to_telegram_channel(input_list):
    """Send each item in input_list to your channel."""
    for item in input_list:
        await bot.send_message(chat_id=CHANNEL_ID, text=str(item))

def main():
    messages = ["aapl", "googl"]
    asyncio.run(send_to_telegram_channel(messages))

if __name__ == "__main__":
    main()
