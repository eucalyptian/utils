from typing import Optional
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text


def _build_odbc_connect(
    server_str: str,
    database: str,
    *,
    use_trusted: bool,
    username: Optional[str] = None,
    password: Optional[str] = None,
    driver: str = "ODBC Driver 17 for SQL Server",
    encrypt: Optional[str] = None,
    trust_server_cert: Optional[bool] = None,
) -> str:
    """Return a URL-encoded ODBC connection string for use with SQLAlchemy's odbc_connect.

    Notes
    -----
    • Works with named instances (e.g., "192.168.10.55\\sql10") or ports (e.g., "192.168.10.55,1435").
    • If `use_trusted` is False, `username` and `password` are required.
    • `encrypt` can be "yes" or "no". Some environments require Encrypt=yes and TrustServerCertificate=yes.
    """
    parts = [
        f"DRIVER={{{{ {driver} }}}}".replace("{{ ", "{").replace(
            " }}", "}"
        ),  # ensure braces around driver name
        f"SERVER={server_str}",
        f"DATABASE={database}",
    ]

    if use_trusted:
        parts.append("Trusted_Connection=yes")
    else:
        if not username or not password:
            raise ValueError(
                "Username and password must be provided when use_trusted is False."
            )
        parts.append(f"UID={username}")
        parts.append(f"PWD={password}")

    if encrypt is not None:
        parts.append(f"Encrypt={encrypt}")
    if trust_server_cert is not None:
        parts.append(f"TrustServerCertificate={'yes' if trust_server_cert else 'no'}")

    # URL-encode the full ODBC string for SQLAlchemy
    return quote_plus(";".join(parts))


def engine_generator(
    server: str,
    database_name: str,
    instance_name: Optional[str] = None,
    use_trusted: bool = True,
    username: Optional[str] = None,
    password: Optional[str] = None,
    *,
    driver: str = "ODBC Driver 17 for SQL Server",
    encrypt: Optional[str] = None,
    trust_server_cert: Optional[bool] = None,
):
    """Create (if needed) and return a SQLAlchemy engine for a SQL Server database.

    Parameters
    ----------
    server : str
        IP/hostname. For a named instance, also supply `instance_name` OR pass "host\\instance" directly.
        For a port-based connection, pass "host,port" and leave `instance_name=None`.
    use_trusted : bool
        True = Windows (Integrated) auth. False = SQL auth (requires username/password).
    encrypt / trust_server_cert : Optional extras to satisfy driver/security policy.
    """

    # Build SERVER string
    server_str = f"{server}\\{instance_name}" if instance_name else server

    # Build a master-DB connection first (for create/check)
    master_odbc = _build_odbc_connect(
        server_str,
        "master",
        use_trusted=use_trusted,
        username=username,
        password=password,
        driver=driver,
        encrypt=encrypt,
        trust_server_cert=trust_server_cert,
    )
    master_url = f"mssql+pyodbc:///?odbc_connect={master_odbc}"

    # Helper fns
    def check_database_exists(engine, dbname):
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM sys.databases WHERE name = :n"), {"n": dbname}
            )
            return result.fetchone() is not None

    def create_database_if_needed(engine, dbname):
        if not check_database_exists(engine, dbname):
            with engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                    text(f"CREATE DATABASE [{dbname}]")
                )
                print(f"Database '{dbname}' created.")
        else:
            print(f"Database '{dbname}' already exists.")

    # Connect to master, ensure DB exists
    engine_master = create_engine(master_url)
    create_database_if_needed(engine_master, database_name)
    engine_master.dispose()

    # Connect to the target DB
    db_odbc = _build_odbc_connect(
        server_str,
        database_name,
        use_trusted=use_trusted,
        username=username,
        password=password,
        driver=driver,
        encrypt=encrypt,
        trust_server_cert=trust_server_cert,
    )
    db_url = f"mssql+pyodbc:///?odbc_connect={db_odbc}"
    return create_engine(db_url)



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


