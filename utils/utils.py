from sqlalchemy import NVARCHAR, create_engine, text, inspect
from pandas.io.sql import get_schema
from typing import Optional, Literal
from urllib.parse import quote_plus
import re

# setup logger to be created based on module name and reset every 24 hours at midnight. only keeps yesterday log as a separate date-named file.
def setup_logger():
  module_name = os.path.splitext(os.path.basename(__file__))[0]
  log_file = f"{module_name}.log"
  # Set up rotating log handler
  log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
  log_handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=1, encoding='utf-8')
  log_handler.setFormatter(log_formatter)
  logger = logging.getLogger(module_name)
  logger.setLevel(logging.INFO)
  logger.addHandler(log_handler)
  return logger

proxies = {
    'http': None,
    'https': None,
    'socks': None,
    'ftp': None,
}

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


def get_dtype_mapping(df, max_length: Literal[255, 'max']):
    if max_length == 'max':
        length = None
    else:
        length = 255
    dtype_mapping = {}
    for col, dtype in df.dtypes.items():
        if dtype in ['object', 'string']:  # Object types are often text columns
            dtype_mapping[col] = NVARCHAR(length)  # Adjust NVARCHAR size as needed
    return dtype_mapping




def parse_create_table(ddl: str) -> dict:
    # Extract the part inside parentheses
    match = re.search(r'\((.*)\)', ddl, re.DOTALL)
    if not match:
        return {}

    columns_block = match.group(1)

    # Split lines and strip
    lines = [line.strip().rstrip(',') for line in columns_block.splitlines() if line.strip()]

    col_types = {}
    for line in lines:
        # Match bracketed or non-bracketed column names
        match = re.match(r'(\[.*?\]|\w+)\s+(.+)', line)
        if match:
            raw_name, raw_type = match.groups()
            col_name = raw_name.strip('[]')  # Remove brackets
            col_type = raw_type.strip()
            col_types[col_name] = col_type

    return col_types

def upsert_sql_table(df, engine_name, table_name, identifier_column, identifier_value, allow_column_mismatch, max_length=255):
    inspector = inspect(engine_name)
    table_exists = table_name in inspector.get_table_names()
    dtype = get_dtype_mapping(df, max_length=max_length)
    auto_dtypes = parse_create_table(get_schema(df, name='my_table', con=engine_name))
    with engine_name.begin() as conn:
        if not table_exists:
            df.to_sql(table_name, con=conn, if_exists='fail', index=False, dtype=dtype)
        else:
            if allow_column_mismatch:
                # Add new columns if needed
                # Get current columns from SQL
                inspector = inspect(conn)
                columns_in_table = [col['name'] for col in inspector.get_columns(table_name)]

                # Find new columns
                new_columns = [col for col in df.columns if col not in columns_in_table]

                # Alter table to add new columns
                for col in new_columns:
                    if col in dtype.keys():
                        new_dtype = dtype[col]  # or infer based on df[col].dtype
                    else:
                        new_dtype = auto_dtypes[col]
                    
                    conn.execute(text(f'ALTER TABLE {table_name} ADD [{col}] {new_dtype}'))
            # Delete old data for this site
            delete_query = text(f"DELETE FROM {table_name} WHERE {identifier_column} = :identifier_value")
            conn.execute(delete_query, {f"identifier_value": identifier_value})
            df.to_sql(table_name, con=conn, if_exists='append', index=False, dtype=dtype)
