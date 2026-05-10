from sqlalchemy import NVARCHAR, inspect, MetaData, Table, create_engine, text
from sqlalchemy.exc import NoSuchTableError, OperationalError
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, Dict, Literal, Union
from datetime import timedelta, datetime
from sqlalchemy.engine import Engine
from pandas.io.sql import get_schema
from urllib.parse import quote_plus
import pandas as pd
import numpy as np
import platform
import logging
import pyodbc
import time
import re
import os


# setup logger to be created based on module name and reset every 24 hours at midnight. only keeps yesterday log as a separate date-named file.
def setup_logger(name, level):
    module_name = os.path.splitext(os.path.basename(name))[0]
    log_file = f"{module_name}.log"
    # Set up rotating log handler
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=1, encoding='utf-8')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger(module_name)
    if level == 'INFO':
        logger.setLevel(logging.INFO)
    if level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    if level == 'WARNING':
        logger.setLevel(logging.WARNING)
    logger.addHandler(log_handler)
    return logger


proxies = {
    'http': None,
    'https': None,
    'socks': None,
    'ftp': None,
}


class SqlServerEngineFactory:
    _engine_cache: Dict[str, Engine] = {}

    @staticmethod
    def detect_driver():
        drivers = pyodbc.drivers()
        sql_drivers = [
            d for d in drivers
            if "SQL Server" in d
        ]
        if not sql_drivers:
            raise RuntimeError(
                "No SQL Server ODBC driver found"
            )
        preferred_order = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server",
        ]
        for preferred in preferred_order:
            if preferred in sql_drivers:
                return preferred
        # fallback
        return sql_drivers[-1]

    @staticmethod
    def build_odbc_string(
            server: str,
            database: str,
            username: Optional[str],
            password: Optional[str],
            encrypt: Optional[str],
            trust_server_cert: Optional[bool],
            driver: Optional[str],
    ):
        if driver is None:
            driver = SqlServerEngineFactory.detect_driver()
        os_platform = platform.system().lower()
        parts = [
            f"DRIVER={{{driver}}}",
            f"SERVER={server}",
            f"DATABASE={database}",
        ]
        if os_platform == "windows":
            parts.append("Trusted_Connection=yes")
        elif os_platform == "linux":
            if not username or not password:
                raise ValueError("Linux requires username and password")
            parts.append(f"UID={username}")
            parts.append(f"PWD={password}")
            if encrypt is None:
                encrypt = "yes"
            if trust_server_cert is None:
                trust_server_cert = True
        else:
            raise RuntimeError("Unsupported OS")

        if encrypt:
            parts.append(f"Encrypt={encrypt}")
        if trust_server_cert is not None:
            parts.append(
                f"TrustServerCertificate={'yes' if trust_server_cert else 'no'}"
            )
        return quote_plus(";".join(parts))

    @staticmethod
    def _create_engine(url: str) -> Engine:
        return create_engine(
            url,
            pool_size=15,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
            future=True,
        )

    @staticmethod
    def _connect_with_retry(url: str, retries=3, delay=2):
        for attempt in range(retries):
            try:
                engine = SqlServerEngineFactory._create_engine(url)
                with engine.connect():
                    return engine
            except OperationalError:
                if attempt == retries - 1:
                    raise
                time.sleep(delay)

    @staticmethod
    def _database_exists(engine: Engine, dbname: str):
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM sys.databases WHERE name = :name"),
                {"name": dbname},
            )
            return result.scalar() is not None

    @staticmethod
    def _create_database(engine: Engine, dbname: str):
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                text(f"CREATE DATABASE [{dbname}]")
            )

    @classmethod
    def get_engine(
            cls,
            server: str,
            database: str,
            instance: Optional[str] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            driver: Optional[str] = None,
            encrypt: Optional[str] = None,
            trust_server_cert: Optional[bool] = None,
    ) -> Engine:

        server_str = f"{server}\\{instance}" if instance else server
        cache_key = f"{server_str}_{database}"
        if cache_key in cls._engine_cache:
            print(f'Engine for {database} already exists... (Singleton)')
            return cls._engine_cache[cache_key]
        master_odbc = cls.build_odbc_string(
            server_str,
            "master",
            username,
            password,
            encrypt,
            trust_server_cert,
            driver,
        )
        master_url = f"mssql+pyodbc:///?odbc_connect={master_odbc}"
        master_engine = cls._connect_with_retry(master_url)
        if not cls._database_exists(master_engine, database):
            print(f"Database {database} does not exist. Creating ...")
            cls._create_database(master_engine, database)
        else:
            print(f'Database {database} already exists...')
        master_engine.dispose()
        db_odbc = cls.build_odbc_string(
            server_str,
            database,
            username,
            password,
            encrypt,
            trust_server_cert,
            driver,
        )
        db_url = f"mssql+pyodbc:///?odbc_connect={db_odbc}"
        engine = cls._connect_with_retry(db_url)
        cls._engine_cache[cache_key] = engine
        return engine


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


def upsert_sql_table(df, engine_name, table_name, identifier_column, identifier_value, allow_column_mismatch,
                     max_length=255, dtype=None):
    inspector = inspect(engine_name)
    table_exists = table_name in inspector.get_table_names()
    if not dtype:
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
            conn.execute(delete_query, {f"identifier_value": float(identifier_value) if isinstance(identifier_value,
                                                                                                   Union[
                                                                                                       int, float, np.integer, np.floating]) else identifier_value})
            df.to_sql(table_name, con=conn, if_exists='append', index=False, dtype=dtype)


def get_dtype_mapping_from_table(
        df: pd.DataFrame,
        engine,
        table_name: str,
        schema: str = "dbo",
        include_only_df_cols: bool = True,
        fallback_max_length: Literal[255, 'max'] = 255
):
    """
    Reads the SQL Server table schema and returns a dtype mapping dictionary
    for use in pandas.to_sql(). If the table does not exist, it falls back to
    generating a mapping using `get_dtype_mapping()`.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe whose columns you want to map.
    engine : sqlalchemy.Engine
        SQLAlchemy engine connected to the SQL Server database.
    table_name : str
        The name of the SQL table to inspect.
    schema : str, default 'dbo'
        Schema name where the table resides.
    include_only_df_cols : bool, default True
        If True, only return mappings for columns present in the dataframe.
    fallback_max_length : {255, 'max'}, default 255
        NVARCHAR length to use if the table does not exist.

    Returns
    -------
    dict
        A dictionary suitable for the `dtype` argument in df.to_sql()
        (e.g., {'fund': NVARCHAR(length=255), 'cost': FLOAT()}).

    Raises
    ------
    sqlalchemy.exc.NoSuchTableError
        If the table does not exist and fallback is disabled.

    Notes
    -----
    - If the table exists, the function reflects the table schema directly
      from SQL Server and uses the same column types.
    - If the table does not exist, it falls back to inferring NVARCHAR types
      for text columns in the DataFrame.

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>> import pandas as pd
    >>> engine = create_engine("mssql+pyodbc://localhost/MyDB?driver=ODBC+Driver+17+for+SQL+Server")

    >>> df = pd.DataFrame({
    ...     'fund': ['A', 'B', 'C'],
    ...     'value': [10.5, 20.2, 15.7]
    ... })

    # Case 1: Table exists — reflect SQL types
    >>> dtype_map = get_dtype_mapping_from_table(df, engine, 'my_table')

    # Case 2: Table doesn't exist — fallback to inferred mapping
    >>> dtype_map = get_dtype_mapping_from_table(df, engine, 'nonexistent_table')

    # Use it in to_sql()
    >>> df.to_sql('my_table', engine, schema='dbo', if_exists='append', index=False, dtype=dtype_map)
    """
    metadata = MetaData()
    try:
        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
        dtype_mapping = {col.name: col.type for col in table.columns}

        if include_only_df_cols:
            dtype_mapping = {col: dtype_mapping[col] for col in df.columns if col in dtype_mapping}

        return dtype_mapping

    except NoSuchTableError:
        print(f"⚠️ Table '{schema}.{table_name}' does not exist — using fallback dtype mapping.")
        return get_dtype_mapping(df, fallback_max_length)


class RUN_WINDOW:
    """
    Params:
      start: format: "HH:MM". Indifferent to trailing zeros.
      end: format: "HH:MM". Indifferent to trailing zeros.
      logger: logger object

    Sample usage:
    run_window = RUN_WINDOW(logger=logger, start="12:00", end="17:01")
    while True:
        if run_window.is_open:
            pass # means that the job can be run.
        else:
             time.sleep(run_window.sleep_time)
             continue
    """

    def __init__(self, logger, start: str, end: str):
        self.logger = logger
        self.working_weekdays = [0, 1, 2, 5, 6]
        self.sleep_time = None
        self.start = start
        self.end = end
        self.start_time_hour = int(start.split(";")[0])
        self.start_time_minute = int(start.split(";")[1])
        self.end_time_hour = int(end.split(";")[0])
        self.end_time_minute = int(end.split(";")[1])

    @property
    def is_open(self):
        # if time is between start to end of open window then run, else: sleep until tomorrow
        now = datetime.now()
        start_time = now.replace(hour=self.start_time_hour, minute=self.start_time_minute, second=0, microsecond=0)
        end_time = now.replace(hour=self.end_time_hour, minute=self.end_time_minute, second=0, microsecond=0)

        if not (start_time <= now <= end_time):
            if now < start_time:
                next_run = (now + timedelta(days=0)).replace(hour=self.start_time_hour, minute=self.start_time_minute,
                                                             second=0, microsecond=0)
            else:  # means: end_time < now
                next_run = (now + timedelta(days=1)).replace(hour=self.start_time_hour, minute=self.start_time_minute,
                                                             second=0, microsecond=0)
            sleep_seconds = max(1, int((next_run - now).total_seconds()))
            self.logger.info(f"Outside run window ({self.start}-{self.end}). Sleeping until {next_run}")
            self.sleep_time = sleep_seconds
            return False
        if datetime.now().weekday() not in self.working_weekdays:
            next_run = (now + timedelta(days=1)).replace(hour=self.start_time_hour, minute=self.start_time_minute,
                                                         second=0, microsecond=0)
            sleep_seconds = max(1, int((next_run - now).total_seconds()))
            self.sleep_time = sleep_seconds
            return False
        return True
