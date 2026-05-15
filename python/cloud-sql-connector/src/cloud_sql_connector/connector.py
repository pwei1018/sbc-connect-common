# Copyright © 2025 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Cloud SQL connection utilities for the Notify API service."""

import threading
import time
from dataclasses import dataclass

from google.cloud.sql.connector import Connector
from sqlalchemy import event

_connector = None
_lock = threading.Lock()


@dataclass
class DBConfig:
    """Database configuration settings."""

    instance_name: str
    database: str
    user: str
    ip_type: str
    schema: str
    enable_iam_auth: bool = True
    driver: str = "pg8000"

    # Connection pool parameters
    pool_size: int = 5
    max_overflow: int = 2
    pool_timeout: int = 10
    pool_recycle: int = 300
    pool_use_lifo: bool = True
    pool_pre_ping: bool = True
    connect_args: dict = None

    def __post_init__(self):
        """Initialize default connect_args if not provided."""
        if self.connect_args is None:
            self.connect_args = {}

    def get_engine_options(self) -> dict:
        """Get SQLAlchemy engine options for this configuration.

        Returns:
            dict: Dictionary of engine options suitable for SQLAlchemy create_engine()
        """
        return {
            "creator": lambda: getconn(self),
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": self.pool_pre_ping,
            "pool_use_lifo": self.pool_use_lifo,
            "connect_args": self.connect_args,
        }


def _get_connector() -> Connector:
    """Get the singleton connector instance with lazy initialization.

    Returns:
        Connector: The singleton connector instance
    """
    global _connector

    if _connector is None:
        with _lock:
            if _connector is None:
                _connector = Connector(refresh_strategy="lazy")

    return _connector


def close_connector() -> None:
    """Close and clear the singleton connector instance."""
    global _connector

    with _lock:
        if _connector is not None:
            _connector.close()
            _connector = None


def getconn(db_config: DBConfig) -> object:
    """Create a database connection.

    Args:
        db_config (DBConfig): The database configuration.

    Returns:
        object: A connection object to the database.
    """
    for attempt in range(3):
        try:
            connector = _get_connector()
            conn = connector.connect(
                instance_connection_string=db_config.instance_name,
                db=db_config.database,
                user=db_config.user,
                ip_type=db_config.ip_type,
                driver=db_config.driver,
                enable_iam_auth=db_config.enable_iam_auth,
            )

            if db_config.schema:
                cursor = conn.cursor()
                cursor.execute(f"SET search_path TO {db_config.schema},public")
                cursor.execute(f"SET LOCAL search_path TO {db_config.schema}, public;")
                cursor.close()

            return conn

        except PermissionError as e:
            if attempt < 2:
                time.sleep(1)
                continue
            raise


def setup_search_path_event_listener(engine, schema):
    """Set up an event listener to set the search path for a database connection.

    Args:
        engine: The SQLAlchemy engine object
        schema: The database schema name to use
    """

    @event.listens_for(engine, "checkout")
    def set_search_path_on_checkout(
        dbapi_connection, connection_record, connection_proxy
    ):
        cursor = dbapi_connection.cursor()
        cursor.execute(f"SET search_path TO {schema},public")
        cursor.close()


def setup_pg8000_close_event_listener(engine):
    """Set up an event listener to wrap dbapi connection close() to suppress pg8000 errors during Cloud Run scale-down.

    Args:
        engine: The SQLAlchemy engine object
    """
    import logging

    try:
        from pg8000.exceptions import InterfaceError
    except ImportError:
        InterfaceError = None

    @event.listens_for(engine, "connect")
    def on_connect(dbapi_conn, _connection_record):
        original_close = dbapi_conn.close

        def safe_close():
            try:
                original_close()
            except Exception as e:
                if InterfaceError and isinstance(e, InterfaceError):
                    logging.getLogger(__name__).debug(
                        "Suppressed pg8000 InterfaceError on connection close during teardown."
                    )
                else:
                    raise

        dbapi_conn.close = safe_close
