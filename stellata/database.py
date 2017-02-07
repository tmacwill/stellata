import contextlib
import json
import logging
import psycopg2
import psycopg2.pool
import psycopg2.extras

import stellata.model

_pool = None

@contextlib.contextmanager
def _cursor():
    connection = _pool.getconn()
    try:
        yield connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        connection.commit()
    finally:
        _pool.putconn(connection)

def initialize(config: dict):
    """Initialize database connection."""

    global _pool

    _pool = psycopg2.pool.ThreadedConnectionPool(
        database=config.get('name', ''),
        minconn=1,
        maxconn=config.get('pool_size', 10),
        host=config.get('host', 'localhost'),
        password=config.get('password', ''),
        port=config.get('port', 5432),
        user=config.get('user', '')
    )

    execute('create extension if not exists "uuid-ossp"')

def execute(sql: str, args: tuple = None):
    """Execute a SQL query with no return value."""

    with _cursor() as cursor:
        logging.debug('Running SQL: ' + str((sql, args)))
        cursor.execute(sql, args)

def query(sql: str, args: tuple = None):
    """Execute a SQL query with a return value."""

    with _cursor() as cursor:
        logging.debug('Running SQL: ' + str((sql, args)))
        cursor.execute(sql, args)
        return cursor.fetchall()
