import contextlib
import json
import logging
import psycopg2
import psycopg2.pool
import psycopg2.extras

import stellata.model

pool = None

class Pool:
    """Database connection pool instance.

    Enables multiple database connections to be defined and used by the application.
    """

    def __init__(self, name='', pool_size=10, host='localhost', password='', port=5432, user=''):
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            database=name,
            minconn=1,
            maxconn=pool_size,
            host=host,
            password=password,
            port=port,
            user=user
        )

        # enable support for UUID creation at the database level
        self.execute('create extension if not exists "uuid-ossp"')

    @contextlib.contextmanager
    def _cursor(self):
        connection = self._pool.getconn()
        try:
            yield connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            connection.commit()
        finally:
            self._pool.putconn(connection)

    def execute(self, sql: str, args: tuple = None):
        """Execute a SQL query with no return value."""

        with self._cursor() as cursor:
            logging.debug('Running SQL: ' + str((sql, args)))
            cursor.execute(sql, args)

    def query(self, sql: str, args: tuple = None):
        """Execute a SQL query with a return value."""

        with self._cursor() as cursor:
            logging.debug('Running SQL: ' + str((sql, args)))
            cursor.execute(sql, args)
            return cursor.fetchall()

def initialize(name='', pool_size=10, host='localhost', password='', port=5432, user=''):
    """Initialize a new database connection and return the pool object.

    Saves a reference to that instance in a module-level variable, so applications with only one database
    can just call this function and not worry about pool objects.
    """

    global pool
    instance = Pool(name=name, pool_size=pool_size, host=host, password=password, port=port, user=user)
    pool = instance
    return instance
