#TODO Needs more work to make it complete

import psycopg2


class DBConnect(object):
    """
    Class for connecting to a postgresql database using a with statement
    """

    def __init__(self, dbhost='', db='', dbuser='', dbpass='', dbport=3306, autocommit=False,
                 *args, **kwargs):
        try:
            self.conn = psycopg2.connect(host=dbhost, database=db, user=dbuser,
                                         password=dbpass, port=dbport)
            self.cursor = self.conn.cursor()
        except psycopg2.Error:
            raise

        self.autocommit = autocommit
        self.fetcharr = []

    def execute(self, sql_str):
        try:
            self.cursor.execute(sql_str)
        except psycopg2.Error:
            raise

        if self.autocommit:
            self.commit()

    def select(self, sql_str):
        self.cursor.execute(sql_str)

        try:
            self.fetcharr = self.cursor.fetchall()
        except psycopg2.Error:
            raise

    def commit(self):
        try:
            self.conn.commit()
        except psycopg2.Error:
            raise

    def rollback(self):
        self.conn.rollback()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def __len__(self):
        return len(self.fetcharr)

    def __iter__(self):
        return iter(self.fetcharr)

    def __getitem__(self, item):
        if item >= len(self.fetcharr):
            raise IndexError
        return self.fetcharr[item]

    def __del__(self):
        self.cursor.close()
        self.conn.close()

        del self.cursor
        del self.conn
