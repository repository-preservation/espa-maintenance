import psycopg2
import numbers


class DBConnectException(Exception):
    pass

class DBConnect(object):
    """
    Class for connecting to a postgresql database using a with statement
    """
    def __init__(self, dbhost='localhost', db='postgres', dbuser='postgres', dbpass='postgres',
                 dbport=5432, autocommit=False, *args, **kwargs):
        try:
            self.conn = psycopg2.connect(host=dbhost, database=db, user=dbuser,
                                         password=dbpass, port=dbport)
            self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            raise DBConnectException(e)

        self.autocommit = autocommit
        self.fetcharr = []

    def execute(self, sql_str, params=None):
        """
        Used for enacting some change on a database
        """
        if not self.verify_type(params):
            params = self.conv_totuple(params)

        try:
            self.cursor.execute(sql_str, params)
        except psycopg2.Error as e:
            raise DBConnectException(e)

        if self.autocommit:
            self.commit()

    def select(self, sql_str, params=None):
        """
        Used for retrieving information from the database
        Results are stored in self.fetcharr to enable more flexible use
        Each row is stored as a tuple in the list array
        """
        if not self.verify_type(params):
            params = self.conv_totuple(params)

        try:
            self.cursor.execute(sql_str, params)
            self.fetcharr = self.cursor.fetchall()
        except psycopg2.Error as e:
            raise DBConnectException(e)

    def commit(self):
        try:
            self.conn.commit()
        except psycopg2.Error as e:
            raise DBConnectException(e)

    def rollback(self):
        self.conn.rollback()

    @staticmethod
    def conv_totuple(val):
        """
        Allow for single string or number parameters to be passed in more easily
        """
        if isinstance(val, (str, numbers.Number)):
            val = (val, )
        else:
            raise DBConnectException('Parameter not a valid string or number')

        return val

    @staticmethod
    def verify_type(val):
        """
        Verify the type is a sequence
        """
        if isinstance(val, (tuple, list, dict)):
            return True
        else:
            return False

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
