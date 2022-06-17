import json
from collections import namedtuple

import boto3
from botocore.exceptions import ClientError
import psycopg2
import psycopg2.extras as pg_extra


class Connector:
    def __init__(self, secret=None, region=None,
                 creds=None, autocommit=False):
        """
        Base constructor of the Connector class
        :param secret: AWS secret name that stores the credentials to connect
        :param region: AWS region name where the secret is stored
        :param creds: Alternative way to connect with the DB using a dict of credentials
        """
        if secret is not None and region is not None:
            self.secret_id = secret
            self.session = boto3.session.Session(region_name=region)
            self.secrets_client = self.session.client(
                service_name="secretsmanager",
                region_name=region,
            )
            self.credentials = self.get_credentials()
        elif creds and type(creds) == dict:
            self.credentials = creds
        host = self.credentials['host']
        port = self.credentials['port']
        user = self.credentials['username']
        pwd = self.credentials['password']
        db = self.credentials['dbname']
        self.conn = psycopg2.connect(
            host=host, port=port,
            database=db, user=user, password=pwd
        )
        self.conn.autocommit = autocommit
        self.cursor = self.conn.cursor()

    def get_credentials(self):
        """
        fetches credentials from the AWS secret manager
        """
        cred = None
        try:
            get_secret_value_response = self.secrets_client.get_secret_value(
                SecretId=self.secret_id
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(
                    "The requested secret "
                    + self.secret_id
                    + " was not found"
                )
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                print("The request was invalid due to:", e)
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                print("The request had invalid params:", e)
            elif e.response["Error"]["Code"] == "DecryptionFailure":
                print(
                    "The requested secret can't be decrypted using the provided KMS key:",
                    e,
                )
            elif e.response["Error"]["Code"] == "InternalServiceError":
                print("An error occurred on service side:", e)
        else:
            if "SecretString" in get_secret_value_response:
                secret = get_secret_value_response["SecretString"]
                cred = json.loads(secret)
        assert cred is not None
        return cred

    def close(self):
        """
        Close a previously existing connection
        :return:
        """
        if self.conn:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

    def commit(self):
        """
        Commit a transaction
        """
        return self.conn.commit()

    def rollback(self):
        """
        Roll-back a transaction
        """
        return self.conn.rollback()

    @staticmethod
    def _format_insert(data):
        """
        Format insert dict values into strings
        """
        cols = ",".join(data.keys())
        vals = ",".join(["%s" for _ in data])

        return cols, vals

    @staticmethod
    def _format_update(data):
        """
        Format update dict values into string
        """
        return "=%s,".join(data.keys()) + "=%s"

    @staticmethod
    def _where(where=None):
        """
        WHERE clause in SQL queries
        """
        if where and len(where) > 0:
            return " WHERE %s" % where[0]
        return ""

    @staticmethod
    def _order(order=None):
        """
        ORDER BY clause in SQL queries
        """
        sql = ""
        if order:
            sql += " ORDER BY %s" % order[0]

            if len(order) > 1:
                sql += " %s" % order[1]
        return sql

    @staticmethod
    def _limit(limit):
        """
        LIMIT clause in SQL queries
        """
        if limit:
            return " LIMIT %d" % limit
        return ""

    @staticmethod
    def _offset(offset):
        """
        OFFSET skips the number of rows before returning the query's output.
        """
        if offset:
            return " OFFSET %d" % offset
        return ""

    @staticmethod
    def _returning(returning):
        """
        The RETURNING clause is used to retrieve values of columns
        that were modified by DML statement
        """
        if returning:
            return " RETURNING %s" % returning
        return ""

    def _select(
        self,
        table=None,
        cols='*',
        where=None,
        order=None,
        limit=None,
        offset=None,
    ):
        """
        Construct a select query
        """
        sql = (
            f"SELECT {cols} FROM {table}"
            + self._where(where)
            + self._order(order)
            + self._limit(limit)
            + self._offset(offset)
        )
        return sql

    def _execute(self, sql, params=None):
        """
        Executes a raw query
        """
        try:
            self.cursor.execute(sql, params)
        except Exception as e:
            print(f"execute() failed due to: {e}")
            raise
        return self.cursor

    def get_version(self):
        """
        Display the Postgres database server version
        """
        print("PostgreSQL database version:")
        self.cursor.execute("SELECT version()")
        db_version = self.cursor.fetchone()
        print(db_version)

    def list_tables(self):
        """
        List the tables within the database
        """
        self.cursor.execute(
            "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
        )
        tables = [i[0] for i in self.cursor.fetchall()]
        return tables

    def execute(self, sql, return_type=None):
        """
        execute a raw sql statement
        :param sql: User constructed SQL statement w/o server side binding
        for e.g. "Select * from table where id = 20"
        :param return_type: None / d / dict
        if the return type is None then it will return a list of tuples
        if the return type is d/ dict it will return a Json Array
        """
        if return_type == 'd' or 'dict':
            cursor = self.conn.cursor(cursor_factory=pg_extra.RealDictCursor)
        else:
            cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            records = cursor.fetchall() if not return_type else \
                [dict(i) for i in cursor.fetchall()]
        except Exception as e:
            print(f"execute() failed due to: {e}")
            raise
        return records

    def create(self, table, schema):
        """
        Create a table with the schema provided
        ob.create('my_table','id SERIAL PRIMARY KEY, name TEXT')
        """
        self._execute(f"CREATE TABLE {table} ({schema})")
        self.conn.commit()

    def truncate(self, table, restart_identity=False, cascade=False):
        """
        Truncate a table or set of tables
        db.truncate('tbl1')
        db.truncate('tbl1, tbl2')
        """
        sql = f"TRUNCATE {table}"
        if restart_identity:
            sql += " RESTART IDENTITY"
        if cascade:
            sql += " CASCADE"
        self._execute(sql)
        self.conn.commit()

    def drop(self, table, cascade=False):
        """
        Drop a table
        """
        sql = f"DROP TABLE IF EXISTS {table}"
        if cascade:
            sql += " CASCADE"
        self._execute(sql)
        self.conn.commit()

    def retrieve(self, table, cols, where=None, order=None, limit=None):
        """
        Retrieve the data from a table for some cols / all cols
        :return: list of tuples
        """
        if cols == "all":
            columns = "*"
        elif isinstance(cols, list):
            columns = ",".join(cols).rstrip(",")
        else:
            columns = cols
        sql = self._select(table, columns, where, order, limit)
        # params is a tuple where the 0th index is the condition and
        # the 1st index is the value
        params = where[1] if where and len(where) == 2 else None
        cursor = self._execute(sql, params)
        rows = cursor.fetchall()
        return rows[len(rows) - limit if limit else 0:]

    def retrieve_dict(self, table, cols, where=None, order=None, limit=None):
        """
        Retrieve a table / subset of tables in a JSON array format
        :return: list of dict
        """
        # open up a new cursor
        cursor = self.conn.cursor(cursor_factory=pg_extra.RealDictCursor)
        if cols == "all" or cols == '*':
            columns = "*"
        elif isinstance(cols, list):
            columns = ",".join(cols).rstrip(",")
        else:
            columns = cols
        sql = self._select(table, columns, where, order, limit)
        params = where[1] if where and len(where) == 2 else None
        cursor.execute(sql, params)
        records = [dict(i) for i in cursor.fetchall()]
        return records

    def retrieve_csv(self, table, cols, where=None, order=None, limit=None):
        """
        Method to retrieve the data from table in a CSV format
        :param table: string table
        :param cols: list of columns
        :param where: Tuple ("parameterized_statement", [parameters])
         for eg: ("id=%s and name=%s", [1, "test"])
        :param order: [field, ASC|DESC]
        :param limit: [limit1, limit2]
        :return: None, stores the file in the csv format in cwd
        """
        if cols == "all" or '*':
            columns = "*"
        elif isinstance(cols, list):
            columns = ",".join(cols).rstrip(",")
        else:
            columns = cols
        sql = self._select(table, columns, where, order, limit)
        op_query = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sql)
        # Note: currently not parameterized to store at a different target location
        with open(f'{table}.csv', 'w') as f:
            self.cursor.copy_expert(op_query, f)

    def insert(self, table, data: dict, returning=None):
        """
        Insert a single record into the database table
        """
        cols, vals = self._format_insert(data)
        sql = f"INSERT INTO {table} ({cols}) VALUES({vals})"
        sql += self._returning(returning)
        cursor = self._execute(sql, list(data.values()))
        return cursor.fetchone() if returning else cursor.rowcount

    def insert_many(self, table, data: list, returning=None):
        """
        Insert multiple records in a single round trip
        :param table: string
        :param data: Json List
        :param returning: Bool
        """
        try:
            assert isinstance(data, list)
            arg_vals = []
            for item in data:
                value = tuple(item.values())
                arg_vals.append(value,)
            data_elem = data[0]
            cols = ",".join(data_elem.keys())
            sql = f"INSERT INTO {table} ({cols}) VALUES %s"
            sql += self._returning(returning)
            pg_extra.execute_values(cur=self.cursor,
                                    sql=sql,
                                    argslist=arg_vals,
                                    fetch=returning)
            return self.cursor.fetchone() if returning else None
        except AssertionError:
            raise

    def update(self, table, data, where=None, returning=None):
        """

        :param table: string table
        :param data:  dict data to update
        :param where: Tuple ("parameterized_statement", [parameters])
         for eg: ("id=%s and name=%s", [1, "test"])
        :param returning:
        :return:
        """
        query = self._format_update(data)
        sql = f"UPDATE {table} SET {query}"
        sql += self._where(where) + self._returning(returning)
        cursor = self._execute(
            sql,
            list(data.values()) + where[1]
            if where and len(where) > 1
            else list(data.values()),
        )
        return cursor.fetchall() if returning else cursor.rowcount

    def delete(self, table, where, returning=None):
        """
        Delete rows based on a where condition
        where: Tuple ("parameterized_statement", [parameters])
         for eg: ("id=%s and name=%s", [1, "test"])
        """
        sql = f'DELETE FROM {table}'
        sql += self._where(where) + self._returning(returning)
        cursor = self._execute(sql, where[1] if where and len(where) > 1 else None)
        return cursor.fetchall() if returning else cursor.rowcount
