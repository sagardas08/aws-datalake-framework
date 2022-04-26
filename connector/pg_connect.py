import json
from collections import namedtuple

import boto3
from botocore.exceptions import ClientError
import psycopg2


class Connector:
    def __init__(self, secret=None, region=None, creds=None):
        """
        Base constructor of the Connector class
        :param secret: AWS secret name that stores the credentials to connect
        :param region: AWS region name where the secret is stored
        :param creds: Alternative way to connect with the DB using a dict of credentials
        """
        if secret is not None and region is not None:
            self.secret_id = secret
            self.session = boto3.session.Session()
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
        self.cursor = self.conn.cursor()

    def get_credentials(self):
        """
        fetches credentials from the AWS secret manager
        """
        data = None
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
                data = json.loads(secret)
        assert data is not None
        return data

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
        fields=(),
        where=None,
        order=None,
        limit=None,
        offset=None,
    ):
        """
        Run a select query
        """
        sql = (
            "SELECT %s FROM %s" % (",".join(fields), table)
            + self._where(where)
            + self._order(order)
            + self._limit(limit)
            + self._offset(offset)
        )
        return self.execute(
            sql, where[1] if where and len(where) == 2 else None
        )

    def _join(
        self,
        tables=(),
        fields=(),
        join_fields=(),
        where=None,
        order=None,
        limit=None,
        offset=None,
    ):
        """
        Run an inner left join query
        """

        fields = [tables[0] + "." + f for f in fields[0]] + [
            tables[1] + "." + f for f in fields[1]
        ]

        sql = "SELECT {0:s} FROM {1:s} LEFT JOIN {2:s} ON ({3:s} = {4:s})".format(
            ",".join(fields),
            tables[0],
            tables[1],
            "{0}.{1}".format(tables[0], join_fields[0]),
            "{0}.{1}".format(tables[1], join_fields[1]),
        )

        sql += (
            self._where(where)
            + self._order(order)
            + self._limit(limit)
            + self._offset(offset)
        )

        return self.execute(
            sql, where[1] if where and len(where) > 1 else None
        )

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

    def execute(self, sql, params=None):
        """
        Executes a raw query
        """
        try:
            self.cursor.execute(sql, params)
        except Exception as e:
            print("execute() failed: " + e.message)
            raise
        return self.cursor

    def create(self, table, schema):
        """
        Create a table with the schema provided
        ob.create('my_table','id SERIAL PRIMARY KEY, name TEXT')
        """
        self.execute("CREATE TABLE %s (%s)" % (table, schema))

    def truncate(self, table, restart_identity=False, cascade=False):
        """
        Truncate a table or set of tables
        db.truncate('tbl1')
        db.truncate('tbl1, tbl2')
        """
        sql = "TRUNCATE %s"
        if restart_identity:
            sql += " RESTART IDENTITY"
        if cascade:
            sql += " CASCADE"
        self.execute(sql % table)

    def drop(self, table, cascade=False):
        """
        Drop a table
        """
        sql = f"DROP TABLE IF EXISTS {table}"
        if cascade:
            sql += " CASCADE"
        self.execute(sql)

    def retrieve(self, table, cols, limit=None):
        """
        Retrieve the data from a table for some cols / all cols
        """
        if cols == "all":
            columns = "*"
        elif isinstance(cols, list):
            columns = ",".join(cols).rstrip(",")
        else:
            columns = cols
        query = "SELECT {0} from {1};".format(columns, table)
        self.cursor.execute(query)
        # fetch data
        rows = self.cursor.fetchall()
        return rows[len(rows) - limit if limit else 0:]

    def join(
        self,
        tables=(),
        cols=(),
        join_cols=(),
        where=None,
        order=None,
        limit=None,
        offset=None,
    ):
        """
        Run an inner left join query
         :param tables: Tables to join (T1, T2)
        :param cols: ([fields from T1], [fields from T2])  -> fields to select
        :param join_cols: (field1, field2) -> fields to join.
        -> field1 belongs to table1 and field2 belongs to table 2
        :param where: = ("parameterized_statement", [parameters])
                eg: ("id=%s and name=%s", [1, "test"])
        :param order: [field, ASC|DESC]
        :param limit: [limit1, limit2]
        :param offset:
        """
        cur = self._join(
            tables, cols, join_cols, where, order, limit, offset
        )
        result = cur.fetchall()

        rows = None
        if result:
            Row = namedtuple("Row", [f[0] for f in cur.description])
            rows = [Row(*r) for r in result]

        return rows

    def insert(self, table, data, returning=None):
        """
        Insert a single record into the database table
        """
        cols, vals = self._format_insert(data)
        sql = "INSERT INTO %s (%s) VALUES(%s)" % (table, cols, vals)
        sql += self._returning(returning)
        print(sql)
        cur = self.execute(sql, list(data.values()))
        self.conn.commit()
        return cur.fetchone() if returning else cur.rowcount

    def update(self, table, data, where=None, returning=None):
        """
        Update + Insert a record more commonly Upsert
        """
        query = self._format_update(data)
        sql = f"UPDATE {table} SET {query}"
        sql += self._where(where) + self._returning(returning)
        cur = self.execute(
            sql,
            list(data.values()) + where[1]
            if where and len(where) > 1
            else list(data.values()),
        )
        self.conn.commit()
        return cur.fetchall() if returning else cur.rowcount

    def delete(self, table, where=None, returning=None):
        """
        Delete rows based on a where condition
        """
        sql = f"DELETE FROM {table} WHERE {where}"
        cur = self.execute(sql)
        return cur.fetchall() if returning else cur.rowcount
