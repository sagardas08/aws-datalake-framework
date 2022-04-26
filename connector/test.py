from datetime import datetime

from pg_connect import Connector

# for testing
creds = {
    'host': '',
    'port': 000,
    'username': '',
    'password': '',
    'dbname': ''
}

# connecting to DB
pg = Connector(creds=creds)

# get the version of Postgre and test if the connector is working or not
pg.get_version()

# list the tables
pg.list_tables()

# execute a SQL statement w/o params
sql_statement = "Select * from some_table"
pg.execute(sql_statement)

# execute a SQL statement with params using server side binding
pg.execute('SELECT * FROM some_table WHERE asset_id=%s and src_sys_id=%s', [11002, 10000])

# truncate a table or a set of tables
table = "some_table"
pg.truncate(table=table)

# drop a table
pg.drop(table=table)

# retrieve data from all the cols of a table
pg.retrieve(table=table, cols='all')

# retrieve data from selected cols of a table
pg.retrieve(table=table, cols=['col1', 'col2', 'col3'])

# retrieve with limit
pg.retrieve(table=table, cols=['col1', 'col2', 'col3'], limit=10)

# inserting data
data = {"genre": "fiction",
        "name": "Book Name vol. 10",
        "price": 1200,
        "published": "%d-%d-1" % (2000, 100)}
pg.insert("some_table", data=data)

# update data
data_to_update = {
    'some_field': 'some_value'
}
pg.update('some_table', data=data_to_update, where=('modified_ts = %s', [datetime.date(2022, 4, 26)]))

# delete a record
pg.delete('some_table', where=('modified_ts >= %s', [datetime.date(2020, 1, 31)]), returning='asset_id')

# test joining 2 tables

# test issues while inserting unicode chars and escape chars

# test corner cases




