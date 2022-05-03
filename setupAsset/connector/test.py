from datetime import datetime

from connector import Connector

# for testing
creds = {
    'host': 'dl-fmwrk-db-instance.capmtud4vnyz.us-east-2.rds.amazonaws.com',
    'port': 5432,
    'username': 'postgresadmin',
    'password': 'postgresadmin123',
    'dbname': 'dl_fmwrk'
}
# connecting to DB
db = Connector(secret='postgres_dev', region='us-east-2')
db = Connector(creds=creds)
print(db.conn.autocommit)

# get the version of Postgre
db.get_version()

# list the tables
db.list_tables()

# execute a SQL statement w/o params
sql_statement = "Select * from some_table"
db._execute(sql_statement)

# execute a SQL statement with params using server side binding
db._execute('SELECT * FROM some_table WHERE asset_id=%s and src_sys_id=%s', [11002, 10000])

# truncate a table or a set of tables
table = "some_table"
db.truncate(table=table)

# drop a table
db.drop(table=table)

# retrieve data from all the cols of a table
db.retrieve(table=table, cols='all')

# postgres - csv

# retrieve data from selected cols of a table
db.retrieve(table=table, cols=['col1', 'col2', 'col3'])

# retrieve with limit
db.retrieve(table=table, cols=['col1', 'col2', 'col3'], limit=10)

# inserting data
data = {"genre": "fiction",
        "name": "Book Name vol. 10",
        "price": 1200,
        "published": "%d-%d-1" % (2000, 100)}
db.insert("some_table", data=data)

# update data
data_to_update = {
    'some_field': 'some_value'
}
db.update('some_table', data=data_to_update, where=('modified_ts = %s', [datetime.date(2022, 4, 26)]))

# delete a record
print(db.delete('dummy2', where=('id=%s', [5]), returning='id'))

# commit
db.commit()

# test joining 2 tables
# test issues while inserting unicode chars and escape chars
# test corner cases



