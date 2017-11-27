import json
from sqlalchemy.dialects.postgresql import JSON
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
# from psycopg2.extras import execute_values
import psycopg2

metadata = sa.MetaData()
Base = declarative_base(metadata=metadata)


class SATable1(Base):
    __tablename__ = 'sa_tbl_1'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(80))
    email = sa.Column(sa.String(80))
    phone = sa.Column(sa.String(80))
    satable2_id = sa.Column(
        sa.Integer,
        sa.ForeignKey('sa_tbl_2.id', ondelete='SET NULL')
    )
    custom_json = sa.Column(JSON)


class SATable2(Base):
    __tablename__ = 'sa_tbl_2'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(80))
    email = sa.Column(sa.String(80))
    phone = sa.Column(sa.String(80))
    custom_json = sa.Column(JSON)


def get_data_table_2():
    return [
        (
            (i + 1),
            'name{}'.format(str(i)),
            'email{}@email.com'.format(str(i)),
            '12345678{}'.format(str(i)),
            json.dumps({
                'external_id': i,
                'name': 'name{}'.format(str(i))
            }),
        ) for i in range(100)]


def get_data_table_1():
    return [
        (
            'name{}'.format(str(i)),
            'email{}@email.com'.format(str(i)),
            '12345678{}'.format(str(i)),
            json.dumps({
                'external_id': i,
                'name': 'name{}'.format(str(i))
            }),
            (i + 1),
        ) for i in range(100)]


def fill_tables(pg_params):
    data2 = get_data_table_2()
    data1 = get_data_table_1()
    conn = psycopg2.connect(**pg_params)
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO sa_tbl_2 (id, name, email, phone, custom_json) VALUES (%s,%s, %s, %s, %s)",
        data2
    )
    conn.commit()
    cur.executemany(
        "INSERT INTO sa_tbl_1 (name, email, phone, custom_json, satable2_id) VALUES  (%s,%s,%s,%s,%s)",
        data1
    )
    conn.commit()
    cur.close()
    conn.close()


QUERIES = {
    "simple_select": {
        "psycopg": "SELECT id from sa_tbl_1",
        "aiopg": "SELECT id from sa_tbl_1",
        "aiopg_sa_cache": sa.select([SATable1.__table__.c.id]).compile(),
        "aiopg_sa": sa.select([SATable1.__table__.c.id]),
        "args": [],
        "setup": "DROP TABLE IF EXISTS sa_tbl_2 CASCADE; DROP TABLE IF EXISTS sa_tbl_1 CASCADE; CREATE TABLE sa_tbl_2 (id SERIAL NOT NULL, name VARCHAR(80), email VARCHAR(80), phone VARCHAR(80), custom_json JSON, PRIMARY KEY (id)); CREATE TABLE sa_tbl_1 (id SERIAL NOT NULL, name VARCHAR(80), email VARCHAR(80), phone VARCHAR(80), satable2_id INTEGER, custom_json JSON, PRIMARY KEY (id), FOREIGN KEY(satable2_id) REFERENCES sa_tbl_2 (id) ON DELETE SET NULL);",  # noqa,
        "teardown": "DROP TABLE sa_tbl_2 CASCADE; DROP TABLE sa_tbl_1 CASCADE;",
        "populate_before_start": fill_tables
    }
    # "simple_insert": {
    #     "name": "simple insert",
    #     "query": "",
    #     "sa_query": "",
    #     "args": []
    # },
    # "join_select": {
    #     "name": "join 2 tables and select",
    #     "query": "",
    #     "sa_query": "",
    #     "args": []
    # }
}
