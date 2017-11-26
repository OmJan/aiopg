import socket
import uuid
import logging
import time

import psycopg2
from docker import Client as DockerClient

logger = logging.getLogger(__name__)


def session_id():
    '''Unique session identifier, random string.'''
    return str(uuid.uuid4())


def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return f


def docker():
    return DockerClient(version='auto')


def pg_server(
        unused_port=unused_port()(),
        docker=docker(),
        session_id=session_id(),
        pg_tag='9.5'
             ):
    docker.pull('postgres:{}'.format(pg_tag))
    container = docker.create_container(
        image='postgres:{}'.format(pg_tag),
        name='aiopg-benchmark-server-{}-{}'.format(pg_tag, session_id),
        ports=[5432],
        detach=True,
    )
    docker.start(container=container['Id'])
    inspection = docker.inspect_container(container['Id'])
    host = inspection['NetworkSettings']['IPAddress']
    pg_params = dict(database='postgres',
                     user='postgres',
                     password='mysecretpassword',
                     host=host,
                     port=5432)
    delay = 0.001
    for i in range(100):
        try:
            conn = psycopg2.connect(**pg_params)
            cur = conn.cursor()
            cur.execute("CREATE EXTENSION hstore;")
            cur.close()
            conn.close()
            break
        except psycopg2.Error:
            time.sleep(delay)
            delay *= 2
    else:
        logger.error("Cannot start postgres server")
    pass
    container['host'] = host
    container['port'] = 5432
    container['pg_params'] = pg_params
    return container


def pg_server_remove(container):
    docker().kill(container=container['Id'])
    docker().remove_container(container['Id'])
