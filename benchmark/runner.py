import time
import json
import asyncio
from concurrent import futures
import argparse

import psycopg2
import aiopg
from aiopg.sa import create_engine
import numpy as np

from queries import QUERIES


def psycopg_connect(args):
    conn = psycopg2.connect(
        user=args.pguser,
        host=args.pghost,
        port=args.pgport,
        password=args.pgpassword,
        database=args.pgdatabase
    )
    return conn


def psycopg_execute(conn, query):  # , args):
    cur = conn.cursor()
    cur.execute(query)  # , args)
    return len(cur.fetchall())


async def aiopg_connect(args):
    conn = await aiopg.connect(
        user=args.pguser,
        host=args.pghost,
        port=args.pgport,
        password=args.pgpassword,
        database=args.pgdatabase
    )
    return conn


async def aiopg_execute(conn, query):  # args
    cur = await conn.cursor()
    await cur.execute(query, args)
    return len(await cur.fetchall())


async def aiopg_sa_connect(engine):
    conn = await engine.acquire()
    return conn


async def aiopg_sa_execute(conn, query):  # , args):
    async with conn.execute(query) as cur:
        res = len(await cur.fetchall())  # , args)
    return res


async def worker(executor, eargs, start, duration, timeout):
    queries = 0
    rows = 0
    latency_stats = np.zeros((timeout * 100,))
    min_latency = float('inf')
    max_latency = 0.0

    while time.monotonic() - start < duration:
        req_start = time.monotonic()
        rows += await executor(*eargs)
        req_time = round((time.monotonic() - req_start) * 1000 * 100)

        if req_time > max_latency:
            max_latency = req_time
        if req_time < min_latency:
            min_latency = req_time
        latency_stats[req_time] += 1
        queries += 1
    return queries, rows, latency_stats, min_latency, max_latency


def sync_worker(executor, eargs, start, duration, timeout):
    queries = 0
    rows = 0
    latency_stats = np.zeros((timeout * 100,))
    min_latency = float('inf')
    max_latency = 0.0

    while time.monotonic() - start < duration:
        req_start = time.monotonic()
        rows += executor(*eargs)
        req_time = round((time.monotonic() - req_start) * 1000 * 100)

        if req_time > max_latency:
            max_latency = req_time
        if req_time < min_latency:
            min_latency = req_time
        latency_stats[req_time] += 1
        queries += 1

    return queries, rows, latency_stats, min_latency, max_latency


def map_results(results, duration):
    min_latency = float('inf')
    max_latency = 0.0
    queries = 0
    rows = 0
    latency_stats = None

    for result in results:
        t_queries, t_rows, t_latency_stats, t_min_latency, t_max_latency =\
            result
        queries += t_queries
        rows += t_rows
        if latency_stats is None:
            latency_stats = t_latency_stats
        else:
            latency_stats = np.add(latency_stats, t_latency_stats)
        if t_max_latency > max_latency:
            max_latency = t_max_latency
        if t_min_latency < min_latency:
            min_latency = t_min_latency
    data = {
        'queries': queries,
        'rows': rows,
        'duration': duration,
        'min_latency': min_latency,
        'max_latency': max_latency,
        'latency_stats': latency_stats.tolist(),
        'output_format': args.output_format
    }
    return data


async def runner(
        args,
        connector,
        executor,
        is_async,
        query,
        query_args,
        is_cache_query
            ):
    timeout = args.timeout * 1000
    concurrency = args.concurrency

    queryname = args.queryname
    if QUERIES[queryname]["setup"]:
        conn = psycopg_connect(args)
        cur = conn.cursor()
        for setup_query in QUERIES[queryname]["setup"].split(';'):
            if len(setup_query):
                cur.execute(setup_query)
                conn.commit()
        cur.close()
        conn.close()
        if QUERIES[queryname]["populate_before_start"]:
            pg_params = {
                "database": args.pgdatabase,
                "user": args.pguser,
                "password": args.pgpassword,
                "host": args.pghost,
                "port": args.pgport
            }
            QUERIES[queryname]["populate_before_start"](pg_params)

    async def _do_run(run_duration):
        start = time.monotonic()

        tasks = []

        if is_async:
            # Asyncio driver
            for i in range(concurrency):
                task = worker(executor, [conns[i], query],  # noqa , query_args], add args
                              start, args.duration, timeout)
                tasks.append(task)
            try:
                results = await asyncio.gather(*tasks)
            except Exception as e:
                print(e)
        else:
            # Sync driver
            with futures.ThreadPoolExecutor(max_workers=concurrency) as e:
                for i in range(concurrency):
                    task = e.submit(sync_worker, executor,
                                    [conns[i], query],  # query_args],
                                    start, run_duration, timeout)
                    tasks.append(task)

                results = [fut.result() for fut in futures.wait(tasks).done]

        end = time.monotonic()

        return results, end - start

    conns = []

    if 'sa' in args.driver:
        async with create_engine(
                user=args.pguser,
                database=args.pgdatabase,
                host=args.pghost,
                password=args.pgpassword,
                minsize=concurrency,
                maxsize=concurrency
        ) as engine:
            for i in range(concurrency):
                conn = await connector(engine)
                conns.append(conn)
            if args.warmup_time:
                await _do_run(args.warmup_time)
            results, duration = await _do_run(args.duration)
            engine.terminate()
        data = map_results(results, duration)

        if QUERIES[queryname]["teardown"]:
            conn = psycopg_connect(args)
            cur = conn.cursor()
            for teardown_query in QUERIES[queryname]["teardown"].split(';'):
                if teardown_query.strip():
                    cur.execute(teardown_query.strip())
            conn.commit()
            cur.close()
            conn.close()
        print(json.dumps(data))
    elif args.driver in ['psycopg', 'aiopg']:
        for i in range(concurrency):
            if is_async:
                conn = await connector(args)
            else:
                conn = connector(args)
            conns.append(conn)
        try:
            try:
                if args.warmup_time:
                    await _do_run(args.warmup_time)

                results, duration = await _do_run(args.duration)
            finally:
                for conn in conns:
                    if is_async:
                        await conn.close()
                    else:
                        conn.close()
            data = map_results(results, duration)

        finally:
            if QUERIES[queryname]["teardown"]:
                conn = psycopg_connect(args)
                cur = conn.cursor()
                for teardown_query in QUERIES[queryname]["teardown"].split(';'):
                    if len(teardown_query):
                        cur.execute(teardown_query)
                        conn.commit()
                cur.close()
                conn.close()
        print(json.dumps(data))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    parser = argparse.ArgumentParser(
        description='benchmark')
    parser.add_argument(
        '-C', '--concurrency', type=int, default=10,
        help='number of concurrent connections')
    parser.add_argument(
        '-D', '--duration', type=int, default=30,
        help='duration of test in seconds')
    parser.add_argument(
        '--timeout', default=2, type=int,
        help='server timeout in seconds')
    parser.add_argument(
        '--warmup-time', type=int, default=5,
        help='duration of warmup period for each benchmark in seconds')
    parser.add_argument(
        '--output-format', default='text', type=str,
        help='output format', choices=['text', 'json'])
    parser.add_argument(
        '--pghost', type=str, default='127.0.0.1',
        help='PostgreSQL server host')
    parser.add_argument(
        '--pgport', type=int, default=5432,
        help='PostgreSQL server port')
    parser.add_argument(
        '--pguser', type=str, default='postgres',
        help='PostgreSQL server user')
    parser.add_argument(
        '--pgdatabase', type=str, default='postgres',
        help='PostgreSQL database name')
    parser.add_argument(
        '--pgpassword', type=str, default='postgres',
        help='PostgreSQL server password')
    parser.add_argument(
        'driver', help='driver implementation to use',
        choices=['psycopg', 'aiopg', 'aiopg_sa', 'aiopg_sa_cache'])
    parser.add_argument(
        'queryname',
        help='query from queries.QUERIES object',
        choices=QUERIES.keys()
    )

    args = parser.parse_args()

    if args.driver == 'psycopg':
        is_async = False
        connector = psycopg_connect
        executor = psycopg_execute
    elif args.driver == 'aiopg':
        is_async = True
        connector = aiopg_connect
        executor = aiopg_execute
    elif args.driver in ['aiopg_sa', 'aiopg_sa_cache']:
        is_async = True
        connector = aiopg_sa_connect
        executor = aiopg_sa_execute
    else:
        raise ValueError('unexpected driver: {!r}'.format(args.driver))
    query = QUERIES[args.queryname][args.driver]
    query_args = QUERIES[args.queryname]['args']

    if 'cache' in args.driver:
        is_cache_query = True
    else:
        is_cache_query = False

    runner_coro = runner(
        args,
        connector,
        executor,
        is_async,
        query,
        query_args,
        is_cache_query
    )

    loop.run_until_complete(runner_coro)
