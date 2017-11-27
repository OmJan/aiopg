"""
sync psycopg vs aiopg vs aiopg.sa vs aiopg.sa with query caching

"""
import sys
import os
import math
from subprocess import call
import argparse
import subprocess
import json
import platform
import warnings

import numpy as np
from docker_postgresql import (
    pg_server,
    pg_server_remove
)

from queries import QUERIES


def platform_info():
    machine = platform.machine()
    processor = platform.processor()
    system = platform.system()

    cpuinfo_f = '/proc/cpuinfo'

    if (processor in {machine, 'unknown'} and os.path.exists(cpuinfo_f)):
        with open(cpuinfo_f, 'rt') as f:
            for line in f:
                if line.startswith('model name'):
                    _, _, p = line.partition(':')
                    processor = p.strip()
                    break

    if 'Linux' in system:

        with warnings.catch_warnings():
            # see issue #1322 for more information
            warnings.filterwarnings(
                'ignore',
                'dist\(\) and linux_distribution\(\) '
                'functions are deprecated .*',
                PendingDeprecationWarning,
            )
            distname, distversion, distid = platform.dist('')

        distribution = '{} {}'.format(distname, distversion).strip()

    else:
        distribution = None

    data = {
        'cpu': processor,
        'arch': machine,
        'system': '{} {}'.format(system, platform.release()),
        'distribution': distribution
    }

    return data


def weighted_quantile(values, quantiles, weights):
    """Very close to np.percentile, but supports weights.
    :param values: np.array with data
    :param quantiles: array-like with many quantiles needed,
           quantiles should be in [0, 1]!
    :param weights: array-like of the same length as `array`
    :return: np.array with computed quantiles.
    """
    values = np.array(values)
    quantiles = np.array(quantiles)
    weights = np.array(weights)
    if not (np.all(quantiles >= 0) and np.all(quantiles <= 1)):
        raise ValueError('quantiles should be in [0, 1]')

    weighted_quantiles = np.cumsum(weights) - 0.5 * weights
    weighted_quantiles -= weighted_quantiles[0]
    weighted_quantiles /= weighted_quantiles[-1]

    return np.interp(quantiles, weighted_quantiles, values)


percentiles = [25, 50, 75, 90, 99, 99.99]


def calc_latency_stats(queries, rows, duration, min_latency, max_latency,
                       latency_stats, output_format='text'):
    arange = np.arange(len(latency_stats))

    mean_latency = np.average(arange, weights=latency_stats)
    variance = np.average((arange - mean_latency) ** 2, weights=latency_stats)
    latency_std = math.sqrt(variance)
    latency_cv = latency_std / mean_latency

    percentile_data = []

    quantiles = weighted_quantile(arange, [p / 100 for p in percentiles],
                                  weights=latency_stats)

    for i, percentile in enumerate(percentiles):
        percentile_data.append((percentile, round(quantiles[i] / 100, 3)))

    data = dict(
        duration=round(duration, 2),
        queries=queries,
        qps=round(queries / duration, 2),
        rps=round(rows / duration, 2),
        latency_min=round(min_latency / 100, 3),
        latency_mean=round(mean_latency / 100, 3),
        latency_max=round(max_latency / 100, 3),
        latency_std=round(latency_std / 100, 3),
        latency_cv=round(latency_cv * 100, 2),
        latency_percentiles=percentile_data
    )

    return data


def format_text(data):
    data = dict(data)

    data['latency_percentiles'] = '; '.join(
        '{}% under {}ms'.format(*v) for v in data['latency_percentiles'])

    output = '''\
{queries} queries in {duration} seconds
Latency: min {latency_min}ms; max {latency_max}ms; mean {latency_mean}ms; \
std: {latency_std}ms ({latency_cv}%)
Latency distribution: {latency_percentiles}
Queries/sec: {qps}
Rows/sec: {rps}
'''.format(**data)

    return output


def process_results(results):
    try:
        lat_data = json.loads(results)
    except json.JSONDecodeError as e:
        print('could not process benchmark results: {}'.format(e),
              file=sys.stderr)
        print(results, file=sys.stderr)
        sys.exit(1)

    latency_stats = np.array(lat_data['latency_stats'])

    return calc_latency_stats(
        lat_data['queries'], lat_data['rows'], lat_data['duration'],
        lat_data['min_latency'], lat_data['max_latency'], latency_stats)


BENCHMARKS = [
    "psycopg",
    "aiopg",
    "aiopg_sa",
    "aiopg_sa_cache"
]


def run_benchmark(
        args,
        benchmarks=BENCHMARKS,
        querynames=[]):
    """
    """
    benchmarks_data = []

    for benchmark in benchmarks:
        if benchmark not in BENCHMARKS:
            continue

        msg = 'Running {} benchmarks...'.format(benchmark)
        print(msg)
        print('=' * len(msg))

        benchmark_data = {
            'name': benchmark,
            'variations': []
        }

        runner_args = {
            'warmup-time': args.warmup_time,
            'duration': args.duration,
            'timeout': args.timeout,
            'pghost': args.pghost,
            'pgport': args.pgport,
            'pguser': args.pguser,
            'pgpassword': args.pgpassword,
            'pgdatabase': args.pgdatabase,
            'output-format': 'json'
        }

        runner_switches = ['--{}={}'.format(k.replace('_', '-'), v)
                           for k, v in runner_args.items()]

        for queryname in querynames:
            print(querynames)
            print('-' * len(querynames))
            print()

            cmdline = ['python3'] + ['runner.py'] + runner_switches + \
                      ['--concurrency={}'.format(args.concurrency)] + \
                      [benchmark, queryname]
            runner_proc = subprocess.run(
                cmdline, stdout=subprocess.PIPE, stderr=sys.stderr)

            if runner_proc.returncode == 3:
                # query not supported by the runner
                continue
            elif runner_proc.returncode != 0:
                msg = 'fatal: benchmark runner exited with exit ' \
                      'code {}'.format(runner_proc.returncode)
                print(msg)
                sys.exit(runner_proc.returncode)
            else:
                data = process_results(runner_proc.stdout.decode('utf-8'))
                benchmark_data['variations'].append(data)
                print(format_text(data))
        benchmarks_data.append(benchmark_data)
    return benchmarks_data


def main():
    parser = argparse.ArgumentParser(
        description='aiopg benchmark runner')
    parser.add_argument(
        '--concurrency', type=str, default='10',
        help='concurrency-levels')
    parser.add_argument(
        '--warmup-time', type=int, default=5,
        help='duration of warmup period for each benchmark in seconds')
    parser.add_argument(
        '--duration', type=int, default=30,
        help='duration of each benchmark in seconds')
    parser.add_argument(
        '--timeout', default=2, type=int,
        help='server timeout in seconds')
    parser.add_argument(
        '--save-json', '-J', default='report.json', type=str,
        help='path to save benchmark results in JSON format')
    parser.add_argument(
        '--pghost', type=str,
        help='PostgreSQL server host.  If not specified, ' +
             'local docker container will be used.')
    parser.add_argument(
        '--pgport', type=int, default=5432,
        help='PostgreSQL server port')
    parser.add_argument(
        '--pguser', type=str, default='postgres',
        help='PostgreSQL server user')
    parser.add_argument(
        '--pgpassword', type=str, default='postgres',
        help='PostgreSQL server password')
    parser.add_argument(
        '--pgdatabase', type=str, default='postgres',
        help='PostgreSQL server password')
    parser.add_argument(
        'benchmarks', help='benchmark(s) split wiht ","',
        type=str,
        default=",".join(BENCHMARKS)
    )
    args = parser.parse_args()
    if not args.pghost:
        container = pg_server(pg_tag='9.5')
        pg_params = container.get('pg_params')
        for i, value in pg_params.items():
            setattr(args, 'pg{}'.format(i), value)
    else:
        pg_params = {
            "host": args.pghost,
            "port": args.pgport,
            "user": args.pguser,
            "password": args.pgpassword,
            "database": args.pgdatabase
        }
    if args.benchmarks:
        benchmarks = [
            item for item in args.benchmarks.split(',') if item in BENCHMARKS]
    else:
        benchmarks = BENCHMARKS
    benchmarks_data = run_benchmark(
        args,
        benchmarks=benchmarks,
        querynames=list(QUERIES.keys())
    )
    if not args.pghost:
        pg_server_remove(container)

    if args.save_json:
        report_data = {
            'date': '%Y-%m-%dT%H:%M:%S%z',
            'duration': args.duration,
            # 'platform': plat_info,
            'concurrency_levels': args.concurrency,
            'querynames': list(QUERIES.keys()),
            'queries': [QUERIES[i]['psycopg'] for i in QUERIES.keys()],
            'benchmarks': benchmarks_data,
        }

    if args.save_json:
        with open(args.save_json, 'w') as f:
            json.dump(report_data, f)


if __name__ == '__main__':
    main()
