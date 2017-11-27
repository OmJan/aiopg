* to run benchmark, about more options see benchmark.main

```sh
python benchmark.py psycopg,aiopg,aiopg_sa
```

* report saves to benchmark/report.json by default, psycopg runs with 10 treads by default, but aiopg, aiopg.sa in one thread

```json
{
  "date": "%Y-%m-%dT%H:%M:%S%z",
  "duration": 30,
  "concurrency_levels": "10",
  "querynames": [
    "simple_select"
  ],
  "queries": [
    "SELECT id from sa_tbl_1"
  ],
  "benchmarks": [
    {
      "name": "aiopg_sa_cache",
      "variations": [
        {
          "duration": 30.0,
          "queries": 118051,
          "qps": 3934.85,
          "rps": 393485.11,
          "latency_min": 0.84,
          "latency_mean": 2.535,
          "latency_max": 13.31,
          "latency_std": 0.315,
          "latency_cv": 12.44,
          "latency_percentiles": [
            [25,2.355],
            [50,2.506],
            [75,2.639],
            [90,2.781],
            [99,3.834],
            [99.9,7.804]
          ]
        }
      ]
    },
    {
      "name": "aiopg",
      "variations": [
        {
          "duration": 30.0,
          "queries": 182446,
          "qps": 6081.38,
          "rps": 608138.08,
          "latency_min": 0.61,
          "latency_mean": 1.638,
          "latency_max": 13.16,
          "latency_std": 0.31,
          "latency_cv": 18.93,
          "latency_percentiles": [
            [25,1.512],
            [50,1.592],
            [75,1.672],
            [90,1.765],
            [99,2.726],
            [99.99,9.318]
          ]
        }
      ]
    },
    {
      "name": "aiopg_sa",
      "variations": [
        {
          "duration": 30.0,
          "queries": 88508,
          "qps": 2950.13,
          "rps": 295012.51,
          "latency_min": 1.59,
          "latency_mean": 3.383,
          "latency_max": 16.17,
          "latency_std": 0.372,
          "latency_cv": 11.01,
          "latency_percentiles": [
            [25,3.26],
            [50,3.361],
            [75,3.447],
            [90,3.587],
            [99,3.927],
            [99.99,15.913]
          ]
        }
      ]
    }
  ]
}
```
