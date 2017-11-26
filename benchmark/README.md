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
      "name": "psycopg",
      "variations": [
        {
          "duration": 30.0,
          "queries": 373705,
          "qps": 12456.37,
          "rps": 1245636.61,
          "latency_min": 0.15,
          "latency_mean": 0.795,
          "latency_max": 10.11,
          "latency_std": 0.548,
          "latency_cv": 69.03,
          "latency_percentiles": [
            [25,0.386],
            [50,0.637],
            [75,1.044],
            [90,1.532],
            [99,2.635],
            [99.99, 6.183]
          ]
        }
      ]
    },
    {
      "name": "aiopg",
      "variations": [
        {
          "duration": 30.0,
          "queries": 186569,
          "qps": 6218.82,
          "rps": 621881.61,
          "latency_min": 0.72,
          "latency_mean": 1.602,
          "latency_max": 6.19,
          "latency_std": 0.112,
          "latency_cv": 6.97,
          "latency_percentiles": [
            [25,1.543],
            [50,1.598],
            [75,1.664],
            [90,1.722],
            [99,1.866],
            [99.99,3.523]
          ]
        }
      ]
    },
    {
      "name": "aiopg_sa",
      "variations": [
        {
          "duration": 30.0,
          "queries": 79803,
          "qps": 2659.86,
          "rps": 265985.77,
          "latency_min": 1.54,
          "latency_mean": 3.752,
          "latency_max": 17.9,
          "latency_std": 0.785,
          "latency_cv": 20.92,
          "latency_percentiles": [
            [25,3.417],
            [50,3.587],
            [75,3.85],
            [90,4.2],
            [99,8.022],
            [99.99,17.815]
          ]
        }
      ]
    }
  ]
}
```
