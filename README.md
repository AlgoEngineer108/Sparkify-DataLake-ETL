# Sparkify Data Lake ETL

A PySpark ETL pipeline that builds an analytics-ready data lake for *Sparkify*,
a fictional music streaming service.

Raw JSON — user activity logs and song metadata — is read from S3 (or a local
path), transformed with Spark into a star schema, and written back out as
partitioned Parquet.

```
JSON (S3 / local)  ──►  Spark  ──►  star schema  ──►  Parquet (S3 / local)
  song_data/              ETL        5 tables         partitioned
  log-data/
```

## Schema

**Fact table**

| Table | Grain | Columns |
|---|---|---|
| `songplays` | one row per song play (`page = NextSong`) | `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent` |

**Dimension tables**

| Table | Columns | Partitioned by |
|---|---|---|
| `users` | `user_id`, `first_name`, `last_name`, `gender`, `level` | — |
| `songs` | `song_id`, `title`, `artist_id`, `year`, `duration` | `year`, `artist_id` |
| `artists` | `artist_id`, `name`, `location`, `latitude`, `longitude` | — |
| `time` | `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday` | `year`, `month` |

## Running locally

Requires Java 11 or 17. A sample of the dataset is committed under `data/`, so
no AWS account or credentials are needed.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python etl.py
```

Spark runs in `local[*]` mode and writes Parquet to `output/`. Expected results
against the bundled sample:

| Table | Rows |
|---|---|
| `songs` | 71 |
| `artists` | 69 |
| `users` | 104 |
| `time_table` | 6,813 |
| `songplays` | 4 |

> `songplays` is small by design here: the committed sample contains only 71
> songs (artists under `song_data/A/A/` and `A/B/`) against 6,813 log events, so
> the title join rarely matches. The full dataset yields thousands of rows.

### Exploring the output

```bash
SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python explore.py
```

Opens a Python shell with all five tables registered as temp views and the
Spark UI on <http://localhost:4040>:

```python
>>> sql("select level, count(*) from songplays group by level").show()
>>> songs.filter("year > 2000").show()
```

Use `--serve` to hold the UI open without the shell.

## Running against S3

Create `dl.cfg` from `dl.cfg.example` and supply paths:

```bash
INPUT_DATA=s3a://udacity-dend/ OUTPUT_DATA=s3a://my-bucket/out/ python etl.py
```

The `hadoop-aws` connector is only pulled in when an S3 path is detected, so
local runs stay offline.

### On EMR

```bash
scp -i <key.pem> etl.py <user>@<emr-master>:~/

spark-submit etl.py \
  --master yarn --deploy-mode client \
  --driver-memory 4g --num-executors 2 \
  --executor-memory 2g --executor-cores 2
```

The EMR instance role needs read/write access to the S3 buckets.

## Credits

Originally written by Sanchit Kumar as part of the Udacity Data Engineering
Nanodegree. See [LICENSE](LICENSE) — MIT, and the notice must be retained in
derived work.
