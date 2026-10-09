"""
Interactive / long-running Spark session over the Data Lake output tables.

    python explore.py            # interactive shell, Spark UI on localhost:4040
    python explore.py --serve    # no shell, just keep the UI alive

Both modes register the five output tables as queryable temp views:
    songs  artists  users  time_table  songplays
"""
import os
import sys
import time

from pyspark.sql import SparkSession

OUTPUT = os.environ.get("OUTPUT_DATA", "output/")
TABLES = ["songs", "artists", "users", "time_table", "songplays"]
UI_PORT = os.environ.get("SPARK_UI_PORT", "4040")


def main():
    serve_only = "--serve" in sys.argv

    spark = (SparkSession.builder
             .appName("SparkifyDataLake-explore")
             .master("local[*]")
             .config("spark.ui.port", UI_PORT)
             .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    frames = {}
    for t in TABLES:
        df = spark.read.parquet(OUTPUT + t)
        df.createOrReplaceTempView(t)
        df.cache().count()          # populate the UI's Storage tab
        frames[t] = df

    ui = spark.sparkContext.uiWebUrl
    print("")
    print("  Spark UI   : {}".format(ui))
    print("  Master     : {}".format(spark.sparkContext.master))
    print("  Tables     : {}".format("  ".join(TABLES)))
    for t in TABLES:
        print("    {:<12} {:>6} rows".format(t, frames[t].count()))
    print("")

    if serve_only:
        print("  Serving UI. Ctrl-C or kill this process to stop.")
        sys.stdout.flush()
        while True:
            time.sleep(3600)
        return

    import code
    try:
        import readline  # noqa: F401  (arrow keys / history in the REPL)
    except ImportError:
        pass

    banner = ("  Try:  spark.sql('select level, count(*) from songplays "
              "group by level').show()\n"
              "  Ctrl-D to exit.\n")
    local = {"spark": spark, "sql": spark.sql}
    local.update(frames)
    code.interact(banner=banner, local=local)
    spark.stop()


if __name__ == "__main__":
    main()
