import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    expr,
    from_json,
    greatest,
    lit,
    max as spark_max,
    min as spark_min,
    stddev_pop,
    sum as spark_sum,
    to_json,
    struct,
    udf,
    when,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# ENVIRONMENT
# ============================================================

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092",
)

RAW_TOPIC = os.getenv(
    "RAW_TOPIC",
    "crypto.raw.trades",
)

SCORE_TOPIC = os.getenv(
    "SCORE_TOPIC",
    "crypto.market.scores",
)

ANOMALY_TOPIC = os.getenv(
    "ANOMALY_TOPIC",
    "crypto.anomalies",
)

HDFS_BASE = os.getenv(
    "HDFS_BASE",
    "hdfs://namenode:9000/crypto-anomaly",
)

DEMO_MEDIUM_THRESHOLD = float(
    os.getenv(
        "DEMO_MEDIUM_THRESHOLD",
        "35.0",
    )
)

RUNTIME_CONFIG_PATH = os.getenv(
    "RUNTIME_CONFIG_PATH",
    "/opt/project/runtime/demo_mode.json",
)


# ============================================================
# HISTORICAL P95 FEATURE BASELINES
# ============================================================

BTC_TRADE_COUNT_P95 = 1800.0
BTC_PRICE_RANGE_P95 = 0.154633
BTC_VOLATILITY_P95 = 0.00043187
BTC_IMBALANCE_P95 = 0.796748

ETH_TRADE_COUNT_P95 = 863.0
ETH_PRICE_RANGE_P95 = 0.155301
ETH_VOLATILITY_P95 = 0.00044227
ETH_IMBALANCE_P95 = 0.789474


# ============================================================
# PRODUCTION EMPIRICAL SCORE THRESHOLDS
#
# MEDIUM   = empirical P90
# HIGH     = empirical P95
# CRITICAL = empirical P99
# ============================================================

BTC_PRODUCTION_MEDIUM = 67.20
BTC_HIGH_THRESHOLD = 86.63
BTC_CRITICAL_THRESHOLD = 95.53

ETH_PRODUCTION_MEDIUM = 72.45
ETH_HIGH_THRESHOLD = 85.58
ETH_CRITICAL_THRESHOLD = 91.85


# ============================================================
# RUNTIME DEMO MODE
#
# This is intentionally read while Spark is running.
#
# Dashboard writes:
#
# {"demo_mode": true}
#
# or:
#
# {"demo_mode": false}
#
# Spark therefore does NOT need to restart when the dashboard
# switch is changed.
# ============================================================

def read_demo_mode():
    try:

        with open(
            RUNTIME_CONFIG_PATH,
            "r",
            encoding="utf-8-sig",
        ) as config_file:

            config = json.load(
                config_file
            )

        return bool(
            config.get(
                "demo_mode",
                False,
            )
        )

    except Exception:
        return False


runtime_demo_mode_udf = udf(
    read_demo_mode,
    BooleanType(),
)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName(
        "RealTimeCryptoMarketAnomalyDetection"
    )
    .config(
        "spark.sql.session.timeZone",
        "UTC",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


# ============================================================
# INPUT SCHEMA
# ============================================================

schema = StructType(
    [
        StructField(
            "event_type",
            StringType(),
            True,
        ),

        StructField(
            "event_time_ms",
            LongType(),
            False,
        ),

        StructField(
            "trade_time_ms",
            LongType(),
            False,
        ),

        StructField(
            "symbol",
            StringType(),
            False,
        ),

        StructField(
            "agg_trade_id",
            LongType(),
            False,
        ),

        StructField(
            "price",
            DoubleType(),
            False,
        ),

        StructField(
            "quantity",
            DoubleType(),
            False,
        ),

        StructField(
            "buyer_is_maker",
            BooleanType(),
            False,
        ),

        StructField(
            "ingested_at",
            StringType(),
            True,
        ),
    ]
)


# ============================================================
# KAFKA INPUT
# ============================================================

raw_kafka = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        BOOTSTRAP,
    )

    .option(
        "subscribe",
        RAW_TOPIC,
    )

    .option(
        "startingOffsets",
        "latest",
    )

    .option(
        "failOnDataLoss",
        "false",
    )

    .load()
)


# ============================================================
# NORMALIZE TRADES
# ============================================================

trades = (
    raw_kafka

    .select(
        from_json(
            col("value").cast("string"),
            schema,
        ).alias("d")
    )

    .select(
        "d.*"
    )

    .withColumn(
        "event_time",
        (
            col("event_time_ms")
            / lit(1000)
        ).cast("timestamp"),
    )

    .withColumn(
        "notional",
        col("price")
        * col("quantity"),
    )

    .withColumn(
        "is_sell_taker",
        col(
            "buyer_is_maker"
        ).cast("int"),
    )

    .withColumn(
        "is_buy_taker",
        lit(1)
        - col(
            "buyer_is_maker"
        ).cast("int"),
    )

    .withWatermark(
        "event_time",
        "30 seconds",
    )
)


# ============================================================
# RAW TRADES → HDFS
# ============================================================

raw_hdfs_query = (
    trades

    .select(
        "event_time",
        "event_time_ms",
        "trade_time_ms",
        "symbol",
        "agg_trade_id",
        "price",
        "quantity",
        "notional",
        "buyer_is_maker",
        "ingested_at",
    )

    .writeStream

    .format(
        "parquet"
    )

    .outputMode(
        "append"
    )

    .option(
        "path",
        f"{HDFS_BASE}/raw_trades",
    )

    .option(
        "checkpointLocation",
        f"{HDFS_BASE}/checkpoints/raw_trades",
    )

    .partitionBy(
        "symbol"
    )

    .start()
)


# ============================================================
# 30 SECOND WINDOWS
# SLIDE EVERY 10 SECONDS
# ============================================================

features = (
    trades

    .groupBy(
        window(
            col("event_time"),
            "30 seconds",
            "10 seconds",
        ),

        col("symbol"),
    )

    .agg(
        count(
            lit(1)
        ).alias(
            "trade_count"
        ),

        avg(
            "price"
        ).alias(
            "mean_price"
        ),

        stddev_pop(
            "price"
        ).alias(
            "price_std"
        ),

        spark_min(
            "price"
        ).alias(
            "min_price"
        ),

        spark_max(
            "price"
        ).alias(
            "max_price"
        ),

        spark_sum(
            "quantity"
        ).alias(
            "volume"
        ),

        spark_sum(
            "notional"
        ).alias(
            "notional_volume"
        ),

        spark_sum(
            "is_buy_taker"
        ).alias(
            "buy_trades"
        ),

        spark_sum(
            "is_sell_taker"
        ).alias(
            "sell_trades"
        ),
    )

    .withColumn(
        "price_std",
        expr(
            "coalesce(price_std, 0.0)"
        ),
    )

    .withColumn(
        "price_range_pct",
        expr(
            """
            CASE
                WHEN mean_price > 0
                THEN
                    (
                        (max_price - min_price)
                        / mean_price
                    ) * 100
                ELSE 0
            END
            """
        ),
    )

    .withColumn(
        "buy_sell_imbalance",
        expr(
            """
            CASE
                WHEN
                    (buy_trades + sell_trades) > 0
                THEN
                    abs(
                        buy_trades - sell_trades
                    )
                    /
                    (
                        buy_trades + sell_trades
                    )
                ELSE 0
            END
            """
        ),
    )

    .withColumn(
        "net_buy_pressure",
        expr(
            """
            CASE
                WHEN
                    (buy_trades + sell_trades) > 0
                THEN
                    (
                        buy_trades - sell_trades
                    )
                    /
                    (
                        buy_trades + sell_trades
                    )
                ELSE 0
            END
            """
        ),
    )

    .withColumn(
        "order_flow_direction",
        expr(
            """
            CASE
                WHEN net_buy_pressure > 0
                    THEN 'BUY_SIDE'

                WHEN net_buy_pressure < 0
                    THEN 'SELL_SIDE'

                ELSE 'BALANCED'
            END
            """
        ),
    )

    .withColumn(
        "coefficient_of_variation",
        expr(
            """
            CASE
                WHEN mean_price > 0
                THEN
                    price_std
                    / mean_price
                ELSE 0
            END
            """
        ),
    )
)


# ============================================================
# SYMBOL-SPECIFIC P95 BASELINES
# ============================================================

calibrated_features = (
    features

    .withColumn(
        "price_range_baseline",

        when(
            col("symbol")
            == "BTCUSDT",

            lit(
                BTC_PRICE_RANGE_P95
            ),
        )

        .when(
            col("symbol")
            == "ETHUSDT",

            lit(
                ETH_PRICE_RANGE_P95
            ),
        )

        .otherwise(
            lit(
                BTC_PRICE_RANGE_P95
            ),
        ),
    )

    .withColumn(
        "volatility_baseline",

        when(
            col("symbol")
            == "BTCUSDT",

            lit(
                BTC_VOLATILITY_P95
            ),
        )

        .when(
            col("symbol")
            == "ETHUSDT",

            lit(
                ETH_VOLATILITY_P95
            ),
        )

        .otherwise(
            lit(
                BTC_VOLATILITY_P95
            ),
        ),
    )

    .withColumn(
        "imbalance_baseline",

        when(
            col("symbol")
            == "BTCUSDT",

            lit(
                BTC_IMBALANCE_P95
            ),
        )

        .when(
            col("symbol")
            == "ETHUSDT",

            lit(
                ETH_IMBALANCE_P95
            ),
        )

        .otherwise(
            lit(
                BTC_IMBALANCE_P95
            ),
        ),
    )

    .withColumn(
        "activity_baseline",

        when(
            col("symbol")
            == "BTCUSDT",

            lit(
                BTC_TRADE_COUNT_P95
            ),
        )

        .when(
            col("symbol")
            == "ETHUSDT",

            lit(
                ETH_TRADE_COUNT_P95
            ),
        )

        .otherwise(
            lit(
                BTC_TRADE_COUNT_P95
            ),
        ),
    )
)


# ============================================================
# NORMALIZED COMPONENTS
#
# Each signal intensity is normalized against historical P95.
# ============================================================

scored = (
    calibrated_features

    .withColumn(
        "price_component",

        greatest(
            lit(0.0),

            expr(
                """
                least(
                    price_range_pct
                    / price_range_baseline,
                    1.0
                )
                """
            ),
        ),
    )

    .withColumn(
        "volatility_component",

        greatest(
            lit(0.0),

            expr(
                """
                least(
                    coefficient_of_variation
                    / volatility_baseline,
                    1.0
                )
                """
            ),
        ),
    )

    .withColumn(
        "imbalance_component",

        greatest(
            lit(0.0),

            expr(
                """
                least(
                    buy_sell_imbalance
                    / imbalance_baseline,
                    1.0
                )
                """
            ),
        ),
    )

    .withColumn(
        "activity_component",

        greatest(
            lit(0.0),

            expr(
                """
                least(
                    trade_count
                    / activity_baseline,
                    1.0
                )
                """
            ),
        ),
    )
)


# ============================================================
# WEIGHTED SCORE
#
# Price Range         = 35%
# Volatility          = 30%
# Buy/Sell Imbalance  = 20%
# Trade Activity      = 15%
# ============================================================

scored = (
    scored

    .withColumn(
        "anomaly_score",

        expr(
            """
            round(
                100 * (
                    0.35 * price_component
                    +
                    0.30 * volatility_component
                    +
                    0.20 * imbalance_component
                    +
                    0.15 * activity_component
                ),
                2
            )
            """
        ),
    )
)


# ============================================================
# REAL PRODUCTION THRESHOLDS
# ============================================================

scored = (
    scored

    .withColumn(
        "production_medium_threshold",

        when(
            col("symbol")
            == "BTCUSDT",

            lit(
                BTC_PRODUCTION_MEDIUM
            ),
        )

        .when(
            col("symbol")
            == "ETHUSDT",

            lit(
                ETH_PRODUCTION_MEDIUM
            ),
        )

        .otherwise(
            lit(
                BTC_PRODUCTION_MEDIUM
            ),
        ),
    )

    .withColumn(
        "high_threshold",

        when(
            col("symbol")
            == "BTCUSDT",

            lit(
                BTC_HIGH_THRESHOLD
            ),
        )

        .when(
            col("symbol")
            == "ETHUSDT",

            lit(
                ETH_HIGH_THRESHOLD
            ),
        )

        .otherwise(
            lit(
                BTC_HIGH_THRESHOLD
            ),
        ),
    )

    .withColumn(
        "critical_threshold",

        when(
            col("symbol")
            == "BTCUSDT",

            lit(
                BTC_CRITICAL_THRESHOLD
            ),
        )

        .when(
            col("symbol")
            == "ETHUSDT",

            lit(
                ETH_CRITICAL_THRESHOLD
            ),
        )

        .otherwise(
            lit(
                BTC_CRITICAL_THRESHOLD
            ),
        ),
    )
)


# ============================================================
# LIVE DASHBOARD-CONTROLLED DEMO MODE
#
# The Python UDF checks the shared runtime file while Spark is
# running. Therefore the next processed market window can use
# the updated setting without restarting Spark.
# ============================================================

scored = (
    scored

    .withColumn(
        "demo_mode",
        runtime_demo_mode_udf(),
    )

    .withColumn(
        "threshold_mode",

        when(
            col("demo_mode"),
            lit("DEMO"),
        )

        .otherwise(
            lit("PRODUCTION")
        ),
    )

    .withColumn(
        "medium_threshold",

        when(
            col("demo_mode"),

            lit(
                DEMO_MEDIUM_THRESHOLD
            ),
        )

        .otherwise(
            col(
                "production_medium_threshold"
            )
        ),
    )
)


# ============================================================
# SEVERITY
#
# CRITICAL and HIGH always use the real calibrated levels.
#
# Only MEDIUM changes in Demo Mode.
# ============================================================

scored = (
    scored

    .withColumn(
        "severity",

        expr(
            """
            CASE

                WHEN anomaly_score
                     >= critical_threshold
                    THEN 'CRITICAL'

                WHEN anomaly_score
                     >= high_threshold
                    THEN 'HIGH'

                WHEN anomaly_score
                     >= medium_threshold
                    THEN 'MEDIUM'

                ELSE 'NORMAL'

            END
            """
        ),
    )

    .withColumn(
        "dominant_signal",

        expr(
            """
            CASE

                WHEN price_component
                     >= volatility_component
                 AND price_component
                     >= imbalance_component
                 AND price_component
                     >= activity_component
                    THEN 'PRICE_RANGE'

                WHEN volatility_component
                     >= imbalance_component
                 AND volatility_component
                     >= activity_component
                    THEN 'VOLATILITY'

                WHEN imbalance_component
                     >= activity_component
                    THEN 'BUY_SELL_IMBALANCE'

                ELSE 'TRADE_ACTIVITY'

            END
            """
        ),
    )
)


# ============================================================
# FLATTEN WINDOW STRUCT
# ============================================================

scored_flat = (
    scored

    .select(
        col(
            "window.start"
        ).alias(
            "window_start"
        ),

        col(
            "window.end"
        ).alias(
            "window_end"
        ),

        "symbol",

        "trade_count",

        "mean_price",

        "price_std",

        "min_price",

        "max_price",

        "volume",

        "notional_volume",

        "buy_trades",

        "sell_trades",

        "price_range_pct",

        "buy_sell_imbalance",

        "net_buy_pressure",

        "order_flow_direction",

        "coefficient_of_variation",

        "price_range_baseline",

        "volatility_baseline",

        "imbalance_baseline",

        "activity_baseline",

        "price_component",

        "volatility_component",

        "imbalance_component",

        "activity_component",

        "anomaly_score",

        "demo_mode",

        "threshold_mode",

        "production_medium_threshold",

        "medium_threshold",

        "high_threshold",

        "critical_threshold",

        "severity",

        "dominant_signal",
    )
)


# ============================================================
# WINDOW SCORES → HDFS
# ============================================================

scores_hdfs_query = (
    scored_flat

    .writeStream

    .format(
        "parquet"
    )

    .outputMode(
        "append"
    )

    .option(
        "path",
        f"{HDFS_BASE}/window_scores",
    )

    .option(
        "checkpointLocation",
        f"{HDFS_BASE}/checkpoints/window_scores",
    )

    .start()
)


# ============================================================
# ALL SCORES → KAFKA
# ============================================================

score_kafka = (
    scored_flat

    .select(
        col(
            "symbol"
        )
        .cast(
            "string"
        )
        .alias(
            "key"
        ),

        to_json(
            struct(
                *[
                    col(column_name)

                    for column_name
                    in scored_flat.columns
                ]
            )
        ).alias(
            "value"
        ),
    )
)


score_kafka_query = (
    score_kafka

    .writeStream

    .format(
        "kafka"
    )

    .option(
        "kafka.bootstrap.servers",
        BOOTSTRAP,
    )

    .option(
        "topic",
        SCORE_TOPIC,
    )

    .option(
        "checkpointLocation",
        f"{HDFS_BASE}/checkpoints/score_kafka",
    )

    .outputMode(
        "append"
    )

    .start()
)


# ============================================================
# THRESHOLD-CROSSING WINDOWS → ANOMALY KAFKA TOPIC
#
# Production:
#
# BTC MEDIUM = 67.20
# ETH MEDIUM = 72.45
#
# Demo:
#
# BTC MEDIUM = 35
# ETH MEDIUM = 35
# ============================================================

anomalies = (
    scored_flat

    .filter(
        col(
            "anomaly_score"
        )
        >=
        col(
            "medium_threshold"
        )
    )
)


anomaly_kafka = (
    anomalies

    .select(
        col(
            "symbol"
        )
        .cast(
            "string"
        )
        .alias(
            "key"
        ),

        to_json(
            struct(
                *[
                    col(column_name)

                    for column_name
                    in anomalies.columns
                ]
            )
        ).alias(
            "value"
        ),
    )
)


anomaly_kafka_query = (
    anomaly_kafka

    .writeStream

    .format(
        "kafka"
    )

    .option(
        "kafka.bootstrap.servers",
        BOOTSTRAP,
    )

    .option(
        "topic",
        ANOMALY_TOPIC,
    )

    .option(
        "checkpointLocation",
        f"{HDFS_BASE}/checkpoints/anomaly_kafka",
    )

    .outputMode(
        "append"
    )

    .start()
)


# ============================================================
# CONSOLE OUTPUT
# ============================================================

console_query = (
    scored_flat

    .select(
        "window_end",
        "symbol",
        "mean_price",
        "trade_count",
        "anomaly_score",
        "threshold_mode",
        "production_medium_threshold",
        "medium_threshold",
        "high_threshold",
        "critical_threshold",
        "severity",
        "order_flow_direction",
        "dominant_signal",
    )

    .writeStream

    .format(
        "console"
    )

    .option(
        "truncate",
        "false",
    )

    .outputMode(
        "append"
    )

    .start()
)


spark.streams.awaitAnyTermination()