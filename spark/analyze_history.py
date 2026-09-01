from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    expr,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    stddev_pop,
    when,
)


# ============================================================
# CONFIGURATION
# ============================================================

HDFS_WINDOW_SCORES = (
    "hdfs://namenode:9000/crypto-anomaly/window_scores"
)


# ============================================================
# HISTORICALLY DERIVED P95 BASELINES
#
# These values were derived from the first historical
# calibration analysis performed over HDFS window features.
#
# BTCUSDT
# ------------------------------------------------------------
# trade_count              p95 = 1800
# price_range_pct          p95 = 0.154633
# coefficient_of_variation p95 = 0.00043187
# buy_sell_imbalance       p95 = 0.796748
#
# ETHUSDT
# ------------------------------------------------------------
# trade_count              p95 = 863
# price_range_pct          p95 = 0.155301
# coefficient_of_variation p95 = 0.00044227
# buy_sell_imbalance       p95 = 0.789474
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
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CryptoCalibratedHistoricalAnalysis")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.parquet.mergeSchema", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


print()
print("=" * 78)
print(
    " REAL-TIME CRYPTO ANOMALY DETECTION"
    " — CALIBRATED HISTORICAL ANALYSIS"
)
print("=" * 78)
print()


# ============================================================
# READ HISTORICAL WINDOW FEATURES FROM HDFS
# ============================================================

raw_df = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(HDFS_WINDOW_SCORES)
)


required_columns = [
    "window_start",
    "window_end",
    "symbol",
    "trade_count",
    "price_range_pct",
    "coefficient_of_variation",
    "buy_sell_imbalance",
]


missing_columns = [
    c
    for c in required_columns
    if c not in raw_df.columns
]


if missing_columns:
    raise RuntimeError(
        "Historical dataset is missing required columns: "
        + ", ".join(missing_columns)
    )


# ============================================================
# CLEAN DATA
# ============================================================

base_df = (
    raw_df
    .select(*required_columns)
    .dropna(
        subset=[
            "window_start",
            "window_end",
            "symbol",
            "trade_count",
            "price_range_pct",
            "coefficient_of_variation",
            "buy_sell_imbalance",
        ]
    )
    .filter(
        col("symbol").isin(
            "BTCUSDT",
            "ETHUSDT",
        )
    )
)


# ============================================================
# CHECK DUPLICATES
# ============================================================

raw_count = base_df.count()


dedup_df = (
    base_df
    .dropDuplicates(
        [
            "symbol",
            "window_start",
            "window_end",
        ]
    )
)


dedup_count = dedup_df.count()

duplicate_count = raw_count - dedup_count


print("=" * 78)
print(" DATASET QUALITY")
print("=" * 78)

print(
    f"Raw historical rows              : {raw_count}"
)

print(
    f"Unique symbol/window rows        : {dedup_count}"
)

print(
    f"Duplicate rows removed           : {duplicate_count}"
)

print()


# ============================================================
# HISTORICAL WINDOW COUNTS
# ============================================================

print("=" * 78)
print(" UNIQUE HISTORICAL WINDOWS BY SYMBOL")
print("=" * 78)

(
    dedup_df
    .groupBy("symbol")
    .count()
    .orderBy("symbol")
    .show(
        truncate=False
    )
)


# ============================================================
# ATTACH SYMBOL-SPECIFIC BASELINES
# ============================================================

calibrated = (
    dedup_df

    .withColumn(
        "price_range_baseline",
        when(
            col("symbol") == "BTCUSDT",
            lit(BTC_PRICE_RANGE_P95),
        )
        .when(
            col("symbol") == "ETHUSDT",
            lit(ETH_PRICE_RANGE_P95),
        ),
    )

    .withColumn(
        "volatility_baseline",
        when(
            col("symbol") == "BTCUSDT",
            lit(BTC_VOLATILITY_P95),
        )
        .when(
            col("symbol") == "ETHUSDT",
            lit(ETH_VOLATILITY_P95),
        ),
    )

    .withColumn(
        "imbalance_baseline",
        when(
            col("symbol") == "BTCUSDT",
            lit(BTC_IMBALANCE_P95),
        )
        .when(
            col("symbol") == "ETHUSDT",
            lit(ETH_IMBALANCE_P95),
        ),
    )

    .withColumn(
        "activity_baseline",
        when(
            col("symbol") == "BTCUSDT",
            lit(BTC_TRADE_COUNT_P95),
        )
        .when(
            col("symbol") == "ETHUSDT",
            lit(ETH_TRADE_COUNT_P95),
        ),
    )
)


# ============================================================
# RECOMPUTE ALL COMPONENTS USING THE NEW CALIBRATED MODEL
#
# IMPORTANT:
# We deliberately DO NOT use the old stored anomaly_score.
#
# Every historical window is rescored from its original
# market features using the new per-symbol historical P95
# normalization.
# ============================================================

calibrated = (
    calibrated

    .withColumn(
        "cal_price_component",
        expr(
            """
            greatest(
                0.0,
                least(
                    price_range_pct / price_range_baseline,
                    1.0
                )
            )
            """
        ),
    )

    .withColumn(
        "cal_volatility_component",
        expr(
            """
            greatest(
                0.0,
                least(
                    coefficient_of_variation
                    / volatility_baseline,
                    1.0
                )
            )
            """
        ),
    )

    .withColumn(
        "cal_imbalance_component",
        expr(
            """
            greatest(
                0.0,
                least(
                    buy_sell_imbalance
                    / imbalance_baseline,
                    1.0
                )
            )
            """
        ),
    )

    .withColumn(
        "cal_activity_component",
        expr(
            """
            greatest(
                0.0,
                least(
                    trade_count / activity_baseline,
                    1.0
                )
            )
            """
        ),
    )

    .withColumn(
        "calibrated_score",
        expr(
            """
            round(
                100 * (
                    0.35 * cal_price_component
                    +
                    0.30 * cal_volatility_component
                    +
                    0.20 * cal_imbalance_component
                    +
                    0.15 * cal_activity_component
                ),
                2
            )
            """
        ),
    )
)


# ============================================================
# CALIBRATED DOMINANT SIGNAL
# ============================================================

calibrated = (
    calibrated
    .withColumn(
        "calibrated_dominant_signal",
        expr(
            """
            CASE

                WHEN cal_price_component
                     >= cal_volatility_component
                 AND cal_price_component
                     >= cal_imbalance_component
                 AND cal_price_component
                     >= cal_activity_component
                    THEN 'PRICE_RANGE'

                WHEN cal_volatility_component
                     >= cal_imbalance_component
                 AND cal_volatility_component
                     >= cal_activity_component
                    THEN 'VOLATILITY'

                WHEN cal_imbalance_component
                     >= cal_activity_component
                    THEN 'BUY_SELL_IMBALANCE'

                ELSE 'TRADE_ACTIVITY'

            END
            """
        ),
    )
)


# ============================================================
# COMPONENT DISTRIBUTION
# ============================================================

print()
print("=" * 78)
print(" CALIBRATED COMPONENT DISTRIBUTION")
print("=" * 78)


component_metrics = [
    "cal_price_component",
    "cal_volatility_component",
    "cal_imbalance_component",
    "cal_activity_component",
]


for metric in component_metrics:

    print()
    print("-" * 78)
    print(
        f" COMPONENT: {metric}"
    )
    print("-" * 78)

    (
        calibrated
        .groupBy("symbol")
        .agg(
            count("*").alias(
                "windows"
            ),

            spark_round(
                avg(metric),
                4,
            ).alias(
                "mean"
            ),

            spark_round(
                stddev_pop(metric),
                4,
            ).alias(
                "stddev"
            ),

            spark_round(
                spark_min(metric),
                4,
            ).alias(
                "min"
            ),

            expr(
                f"percentile_approx({metric}, 0.50, 10000)"
            ).alias(
                "p50"
            ),

            expr(
                f"percentile_approx({metric}, 0.90, 10000)"
            ).alias(
                "p90"
            ),

            expr(
                f"percentile_approx({metric}, 0.95, 10000)"
            ).alias(
                "p95"
            ),

            expr(
                f"percentile_approx({metric}, 0.99, 10000)"
            ).alias(
                "p99"
            ),

            spark_round(
                spark_max(metric),
                4,
            ).alias(
                "max"
            ),
        )
        .orderBy("symbol")
        .show(
            truncate=False
        )
    )


# ============================================================
# CALIBRATED SCORE DISTRIBUTION
# ============================================================

print()
print("=" * 78)
print(" CALIBRATED ANOMALY SCORE DISTRIBUTION")
print("=" * 78)

score_distribution = (
    calibrated
    .groupBy("symbol")
    .agg(
        count("*").alias(
            "windows"
        ),

        spark_round(
            avg("calibrated_score"),
            2,
        ).alias(
            "mean"
        ),

        spark_round(
            stddev_pop(
                "calibrated_score"
            ),
            2,
        ).alias(
            "stddev"
        ),

        spark_round(
            spark_min(
                "calibrated_score"
            ),
            2,
        ).alias(
            "min"
        ),

        expr(
            """
            percentile_approx(
                calibrated_score,
                0.50,
                10000
            )
            """
        ).alias(
            "p50"
        ),

        expr(
            """
            percentile_approx(
                calibrated_score,
                0.90,
                10000
            )
            """
        ).alias(
            "p90"
        ),

        expr(
            """
            percentile_approx(
                calibrated_score,
                0.95,
                10000
            )
            """
        ).alias(
            "p95"
        ),

        expr(
            """
            percentile_approx(
                calibrated_score,
                0.99,
                10000
            )
            """
        ).alias(
            "p99"
        ),

        spark_round(
            spark_max(
                "calibrated_score"
            ),
            2,
        ).alias(
            "max"
        ),
    )
    .orderBy("symbol")
)


score_distribution.show(
    truncate=False
)


# ============================================================
# CURRENT 35 / 55 / 75 THRESHOLDS APPLIED TO NEW SCORES
#
# This tells us how sensitive the original severity cutoffs
# would be AFTER component calibration.
# ============================================================

print()
print("=" * 78)
print(" CURRENT 35 / 55 / 75 RATES ON CALIBRATED SCORES")
print("=" * 78)


threshold_rates = (
    calibrated
    .groupBy("symbol")
    .agg(
        count("*").alias(
            "total_windows"
        ),

        expr(
            """
            sum(
                case
                    when calibrated_score >= 35
                    then 1
                    else 0
                end
            )
            """
        ).alias(
            "medium_or_above"
        ),

        expr(
            """
            sum(
                case
                    when calibrated_score >= 55
                    then 1
                    else 0
                end
            )
            """
        ).alias(
            "high_or_above"
        ),

        expr(
            """
            sum(
                case
                    when calibrated_score >= 75
                    then 1
                    else 0
                end
            )
            """
        ).alias(
            "critical_or_above"
        ),
    )

    .withColumn(
        "medium_rate_pct",
        spark_round(
            (
                col("medium_or_above")
                / col("total_windows")
            )
            * 100,
            2,
        ),
    )

    .withColumn(
        "high_rate_pct",
        spark_round(
            (
                col("high_or_above")
                / col("total_windows")
            )
            * 100,
            2,
        ),
    )

    .withColumn(
        "critical_rate_pct",
        spark_round(
            (
                col("critical_or_above")
                / col("total_windows")
            )
            * 100,
            2,
        ),
    )

    .orderBy("symbol")
)


threshold_rates.show(
    truncate=False
)


# ============================================================
# EMPIRICAL SCORE THRESHOLDS
#
# p90 = unusual relative to about 90% of historical windows
# p95 = unusual relative to about 95%
# p99 = extreme relative to about 99%
# ============================================================

print()
print("=" * 78)
print(" EMPIRICAL CALIBRATED SEVERITY THRESHOLDS")
print("=" * 78)


recommended_thresholds = (
    calibrated
    .groupBy("symbol")
    .agg(
        expr(
            """
            percentile_approx(
                calibrated_score,
                0.90,
                10000
            )
            """
        ).alias(
            "medium_p90"
        ),

        expr(
            """
            percentile_approx(
                calibrated_score,
                0.95,
                10000
            )
            """
        ).alias(
            "high_p95"
        ),

        expr(
            """
            percentile_approx(
                calibrated_score,
                0.99,
                10000
            )
            """
        ).alias(
            "critical_p99"
        ),
    )
    .orderBy("symbol")
)


recommended_thresholds.show(
    truncate=False
)


# ============================================================
# DOMINANT SIGNAL DISTRIBUTION AFTER CALIBRATION
# ============================================================

print()
print("=" * 78)
print(" CALIBRATED DOMINANT SIGNAL DISTRIBUTION")
print("=" * 78)


(
    calibrated
    .groupBy(
        "symbol",
        "calibrated_dominant_signal",
    )
    .count()
    .orderBy(
        "symbol",
        col("count").desc(),
    )
    .show(
        truncate=False
    )
)


# ============================================================
# HIGHEST-SCORING WINDOWS
#
# Useful for demonstrations and report examples.
# ============================================================

print()
print("=" * 78)
print(" TOP 10 CALIBRATED WINDOWS")
print("=" * 78)


(
    calibrated
    .select(
        "window_start",
        "window_end",
        "symbol",
        "trade_count",
        "price_range_pct",
        "coefficient_of_variation",
        "buy_sell_imbalance",
        "cal_price_component",
        "cal_volatility_component",
        "cal_imbalance_component",
        "cal_activity_component",
        "calibrated_score",
        "calibrated_dominant_signal",
    )
    .orderBy(
        col("calibrated_score").desc()
    )
    .show(
        10,
        truncate=False,
    )
)


# ============================================================
# COMPLETION
# ============================================================

print()
print("=" * 78)
print(
    " CALIBRATED HISTORICAL ANALYSIS COMPLETE"
)
print("=" * 78)
print()

spark.stop()