import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from kafka import KafkaConsumer


# ============================================================
# CONFIGURATION
# ============================================================

BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092",
)

SCORE_TOPIC = os.getenv(
    "SCORE_TOPIC",
    "crypto.market.scores",
)

ANOMALY_TOPIC = os.getenv(
    "ANOMALY_TOPIC",
    "crypto.anomalies",
)

RUNTIME_CONFIG_PATH = os.getenv(
    "RUNTIME_CONFIG_PATH",
    "/app/runtime/demo_mode.json",
)

DEMO_MEDIUM_THRESHOLD = float(
    os.getenv(
        "DEMO_MEDIUM_THRESHOLD",
        "35.0",
    )
)

REFRESH_SECONDS = 3


# ============================================================
# PAGE
# ============================================================

st.set_page_config(
    page_title="Crypto Signal Intelligence",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ============================================================
# CSS
# ============================================================

st.html(
    """
<style>

html,
body,
[data-testid="stAppViewContainer"],
[data-testid="stApp"] {

    background:
        radial-gradient(
            circle at 12% 0%,
            rgba(70,90,255,.13),
            transparent 30%
        ),

        radial-gradient(
            circle at 90% 5%,
            rgba(126,64,255,.10),
            transparent 30%
        ),

        linear-gradient(
            180deg,
            #06080d 0%,
            #090c13 50%,
            #05070b 100%
        );

    color: #f5f7fb;
}


[data-testid="stHeader"] {
    background: transparent;
}


[data-testid="stToolbar"] {
    visibility: hidden;
}


#MainMenu,
footer {
    visibility: hidden;
}


.block-container {

    max-width: 1520px;

    padding-top: 1.1rem;

    padding-left: 2.2rem;

    padding-right: 2.2rem;

    padding-bottom: 3rem;
}


* {
    box-sizing: border-box;
}


/* ============================================================
   GLASS PANELS
============================================================ */

[data-testid="stVerticalBlockBorderWrapper"] {

    border:
        1px solid rgba(255,255,255,.075)
        !important;

    border-radius:
        22px
        !important;

    background:
        linear-gradient(
            145deg,
            rgba(255,255,255,.048),
            rgba(255,255,255,.015)
        )
        !important;

    box-shadow:
        inset 0 1px 0 rgba(255,255,255,.03),
        0 22px 60px rgba(0,0,0,.18);
}


/* ============================================================
   TOPBAR
============================================================ */

.topbar {

    display: flex;

    align-items: center;

    justify-content: space-between;

    border-bottom:
        1px solid rgba(255,255,255,.07);

    padding:
        9px 0 18px 0;

    margin-bottom:
        18px;
}


.brand-title {

    font-size: 14px;

    font-weight: 800;

    letter-spacing: .08em;

    color: #f4f6fb;
}


.brand-sub {

    margin-top: 4px;

    font-size: 9px;

    text-transform: uppercase;

    letter-spacing: .18em;

    color: #667080;
}


.tech-stack {

    display: flex;

    gap: 7px;
}


.tech-pill {

    font-size: 9px;

    letter-spacing: .1em;

    color: #768091;

    padding: 7px 10px;

    border-radius: 9px;

    border:
        1px solid rgba(255,255,255,.07);

    background:
        rgba(255,255,255,.025);
}


/* ============================================================
   HERO
============================================================ */

.hero {

    padding:
        20px 0 27px 0;
}


.hero-kicker {

    color: #7887ff;

    font-size: 10px;

    font-weight: 800;

    text-transform: uppercase;

    letter-spacing: .2em;

    margin-bottom: 13px;
}


.hero-title {

    font-size:
        clamp(
            42px,
            5.1vw,
            76px
        );

    line-height: .96;

    max-width: 950px;

    font-weight: 800;

    letter-spacing: -.055em;

    background:
        linear-gradient(
            100deg,
            #ffffff,
            #d9ddea 52%,
            #747e92
        );

    -webkit-background-clip: text;

    -webkit-text-fill-color: transparent;
}


.hero-copy {

    color: #8992a2;

    max-width: 770px;

    line-height: 1.7;

    font-size: 13px;

    margin-top: 19px;
}


/* ============================================================
   STATUS
============================================================ */

.status-row {

    display: flex;

    align-items: center;

    justify-content: flex-end;

    gap: 8px;

    flex-wrap: wrap;

    margin-bottom: 12px;
}


.live-pill,
.offline-pill,
.demo-pill,
.production-pill,
.pending-pill {

    display: inline-flex;

    align-items: center;

    gap: 8px;

    padding: 8px 12px;

    border-radius: 999px;

    font-size: 9px;

    font-weight: 800;

    letter-spacing: .12em;

    white-space: nowrap;
}


.live-pill {

    color: #72efaf;

    background:
        rgba(50,230,143,.07);

    border:
        1px solid rgba(50,230,143,.18);
}


.offline-pill {

    color: #ffc27c;

    background:
        rgba(255,171,75,.07);

    border:
        1px solid rgba(255,171,75,.18);
}


.demo-pill {

    color: #ffd06a;

    background:
        rgba(255,198,67,.075);

    border:
        1px solid rgba(255,198,67,.20);
}


.production-pill {

    color: #93a0ff;

    background:
        rgba(110,125,255,.075);

    border:
        1px solid rgba(110,125,255,.18);
}


.pending-pill {

    color: #ceb7ff;

    background:
        rgba(170,120,255,.075);

    border:
        1px solid rgba(170,120,255,.18);
}


.live-dot {

    width: 7px;

    height: 7px;

    background: #4be79b;

    border-radius: 50%;

    box-shadow:
        0 0 12px rgba(75,231,155,.65);
}


/* ============================================================
   MODE CONTROL
============================================================ */

.mode-title {

    font-size: 10px;

    text-transform: uppercase;

    letter-spacing: .16em;

    color: #757f90;

    margin-bottom: 5px;
}


.mode-heading {

    font-size: 18px;

    font-weight: 760;

    letter-spacing: -.02em;

    color: #f1f3f8;
}


.mode-copy {

    margin-top: 6px;

    color: #747e8e;

    font-size: 10px;

    line-height: 1.55;
}


.demo-banner {

    margin-top: 4px;

    margin-bottom: 22px;

    padding: 13px 16px;

    border-radius: 14px;

    background:
        linear-gradient(
            90deg,
            rgba(255,190,61,.075),
            rgba(255,145,59,.025)
        );

    border:
        1px solid rgba(255,194,70,.16);

    color: #c6a961;

    font-size: 10px;

    line-height: 1.6;
}


.demo-banner strong {

    color: #f4cf70;
}


/* ============================================================
   SECTIONS
============================================================ */

.section-heading {

    margin-top: 26px;

    font-size: 20px;

    font-weight: 750;

    letter-spacing: -.025em;

    color: #f2f4f8;
}


.section-copy {

    margin-top: 4px;

    margin-bottom: 17px;

    font-size: 10px;

    color: #667080;
}


/* ============================================================
   COIN CARDS
============================================================ */

.coin-row {

    display: flex;

    align-items: flex-start;

    justify-content: space-between;
}


.coin-code {

    color: #747e8e;

    font-size: 9px;

    text-transform: uppercase;

    letter-spacing: .16em;
}


.coin-name {

    font-size: 19px;

    font-weight: 750;

    margin-top: 5px;
}


.coin-price {

    font-size: 38px;

    font-weight: 780;

    letter-spacing: -.045em;

    margin-top: 9px;
}


.coin-caption {

    color: #606a7b;

    font-size: 9px;

    text-transform: uppercase;

    letter-spacing: .09em;

    margin-top: 3px;
}


/* ============================================================
   SEVERITY
============================================================ */

.severity {

    border-radius: 999px;

    padding: 7px 10px;

    font-size: 9px;

    font-weight: 850;

    letter-spacing: .1em;
}


.normal {

    color: #6eedad;

    background:
        rgba(50,230,143,.07);

    border:
        1px solid rgba(50,230,143,.18);
}


.medium {

    color: #ffd16c;

    background:
        rgba(255,204,77,.07);

    border:
        1px solid rgba(255,204,77,.18);
}


.high {

    color: #ff9e69;

    background:
        rgba(255,136,76,.08);

    border:
        1px solid rgba(255,136,76,.20);
}


.critical {

    color: #ff7188;

    background:
        rgba(255,80,110,.09);

    border:
        1px solid rgba(255,80,110,.22);
}


/* ============================================================
   METRICS
============================================================ */

[data-testid="stMetric"] {

    background:
        rgba(255,255,255,.02);

    border:
        1px solid rgba(255,255,255,.05);

    padding: 14px 16px;

    border-radius: 14px;
}


[data-testid="stMetricLabel"] {
    color: #6f7888;
}


[data-testid="stMetricValue"] {
    color: #f4f6fb;
}


/* ============================================================
   SIGNAL BARS
============================================================ */

.signal {

    margin-bottom: 17px;
}


.signal-head {

    display: flex;

    justify-content: space-between;

    margin-bottom: 7px;

    font-size: 10px;
}


.signal-name {
    color: #929aaa;
}


.signal-value {

    color: #eef1f7;

    font-weight: 700;
}


.signal-track {

    height: 5px;

    background:
        rgba(255,255,255,.055);

    border-radius: 999px;

    overflow: hidden;
}


.signal-fill {

    height: 100%;

    border-radius: 999px;

    background:
        linear-gradient(
            90deg,
            #6676ff,
            #8f6dff
        );

    box-shadow:
        0 0 17px rgba(105,116,255,.28);
}


/* ============================================================
   FLOW
============================================================ */

.flow-panel {

    padding: 18px;

    border-radius: 17px;

    border:
        1px solid rgba(255,255,255,.06);
}


.flow-buy {

    background:
        linear-gradient(
            135deg,
            rgba(48,225,142,.085),
            rgba(48,225,142,.015)
        );
}


.flow-sell {

    background:
        linear-gradient(
            135deg,
            rgba(255,83,111,.085),
            rgba(255,83,111,.015)
        );
}


.flow-balanced {

    background:
        rgba(255,255,255,.025);
}


.flow-caption {

    color: #697384;

    font-size: 9px;

    text-transform: uppercase;

    letter-spacing: .12em;
}


.flow-title {

    margin-top: 8px;

    font-size: 21px;

    font-weight: 750;
}


.flow-description {

    color: #818a9a;

    margin-top: 7px;

    font-size: 10px;

    line-height: 1.6;
}


/* ============================================================
   MODEL
============================================================ */

.model-title {

    color: #6d7788;

    font-size: 9px;

    text-transform: uppercase;

    letter-spacing: .12em;
}


.model-value {

    margin-top: 8px;

    font-size: 18px;

    font-weight: 740;

    color: #f1f3f8;
}


.model-copy {

    margin-top: 8px;

    color: #747e8e;

    font-size: 10px;

    line-height: 1.6;
}


/* ============================================================
   STREAMLIT
============================================================ */

[data-testid="stDataFrame"] {

    border-radius: 18px;

    overflow: hidden;

    border:
        1px solid rgba(255,255,255,.07);
}


[data-baseweb="select"] > div {

    background:
        rgba(255,255,255,.035);

    border-color:
        rgba(255,255,255,.08);
}


/* Toggle */

[data-testid="stToggle"] {

    padding-top: 2px;
}


</style>
"""
)


# ============================================================
# RUNTIME MODE FILE
# ============================================================

def read_requested_demo_mode():
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


def write_requested_demo_mode(
    enabled,
):

    directory = os.path.dirname(
        RUNTIME_CONFIG_PATH
    )

    os.makedirs(
        directory,
        exist_ok=True,
    )

    temporary_path = (
        RUNTIME_CONFIG_PATH
        + ".tmp"
    )

    payload = {
        "demo_mode":
            bool(
                enabled
            )
    }

    with open(
        temporary_path,
        "w",
        encoding="utf-8",
    ) as config_file:

        json.dump(
            payload,
            config_file,
        )

    os.replace(
        temporary_path,
        RUNTIME_CONFIG_PATH,
    )


# ============================================================
# KAFKA SHARED STATE
# ============================================================

@st.cache_resource
def get_state():

    return {

        "scores":
            deque(
                maxlen=3000
            ),

        "anomalies":
            deque(
                maxlen=1200
            ),

        "lock":
            threading.Lock(),

        "started":
            False,

        "score_error":
            None,

        "anomaly_error":
            None,

        "last_score_received":
            None,

        "last_anomaly_received":
            None,
    }


state = get_state()


# ============================================================
# KAFKA CONSUMER
# ============================================================

def consume_topic(
    topic,
    group_id,
    destination,
    time_key,
    error_key,
):

    while True:

        consumer = None

        try:

            consumer = KafkaConsumer(
                topic,

                bootstrap_servers=
                    BOOTSTRAP,

                group_id=
                    group_id,

                auto_offset_reset=
                    "latest",

                enable_auto_commit=
                    True,

                value_deserializer=
                    lambda value:
                    json.loads(
                        value.decode(
                            "utf-8"
                        )
                    ),

                request_timeout_ms=
                    30000,

                session_timeout_ms=
                    10000,
            )

            with state[
                "lock"
            ]:

                state[
                    error_key
                ] = None


            while True:

                messages = consumer.poll(
                    timeout_ms=1000,
                    max_records=200,
                )


                for _, batch in messages.items():

                    for message in batch:

                        payload = (
                            message.value
                        )

                        if not isinstance(
                            payload,
                            dict,
                        ):
                            continue


                        payload[
                            "_received_at"
                        ] = (
                            datetime.now(
                                timezone.utc
                            ).isoformat()
                        )


                        with state[
                            "lock"
                        ]:

                            state[
                                destination
                            ].append(
                                payload
                            )

                            state[
                                time_key
                            ] = (
                                time.time()
                            )


        except Exception as exc:

            with state[
                "lock"
            ]:

                state[
                    error_key
                ] = str(
                    exc
                )

            time.sleep(
                3
            )


        finally:

            if consumer is not None:

                try:
                    consumer.close()

                except Exception:
                    pass


# ============================================================
# START BACKGROUND CONSUMERS
# ============================================================

if not state[
    "started"
]:

    with state[
        "lock"
    ]:

        if not state[
            "started"
        ]:

            threading.Thread(
                target=
                    consume_topic,

                args=(
                    SCORE_TOPIC,
                    "premium-dashboard-scores-live-toggle-v1",
                    "scores",
                    "last_score_received",
                    "score_error",
                ),

                daemon=True,
            ).start()


            threading.Thread(
                target=
                    consume_topic,

                args=(
                    ANOMALY_TOPIC,
                    "premium-dashboard-anomalies-live-toggle-v1",
                    "anomalies",
                    "last_anomaly_received",
                    "anomaly_error",
                ),

                daemon=True,
            ).start()


            state[
                "started"
            ] = True


# ============================================================
# HELPERS
# ============================================================

def safe_float(
    value,
    default=0.0,
):

    try:
        return float(
            value
        )

    except (
        TypeError,
        ValueError,
    ):
        return default


def safe_int(
    value,
    default=0,
):

    try:
        return int(
            value
        )

    except (
        TypeError,
        ValueError,
    ):
        return default


def latest(
    records,
    symbol,
):

    matches = [
        row

        for row in records

        if row.get(
            "symbol"
        )
        == symbol
    ]

    if matches:
        return matches[-1]

    return None


def severity_css(
    value,
):

    value = str(
        value
        or "NORMAL"
    ).lower()

    if value not in {
        "normal",
        "medium",
        "high",
        "critical",
    }:
        return "normal"

    return value


def direction_css(
    value,
):

    value = str(
        value
        or "BALANCED"
    ).upper()

    if value == "BUY_SIDE":
        return "flow-buy"

    if value == "SELL_SIDE":
        return "flow-sell"

    return "flow-balanced"


def direction_title(
    value,
):

    value = str(
        value
        or "BALANCED"
    ).upper()

    if value == "BUY_SIDE":
        return "Buy-side pressure"

    if value == "SELL_SIDE":
        return "Sell-side pressure"

    return "Balanced flow"


def nice_time(
    value,
):

    try:

        timestamp = pd.to_datetime(
            value,
            utc=True,
        )

        return timestamp.strftime(
            "%H:%M:%S"
        )

    except Exception:
        return "—"


def driver_copy(
    signal,
):

    mapping = {

        "PRICE_RANGE":
            (
                "Short-term price expansion currently "
                "contributes the strongest normalized signal."
            ),

        "VOLATILITY":
            (
                "Price dispersion around the rolling mean "
                "currently contributes the strongest signal."
            ),

        "BUY_SELL_IMBALANCE":
            (
                "Buy/sell order-flow imbalance currently "
                "contributes the strongest normalized signal."
            ),

        "TRADE_ACTIVITY":
            (
                "Trade frequency relative to its historical "
                "baseline currently contributes the strongest signal."
            ),
    }

    return mapping.get(
        signal,
        "Waiting for dominant-signal information.",
    )


# ============================================================
# STATE SNAPSHOT
# ============================================================

with state[
    "lock"
]:

    scores = list(
        state[
            "scores"
        ]
    )

    anomalies = list(
        state[
            "anomalies"
        ]
    )

    last_score = state[
        "last_score_received"
    ]

    score_error = state[
        "score_error"
    ]


pipeline_live = (
    last_score
    is not None

    and

    (
        time.time()
        - last_score
    )
    < 90
)


latest_record = (
    scores[-1]
    if scores
    else None
)


# ============================================================
# REQUESTED MODE
# ============================================================

requested_demo_mode = (
    read_requested_demo_mode()
)


if (
    "demo_mode_control"
    not in st.session_state
):

    st.session_state[
        "demo_mode_control"
    ] = requested_demo_mode


# ============================================================
# AUTHORITATIVE SPARK MODE
# ============================================================

spark_demo_mode = (

    bool(
        latest_record.get(
            "demo_mode",
            False,
        )
    )

    if latest_record
    else False
)


mode_change_pending = (
    latest_record is not None

    and

    requested_demo_mode
    != spark_demo_mode
)


# ============================================================
# HEADER — NO LOGO
# ============================================================

st.html(
    """
<div class="topbar">

    <div>

        <div class="brand-title">
            SIGNAL INTELLIGENCE
        </div>

        <div class="brand-sub">
            Streaming Market Analytics
        </div>

    </div>


    <div class="tech-stack">

        <div class="tech-pill">
            KAFKA
        </div>

        <div class="tech-pill">
            SPARK
        </div>

        <div class="tech-pill">
            HDFS
        </div>

    </div>

</div>
"""
)


# ============================================================
# HERO
# ============================================================

hero_left, hero_right = st.columns(
    [
        3.7,
        1.3,
    ],
    vertical_alignment=
        "bottom",
)


with hero_left:

    st.html(
        """
<div class="hero">

    <div class="hero-kicker">
        Real-Time Market Behaviour Engine
    </div>

    <div class="hero-title">
        Detect the signal<br>
        inside the noise.
    </div>

    <div class="hero-copy">

        Live cryptocurrency trades are transformed
        into statistically calibrated short-term
        market-behaviour signals using Kafka,
        Spark Structured Streaming and historical
        HDFS baselines.

    </div>

</div>
"""
    )


with hero_right:

    status_html = ""

    if pipeline_live:

        status_html += """
<div class="live-pill">

    <span class="live-dot">
    </span>

    LIVE DATA FLOW

</div>
"""

    else:

        status_html += """
<div class="offline-pill">
    AWAITING STREAM
</div>
"""


    if latest_record:

        if mode_change_pending:

            status_html += """
<div class="pending-pill">
    MODE SWITCH PENDING
</div>
"""

        elif spark_demo_mode:

            status_html += """
<div class="demo-pill">
    DEMO MODE
</div>
"""

        else:

            status_html += """
<div class="production-pill">
    PRODUCTION MODE
</div>
"""


    st.html(
        f"""
<div class="status-row">
    {status_html}
</div>
"""
    )


# ============================================================
# MODE CONTROL
# ============================================================

with hero_right:

    with st.container(
        border=True
    ):

        st.html(
            """
<div class="mode-title">
    Detector Control
</div>

<div class="mode-heading">
    Demo Mode
</div>

<div class="mode-copy">

    Switch the live MEDIUM event threshold between
    empirical production P90 and the demonstration
    threshold.

</div>
"""
        )


        user_toggle = st.toggle(
            "Enable demonstration threshold",
            key=
                "demo_mode_control",
        )


        current_file_mode = (
            read_requested_demo_mode()
        )


        if (
            user_toggle
            != current_file_mode
        ):

            write_requested_demo_mode(
                user_toggle
            )

            requested_demo_mode = (
                user_toggle
            )

            mode_change_pending = True


        if user_toggle:

            st.caption(
                f"Requested threshold: "
                f"MEDIUM = "
                f"{DEMO_MEDIUM_THRESHOLD:.0f}"
            )

        else:

            st.caption(
                "Requested threshold: "
                "Production P90"
            )


# ============================================================
# ERRORS
# ============================================================

if score_error:

    st.error(
        "Kafka consumer error: "
        + str(
            score_error
        )
    )


# ============================================================
# WAITING FOR FIRST FRESH SCORE
# ============================================================

if not scores:

    st.info(
        "Waiting for fresh calibrated "
        "Spark windows..."
    )

    time.sleep(
        REFRESH_SECONDS
    )

    st.rerun()


# ============================================================
# AUTHORITATIVE MODE BANNER
# ============================================================

if mode_change_pending:

    target_mode = (
        "DEMO"
        if requested_demo_mode
        else "PRODUCTION"
    )

    st.info(
        f"Switch requested → {target_mode} MODE. "
        "Spark will apply it to the next completed "
        "market window."
    )


elif spark_demo_mode:

    effective_demo_threshold = (
        safe_float(
            latest_record.get(
                "medium_threshold",
                DEMO_MEDIUM_THRESHOLD,
            )
        )
    )

    st.html(
        f"""
<div class="demo-banner">

    <strong>
        DEMONSTRATION MODE ACTIVE
    </strong>

    &nbsp;—&nbsp;

    Effective MEDIUM threshold:

    <strong>
        {effective_demo_threshold:.2f}
    </strong>

    &nbsp;•&nbsp;

    The underlying anomaly score is unchanged.

    &nbsp;•&nbsp;

    BTC production P90 = 67.20

    &nbsp;•&nbsp;

    ETH production P90 = 72.45

</div>
"""
    )


# ============================================================
# LATEST BTC / ETH
# ============================================================

btc = latest(
    scores,
    "BTCUSDT",
)

eth = latest(
    scores,
    "ETHUSDT",
)


def render_coin(
    payload,
    symbol,
    name,
):

    with st.container(
        border=True
    ):

        if payload is None:

            st.write(
                f"Waiting for {name}..."
            )

            return


        score = safe_float(
            payload.get(
                "anomaly_score"
            )
        )


        severity = str(
            payload.get(
                "severity",
                "NORMAL",
            )
        ).upper()


        price = safe_float(
            payload.get(
                "mean_price"
            )
        )


        trades = safe_int(
            payload.get(
                "trade_count"
            )
        )


        price_range = safe_float(
            payload.get(
                "price_range_pct"
            )
        )


        imbalance = safe_float(
            payload.get(
                "buy_sell_imbalance"
            )
        )


        medium = safe_float(
            payload.get(
                "medium_threshold"
            )
        )


        production_medium = safe_float(
            payload.get(
                "production_medium_threshold"
            )
        )


        driver = str(
            payload.get(
                "dominant_signal",
                "—",
            )
        ).replace(
            "_",
            " ",
        )


        st.html(
            f"""
<div class="coin-row">

    <div>

        <div class="coin-code">
            {symbol}
        </div>

        <div class="coin-name">
            {name}
        </div>

        <div class="coin-price">
            ${price:,.2f}
        </div>

        <div class="coin-caption">
            30-second rolling mean
        </div>

    </div>


    <div class="
        severity
        {severity_css(severity)}
    ">
        {severity}
    </div>

</div>
"""
        )


        st.metric(
            "Calibrated anomaly score",
            f"{score:.2f} / 100",
        )


        metric_left, metric_right = (
            st.columns(
                2
            )
        )


        with metric_left:

            st.metric(
                "30s Trades",
                f"{trades:,}",
            )

            st.metric(
                "Price Range",
                f"{price_range:.4f}%",
            )


        with metric_right:

            st.metric(
                "Trades / sec",
                f"{trades / 30:.1f}",
            )

            st.metric(
                "Imbalance",
                f"{imbalance:.3f}",
            )


        if payload.get(
            "demo_mode",
            False,
        ):

            st.caption(
                f"Demo MEDIUM: "
                f"{medium:.2f}"
                f"  •  "
                f"Production P90: "
                f"{production_medium:.2f}"
                f"  •  "
                f"Driver: {driver}"
            )

        else:

            st.caption(
                f"Production MEDIUM: "
                f"{production_medium:.2f}"
                f"  •  "
                f"Driver: {driver}"
            )


coin_columns = st.columns(
    2,
    gap="large",
)


with coin_columns[0]:

    render_coin(
        btc,
        "BTCUSDT",
        "Bitcoin",
    )


with coin_columns[1]:

    render_coin(
        eth,
        "ETHUSDT",
        "Ethereum",
    )


# ============================================================
# SIGNAL INTELLIGENCE
# ============================================================

st.html(
    """
<div class="section-heading">
    Signal Intelligence
</div>

<div class="section-copy">
    Inspect the live calibrated behaviour
    of one cryptocurrency market.
</div>
"""
)


symbol = st.selectbox(
    "Market",
    [
        "BTCUSDT",
        "ETHUSDT",
    ],
    label_visibility=
        "collapsed",
)


selected = latest(
    scores,
    symbol,
)


if selected:

    score = safe_float(
        selected.get(
            "anomaly_score"
        )
    )


    medium = safe_float(
        selected.get(
            "medium_threshold"
        )
    )


    production_medium = safe_float(
        selected.get(
            "production_medium_threshold"
        )
    )


    high = safe_float(
        selected.get(
            "high_threshold"
        )
    )


    critical = safe_float(
        selected.get(
            "critical_threshold"
        )
    )


    price_component = safe_float(
        selected.get(
            "price_component"
        )
    )


    volatility_component = safe_float(
        selected.get(
            "volatility_component"
        )
    )


    imbalance_component = safe_float(
        selected.get(
            "imbalance_component"
        )
    )


    activity_component = safe_float(
        selected.get(
            "activity_component"
        )
    )


    direction = selected.get(
        "order_flow_direction",
        "BALANCED",
    )


    pressure = safe_float(
        selected.get(
            "net_buy_pressure"
        )
    )


    driver = selected.get(
        "dominant_signal",
        "—",
    )


    # ========================================================
    # SCORE TIMELINE
    # ========================================================

    chart_column, signal_column = (
        st.columns(
            [
                2.1,
                1,
            ],
            gap="large",
        )
    )


    with chart_column:

        with st.container(
            border=True
        ):

            st.subheader(
                "Market Behaviour Score"
            )

            st.caption(
                "Live calibrated score and "
                "current severity thresholds"
            )


            market_rows = [
                row

                for row in scores

                if row.get(
                    "symbol"
                )
                == symbol
            ][-120:]


            score_df = pd.DataFrame(
                market_rows
            )


            score_figure = (
                go.Figure()
            )


            if not score_df.empty:

                score_df[
                    "window_end"
                ] = pd.to_datetime(
                    score_df[
                        "window_end"
                    ],
                    utc=True,
                    errors="coerce",
                )


                score_df[
                    "anomaly_score"
                ] = pd.to_numeric(
                    score_df[
                        "anomaly_score"
                    ],
                    errors="coerce",
                )


                score_df = (
                    score_df

                    .dropna(
                        subset=[
                            "window_end",
                            "anomaly_score",
                        ]
                    )

                    .sort_values(
                        "window_end"
                    )
                )


                score_figure.add_trace(
                    go.Scatter(
                        x=
                            score_df[
                                "window_end"
                            ],

                        y=
                            score_df[
                                "anomaly_score"
                            ],

                        mode=
                            "lines",

                        line=
                            dict(
                                width=2.3,
                                color="#7484ff",
                            ),

                        fill=
                            "tozeroy",

                        fillcolor=
                            "rgba(110,125,255,.08)",

                        hovertemplate=
                            "%{x}<br>"
                            "Score %{y:.2f}"
                            "<extra></extra>",
                    )
                )


            selected_demo = bool(
                selected.get(
                    "demo_mode",
                    False,
                )
            )


            score_figure.add_hline(
                y=
                    medium,

                line_dash=
                    "dot",

                line_color=
                    "rgba(255,207,102,.72)",

                annotation_text=
                    (
                        f"DEMO MEDIUM {medium:.2f}"

                        if selected_demo

                        else

                        f"MEDIUM {medium:.2f}"
                    ),

                annotation_position=
                    "top left",
            )


            if selected_demo:

                score_figure.add_hline(
                    y=
                        production_medium,

                    line_dash=
                        "dash",

                    line_color=
                        "rgba(134,146,255,.45)",

                    annotation_text=
                        (
                            "PRODUCTION P90 "
                            f"{production_medium:.2f}"
                        ),

                    annotation_position=
                        "bottom left",
                )


            score_figure.add_hline(
                y=
                    high,

                line_dash=
                    "dot",

                line_color=
                    "rgba(255,145,86,.68)",

                annotation_text=
                    f"HIGH {high:.2f}",

                annotation_position=
                    "top left",
            )


            score_figure.add_hline(
                y=
                    critical,

                line_dash=
                    "dot",

                line_color=
                    "rgba(255,84,112,.72)",

                annotation_text=
                    f"CRITICAL {critical:.2f}",

                annotation_position=
                    "top left",
            )


            score_figure.update_layout(
                height=
                    390,

                margin=
                    dict(
                        l=10,
                        r=10,
                        t=20,
                        b=10,
                    ),

                paper_bgcolor=
                    "rgba(0,0,0,0)",

                plot_bgcolor=
                    "rgba(0,0,0,0)",

                showlegend=
                    False,

                font=
                    dict(
                        color="#7d8797",
                        size=10,
                    ),

                xaxis=
                    dict(
                        showgrid=False,
                        zeroline=False,
                    ),

                yaxis=
                    dict(
                        range=[
                            0,
                            100,
                        ],

                        gridcolor=
                            "rgba(255,255,255,.05)",

                        zeroline=False,
                    ),
            )


            st.plotly_chart(
                score_figure,
                width="stretch",
                config={
                    "displayModeBar":
                        False
                },
            )


    # ========================================================
    # EXPLAINABLE SCORE
    # ========================================================

    with signal_column:

        with st.container(
            border=True
        ):

            st.subheader(
                "Explainable Score"
            )

            st.caption(
                "Signal intensity relative "
                "to historical P95"
            )


            components = [
                (
                    "Price Range",
                    price_component,
                ),

                (
                    "Volatility",
                    volatility_component,
                ),

                (
                    "Buy / Sell Imbalance",
                    imbalance_component,
                ),

                (
                    "Trade Activity",
                    activity_component,
                ),
            ]


            for (
                label,
                component_value,
            ) in components:

                percentage = min(
                    max(
                        component_value
                        * 100,
                        0,
                    ),
                    100,
                )


                st.html(
                    f"""
<div class="signal">

    <div class="signal-head">

        <span class="signal-name">
            {label}
        </span>

        <span class="signal-value">
            {percentage:.1f}%
        </span>

    </div>


    <div class="signal-track">

        <div
            class="signal-fill"

            style="
                width:
                {percentage}%
            "
        >
        </div>

    </div>

</div>
"""
                )


            st.caption(
                "Strongest driver: "
                + str(
                    driver
                ).replace(
                    "_",
                    " ",
                )
            )


            st.write(
                driver_copy(
                    driver
                )
            )


    # ========================================================
    # WEIGHTED CONTRIBUTION + ORDER FLOW
    # ========================================================

    contribution_column, flow_column = (
        st.columns(
            [
                1.4,
                1,
            ],
            gap="large",
        )
    )


    with contribution_column:

        with st.container(
            border=True
        ):

            st.subheader(
                "Weighted Contribution"
            )


            contributions = (
                pd.DataFrame(
                    {
                        "Signal": [
                            "Price Range",
                            "Volatility",
                            "Buy/Sell Imbalance",
                            "Trade Activity",
                        ],

                        "Points": [
                            price_component
                            * 35,

                            volatility_component
                            * 30,

                            imbalance_component
                            * 20,

                            activity_component
                            * 15,
                        ],
                    }
                )
            )


            contribution_figure = (
                go.Figure(
                    go.Bar(
                        x=
                            contributions[
                                "Points"
                            ],

                        y=
                            contributions[
                                "Signal"
                            ],

                        orientation=
                            "h",

                        marker=
                            dict(
                                color=
                                    "#7585ff"
                            ),
                    )
                )
            )


            contribution_figure.update_layout(
                height=
                    290,

                margin=
                    dict(
                        l=10,
                        r=15,
                        t=10,
                        b=10,
                    ),

                paper_bgcolor=
                    "rgba(0,0,0,0)",

                plot_bgcolor=
                    "rgba(0,0,0,0)",

                showlegend=
                    False,

                font=
                    dict(
                        color="#8993a3",
                        size=10,
                    ),

                xaxis=
                    dict(
                        gridcolor=
                            "rgba(255,255,255,.045)",

                        zeroline=False,
                    ),

                yaxis=
                    dict(
                        autorange=
                            "reversed",

                        showgrid=False,
                    ),
            )


            st.plotly_chart(
                contribution_figure,
                width="stretch",
                config={
                    "displayModeBar":
                        False
                },
            )


            calculated_score = (
                price_component
                * 35

                +

                volatility_component
                * 30

                +

                imbalance_component
                * 20

                +

                activity_component
                * 15
            )


            calc_column, spark_column = (
                st.columns(
                    2
                )
            )


            calc_column.metric(
                "Calculated",
                f"{calculated_score:.2f}",
            )


            spark_column.metric(
                "Spark Score",
                f"{score:.2f}",
            )


    with flow_column:

        with st.container(
            border=True
        ):

            st.subheader(
                "Order-Flow Pressure"
            )


            magnitude = (
                abs(
                    pressure
                )
                * 100
            )


            if direction == "BUY_SIDE":

                flow_description = (
                    "Taker activity is currently "
                    "tilted toward buyers by "
                    f"{magnitude:.1f}%."
                )

            elif direction == "SELL_SIDE":

                flow_description = (
                    "Taker activity is currently "
                    "tilted toward sellers by "
                    f"{magnitude:.1f}%."
                )

            else:

                flow_description = (
                    "Current taker activity "
                    "is approximately balanced."
                )


            st.html(
                f"""
<div class="
    flow-panel
    {direction_css(direction)}
">

    <div class="flow-caption">
        Current direction
    </div>

    <div class="flow-title">
        {direction_title(direction)}
    </div>

    <div class="flow-description">
        {flow_description}
    </div>

</div>
"""
            )


            st.write(
                ""
            )


            pressure_column, imbalance_column = (
                st.columns(
                    2
                )
            )


            pressure_column.metric(
                "Net Pressure",
                f"{pressure:+.3f}",
            )


            imbalance_column.metric(
                "Absolute Imbalance",
                (
                    f"{safe_float(
                        selected.get(
                            'buy_sell_imbalance'
                        )
                    ):.3f}"
                ),
            )


            buy_column, sell_column = (
                st.columns(
                    2
                )
            )


            buy_column.metric(
                "Buy Trades",
                (
                    f"{safe_int(
                        selected.get(
                            'buy_trades'
                        )
                    ):,}"
                ),
            )


            sell_column.metric(
                "Sell Trades",
                (
                    f"{safe_int(
                        selected.get(
                            'sell_trades'
                        )
                    ):,}"
                ),
            )


# ============================================================
# RECENT WINDOWS
# ============================================================

st.html(
    """
<div class="section-heading">
    Live Market Windows
</div>

<div class="section-copy">
    Most recent completed 30-second Spark windows.
</div>
"""
)


recent_rows = []


for row in reversed(
    scores[-40:]
):

    recent_rows.append(
        {
            "Time":
                nice_time(
                    row.get(
                        "window_end"
                    )
                ),

            "Market":
                row.get(
                    "symbol",
                    "—",
                ),

            "Score":
                round(
                    safe_float(
                        row.get(
                            "anomaly_score"
                        )
                    ),
                    2,
                ),

            "Severity":
                row.get(
                    "severity",
                    "NORMAL",
                ),

            "Mode":
                row.get(
                    "threshold_mode",
                    "PRODUCTION",
                ),

            "Direction":
                row.get(
                    "order_flow_direction",
                    "—",
                ),

            "Trades":
                safe_int(
                    row.get(
                        "trade_count"
                    )
                ),

            "Driver":
                str(
                    row.get(
                        "dominant_signal",
                        "—",
                    )
                ).replace(
                    "_",
                    " ",
                ),
        }
    )


st.dataframe(
    pd.DataFrame(
        recent_rows
    ),
    width="stretch",
    hide_index=True,
    height=320,
)


# ============================================================
# THRESHOLD-CROSSING EVENTS
# ============================================================

st.html(
    """
<div class="section-heading">
    Threshold-Crossing Events
</div>

<div class="section-copy">

    A completed market window appears here
    after crossing its effective MEDIUM threshold.

</div>
"""
)


if anomalies:

    anomaly_rows = []


    for row in reversed(
        anomalies[-40:]
    ):

        anomaly_rows.append(
            {
                "Time":
                    nice_time(
                        row.get(
                            "window_end"
                        )
                    ),

                "Market":
                    row.get(
                        "symbol",
                        "—",
                    ),

                "Score":
                    round(
                        safe_float(
                            row.get(
                                "anomaly_score"
                            )
                        ),
                        2,
                    ),

                "Severity":
                    row.get(
                        "severity",
                        "—",
                    ),

                "Mode":
                    row.get(
                        "threshold_mode",
                        "—",
                    ),

                "Effective Threshold":
                    round(
                        safe_float(
                            row.get(
                                "medium_threshold"
                            )
                        ),
                        2,
                    ),

                "Production P90":
                    round(
                        safe_float(
                            row.get(
                                "production_medium_threshold"
                            )
                        ),
                        2,
                    ),

                "Direction":
                    row.get(
                        "order_flow_direction",
                        "—",
                    ),

                "Driver":
                    str(
                        row.get(
                            "dominant_signal",
                            "—",
                        )
                    ).replace(
                        "_",
                        " ",
                    ),
            }
        )


    st.dataframe(
        pd.DataFrame(
            anomaly_rows
        ),
        width="stretch",
        hide_index=True,
        height=300,
    )


else:

    st.info(
        "No threshold-crossing event "
        "has been received yet."
    )


# ============================================================
# MODEL TRANSPARENCY
# ============================================================

st.html(
    """
<div class="section-heading">
    Model Transparency
</div>

<div class="section-copy">

    Demo Mode changes only the event threshold.
    It does not modify the calibrated feature
    normalization or the anomaly score itself.

</div>
"""
)


model_one, model_two, model_three = (
    st.columns(
        3,
        gap="large",
    )
)


with model_one:

    with st.container(
        border=True
    ):

        st.html(
            """
<div class="model-title">
    Feature Calibration
</div>

<div class="model-value">
    Historical P95
</div>

<div class="model-copy">

    Price range, volatility, buy/sell
    imbalance and activity are normalized
    against cryptocurrency-specific
    historical P95 baselines stored in HDFS.

</div>
"""
        )


with model_two:

    with st.container(
        border=True
    ):

        st.html(
            """
<div class="model-title">
    Production Severity
</div>

<div class="model-value">
    P90 · P95 · P99
</div>

<div class="model-copy">

    Production MEDIUM, HIGH and CRITICAL
    levels remain based on empirical
    historical score percentiles.

</div>
"""
        )


with model_three:

    with st.container(
        border=True
    ):

        st.html(
            f"""
<div class="model-title">
    Demonstration Control
</div>

<div class="model-value">
    MEDIUM = {DEMO_MEDIUM_THRESHOLD:.0f}
</div>

<div class="model-copy">

    The dashboard may temporarily lower
    only the effective MEDIUM event
    threshold. HIGH and CRITICAL retain
    their calibrated production values.

</div>
"""
        )


# ============================================================
# FOOTER
# ============================================================

st.caption(
    "REAL-TIME CRYPTOCURRENCY MARKET ANOMALY DETECTION"
    "  •  "
    "KAFKA"
    "  •  "
    "SPARK STRUCTURED STREAMING"
    "  •  "
    "HDFS"
)


# ============================================================
# AUTO REFRESH
# ============================================================

time.sleep(
    REFRESH_SECONDS
)

st.rerun()