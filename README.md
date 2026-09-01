# \# Real-Time Cryptocurrency Market Anomaly Detection Using Multi-Signal Streaming Analytics

# 

# \## An End-to-End Kafka, Spark Structured Streaming, and HDFS Pipeline

# 

# This project implements a real-time cryptocurrency market monitoring system using Apache Kafka, Apache Spark Structured Streaming, Apache Hadoop HDFS, and Streamlit.

# 

# The system consumes live BTCUSDT and ETHUSDT aggregate trade data from Binance, processes the trades in real time, computes interpretable multi-signal anomaly scores, stores historical results in HDFS, and presents the results through a live analytics dashboard.

# 

# \---

# 

# \## Project Objective

# 

# The objective is to detect statistically unusual short-term cryptocurrency market behaviour using multiple live market signals.

# 

# The project does not attempt to identify fraud or market manipulation and does not predict future cryptocurrency prices.

# 

# Instead, it identifies unusual market windows based on:

# 

# \- Price movement

# \- Price volatility

# \- Trading activity

# \- Buy/sell imbalance

# \- Order-flow direction

# 

# \---

# 

# \## Architecture

# 

# ```text

# Binance Public WebSocket

# &#x20;       |

# &#x20;       v

# Python Producer

# &#x20;       |

# &#x20;       v

# Apache Kafka

# crypto.raw.trades

# &#x20;       |

# &#x20;       v

# Apache Spark Structured Streaming

# &#x20;       |

# &#x20;       +-------------------------------+

# &#x20;       |                               |

# &#x20;       v                               v

# Apache HDFS                       Kafka Output Topics

# raw\_trades                        crypto.market.scores

# window\_scores                     crypto.anomalies

# checkpoints                              |

# &#x20;                                       v

# &#x20;                                Streamlit Dashboard

