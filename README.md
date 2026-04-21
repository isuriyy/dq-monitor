# DQ Monitor — Data Quality Monitoring Platform

> \*\*Built by Isuri Wijegunawardhana\*\* · Python · Flask · JavaScript · Google Gemini API · 76 automated tests

A production-grade data quality monitoring platform that automatically profiles multiple databases in parallel, detects statistical anomalies using three algorithms, generates AI-powered plain-English explanations and root cause analysis, sends real-time Slack and email alerts, and presents everything in a professional web dashboard — without any manual intervention after setup.

\---

## The Problem This Solves

Standard database monitoring tells you when a server goes offline. It cannot detect:

* A pipeline that loaded **9 rows instead of 2,000** — server was healthy, data was silently wrong
* A null rate that **jumped from 2% to 45%** — upstream API changed its format with no warning
* **Prices multiplied by 100** due to a currency conversion bug — every row loaded successfully
* **Schema drift** — a column type changed in production, breaking downstream dashboards silently
* **Cross-database inconsistency** — customer counts diverged 80% between CRM and orders database

DQ Monitor catches all of these. It addresses the same problem that **Monte Carlo** ($1.6B), **Great Expectations**, and **Soda** are built to solve — data observability is the missing layer above infrastructure monitoring that every data team needs.

\---

## Features

|Feature|Description|
|-|-|
|Multi-source profiling|Profiles multiple databases simultaneously using ThreadPoolExecutor|
|3-algorithm anomaly detection|Z-Score, IQR, and Isolation Forest running in parallel across all sources|
|LLM explanations|Plain-English anomaly explanations via Google Gemini API (free tier)|
|Root cause analysis|3 ranked likely causes + SQL queries to run + concrete fix instructions|
|AI chat assistant|Ask natural-language questions about your data quality in the dashboard|
|Cross-DB checks|Row count consistency, null rates, schema differences across sources|
|Scheduled pipeline|APScheduler — full pipeline runs automatically every N minutes|
|Deduplicated alerts|Slack + email alerts with 60-minute suppression window — no spam|
|Professional dashboard|Dark mode, responsive, source selector, 7-page navigation|
|One-click reports|PDF, CSV, and Excel download directly from the browser|
|History audit trail|Every pipeline run, table profiled, and alert sent — stored permanently|
|76 automated tests|Unit, integration, and data quality tests — all pass in under 6 seconds|

## Testing

76-test suite covering all four professional testing types.

```bash
python -m pytest
# 76 passed in 20.79s
```

|Type|Tests|What is tested|
|-|-|-|
|Unit tests|40|Anomaly detectors, DQ scoring, URL building, LLM explainer, root cause|
|Integration tests|18|Real databases, real JSON files, alert deduplication|
|Data quality tests|8|4 real-world scenarios end-to-end|
|**Total**|**76 passed**|0 failures|

### Data quality test scenarios

These tests prove the system's actual purpose — if something goes wrong with data, does DQ Monitor catch it?

```
Scenario 1 — Pipeline failure
  GIVEN: 30 days of stable \~2000 rows
  WHEN:  Today's pipeline loads only 9 rows (99.5% drop)
  THEN:  Z-Score and IQR both flag CRITICAL ✓

Scenario 2 — Null explosion  
  GIVEN: 30 days with \~2% null rate
  WHEN:  Today's null rate jumps to 45.2%
  THEN:  System detects and classifies severity correctly ✓

Scenario 3 — Value corruption
  GIVEN: 30 days with order total mean \~$1,000
  WHEN:  Today's mean is $100,000 (currency conversion bug ×100)
  THEN:  Mean spike flagged as CRITICAL ✓

Scenario 4 — Silent duplication
  GIVEN: 30 days with \~2,000 rows
  WHEN:  Today has 4,000 rows (pipeline ran twice)
  THEN:  Row count spike detected as HIGH ✓

Scenario 5 — Clean pipeline (no false positives)
  GIVEN: 30 days of normal data
  WHEN:  Today's pipeline runs cleanly
  THEN:  Zero findings, zero false alarms ✓
```

Each test creates a fresh isolated database, builds 30 days of clean history, injects a specific real-world problem, runs the detectors, and asserts the correct outcome.

\---

\---

## Architecture

```
dq\_monitor/
├── main.py                        # Multi-source parallel profiler
├── run\_anomaly\_detection.py       # Z-Score + IQR + Isolation Forest + LLM
├── cross\_db\_checks.py             # Cross-database consistency checks
├── run\_alerting.py                # Slack + email with deduplication
├── scheduler.py                   # APScheduler — full automated pipeline
├── generate\_report.py             # PDF + CSV + Excel report generator
├── api\_server.py                  # Flask API — connections, reports, AI chat
├── serve.py                       # Static file server for dashboard
├── export\_dashboard\_data.py       # Exports all data to JSON
├── anomaly/
│   ├── zscore\_detector.py         # Z-Score anomaly detector
│   ├── iqr\_detector.py            # IQR anomaly detector
│   ├── isolation\_forest\_detector.py  # ML anomaly detector
│   ├── llm\_explainer.py           # Gemini — plain-English explanations
│   └── llm\_assistant.py           # Root cause analysis + AI chat
├── profiler/
│   ├── connector.py               # Multi-dialect DB connector
│   ├── profiler.py                # Table profiler — all column metrics
│   └── schema\_fingerprint.py      # Schema drift detection
├── alerting/
│   ├── slack\_sender.py            # Slack webhook sender
│   ├── email\_sender.py            # Gmail SMTP sender
│   └── dedup\_store.py             # 60-min deduplication window
├── tests/
│   ├── test\_detectors.py          # Unit — Z-Score, IQR, Isolation Forest
│   ├── test\_scoring.py            # Unit — scoring, URLs, explainer
│   ├── test\_integration.py        # Integration — real databases
│   └── test\_data\_quality.py       # DQ tests — 4 real-world scenarios
├── config/sources.yaml            # Database connections
├── web\_dashboard/                 # 7-page HTML/CSS/JS dashboard
└── metadata.db                    # SQLite — all snapshots, alerts, history
```

\---

## How It Works

Every scheduled run executes this full pipeline automatically:

```
1. Profile all databases     →  parallel threads, saves to metadata.db
2. Cross-DB checks           →  row counts, null rates, missing tables
3. Anomaly detection         →  Z-Score + IQR + Isolation Forest on all metrics
4. LLM explanations          →  Gemini API — one sentence per anomaly
5. Root cause analysis       →  3 ranked causes + SQL queries + fix steps
6. Send alerts               →  Slack + email, deduplicated
7. Export dashboard data     →  JSON files updated, dashboard auto-refreshes
```

\---

## Quick Start

### 1\. Install dependencies

```bash
pip install pandas scikit-learn sqlalchemy apscheduler flask flask-cors \\
            reportlab openpyxl rich pyyaml google-genai pytest
```

### 2\. Configure databases in `config/sources.yaml`

```yaml
sources:
  - name: my\_database
    dialect: sqlite        # or postgresql, mysql
    path: ./data/mydb.db
    description: "My database"
```

### 3\. Add API keys to `.env`

```
GEMINI\_API\_KEY=AIzaSy...
SLACK\_WEBHOOK\_URL=https://hooks.slack.com/...
EMAIL\_SENDER=you@gmail.com
EMAIL\_PASSWORD=your\_app\_password
EMAIL\_RECEIVER=team@company.com
```

### 4\. Run once

```bash
python scheduler.py --once
```

### 5\. Run the tests

```bash
python -m pytest
# 76 passed in 20.79s
```

### 6\. Start the dashboard

```bash
python serve.py        # Window 1 → http://localhost:8080
python api\_server.py   # Window 2 → http://localhost:5050
```

### 7\. Schedule automatic runs

```bash
python scheduler.py                      # every 60 minutes
python scheduler.py --interval 30        # every 30 minutes
python scheduler.py --cron "0 8 \* \* \*"  # daily at 8am
```

\---

## Running Tests

```bash
python -m pytest                              # all 76 tests
python -m pytest tests/test\_data\_quality.py  # data quality scenarios only
python -m pytest --cov=. --cov-report=term-missing  # with coverage
```

|Type|Count|What is tested|
|-|-|-|
|Unit|40|Detectors, scoring, URL building, LLM explainer, root cause|
|Integration|18|Real database connections, deduplication, real file exports|
|Data quality|8|Pipeline failure, null explosion, price corruption, silent duplication|

\---

## Sample Output

**Terminal:**

```
━━━ ECOMMERCE\_DB.ORDERS ━━━
  CRITICAL  std.total  iqr  Today: 868.86  Expected: 344-471

  → The spread of order values is unusually high today — outlier values
    may have been loaded or the data distribution changed unexpectedly.

  Root cause:
    1. Outlier rows loaded — extreme total values skewing the distribution
    2. Currency/unit change — values switched units for some rows
    3. Data corruption — invalid values slipped through validation

  Check: SELECT MIN(total), MAX(total), AVG(total) FROM orders
  Fix:   Identify outlier rows, add range validation to pipeline.
```

**AI Assistant:**

```
You: Which table should I investigate first?
AI:  The orders table is most urgent — CRITICAL anomaly in std.total
     (868.86 vs expected 344-471). Run SELECT MIN(total), MAX(total)
     FROM orders to find the extreme values causing the spike.
```

\---

## Technology Stack

* **Backend:** Python 3.14, pandas, scikit-learn, SQLAlchemy, APScheduler
* **AI / LLM:** Google Gemini API — free tier, graceful rule-based fallback
* **Alerting:** Slack Webhooks, Gmail SMTP
* **API:** Flask, flask-cors
* **Frontend:** Vanilla HTML/CSS/JavaScript, Chart.js — no framework
* **Databases:** SQLite, PostgreSQL, MySQL, BigQuery, RDS, Snowflake
* **Reports:** ReportLab (PDF), openpyxl (Excel), csv
* **Testing:** pytest, pytest-cov

\---

## Known Limitations

* Anomaly detection needs 7+ historical snapshots per table to warm up
* Gemini free tier has rate limits — rule-based fallback activates automatically
* Dashboard requires a local web server — cannot be opened as a plain HTML file
* PDF download requires `api\_server.py` running alongside `serve.py`

\---

## About

Built by **Isuri Wijegunawardhana** as a project demonstrating production-grade data engineering skills including statistical anomaly detection, LLM API integration, multi-database parallel architecture, real-time alerting, full-stack web development, and a 76-test automated test suite.

Modelled on commercial data observability platforms (Monte Carlo, Great Expectations, Soda). Solves the same core problem: **database uptime monitoring is not enough — data quality monitoring is the missing layer that every data team needs.**

*Built entirely from scratch. No templates, no boilerplate, no tutorial followed.*

