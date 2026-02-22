# Irembo Voice AI Analytics

**Analytics framework for the Irembo Voice AI platform** — KPI design, data modeling, friction analysis, error reduction methodology, and data governance. Built as part of a Data Analyst take-home assignment evaluating whether Voice AI is improving accessibility, efficiency, and adoption of public services in Rwanda.

---

## Dataset

The analysis is grounded in **five CSV source tables** representing a realistic end-to-end Voice AI system (Jan – Apr 2025):

| File | Description | Rows |
|---|---|---|
| `users.csv` | Anonymised citizen profiles with region, disability flag, and first-time digital user flag | 415 users |
| `voice_sessions.csv` | One row per voice call / journey with outcome, duration, and turn count | 1,200 sessions |
| `voice_turns.csv` | Utterance-level interactions — one row per turn, with error type and intent | ~6,500 turns |
| `voice_ai_metrics.csv` | AI performance metrics aggregated per session (ASR confidence, misunderstanding rate, etc.) | 1,200 rows |
| `applications.csv` | Service applications linked to sessions, with status, service code, channel, and submission time | 900 applications |

---

## Repository Structure

```
irembo-voiceai-analytics/
│
├── part-1/                         # KPI Framework (Accessibility, Efficiency, Adoption)
│   ├── accessibility.sql
│   ├── efficiency.sql
│   └── adoption.sql
│
├── part-2/                         # Data Modeling — dbt-style 3-layer pipeline
│   ├── staging/
│   │   └── sql/
│   │       ├── stg_users.sql
│   │       ├── stg_sessions.sql
│   │       └── stg_ai_metrics.sql
│   ├── intermediate/
│   │   └── sql/
│   │       ├── int_turns_aggregated.sql
│   │       └── int_sessions_enriched.sql
│   ├── mart/
│   │   ├── fact_voice_ai_sessions.sql  ← main analysis-ready table
│   │   └── fact_voice_ai_sessions.csv  ← sample output (1,200 rows)
│   └── usecase/
│       ├── accessibility.sql
│       ├── efficieny.sql
│       ├── adoption.sql
│       └── end_to_end.sql
│
├── part-3/                         # Insight Generation
│   ├── frictions/
│   │   ├── point1.sql              # Turn error rate analysis
│   │   ├── point2.sql              # Recovery failure analysis
│   │   └── point3.sql              # Escalation logic audit
│   └── completion/
│       ├── voice_vs_non_voice.sql
│       └── rural_vs_urban.sql
│
└── README.md
```

---

## How to Use the SQL

### Tool
All SQL in this repository was **written and tested in Metabase** using ClickHouse-compatible SQL syntax. Queries are ready to run as long as your data sources are connected.

### The only change you need to make
Each query references the source tables by the upload names used during development (e.g. `upload_users_20260220015436`). You will need to **replace those table names** with the actual names of your connected data sources.

For example, in `fact_voice_ai_sessions.sql`:

```sql
-- Change this:
FROM upload_users_20260220015436

-- To this (your actual table name):
FROM users
```

The five source tables to rename are:

| Placeholder name in SQL | Replace with |
|---|---|
| `upload_users_20260220015436` | your `users` table |
| `upload_voice_sessions_20260220015550` | your `voice_sessions` table |
| `upload_voice_turns_20260220015613` | your `voice_turns` table |
| `upload_voice_ai_metrics_20260220015601` | your `voice_ai_metrics` table |
| `upload_applications_20260220015647` | your `applications` table |

No other changes are required.

---

## Data Model: `fact_voice_ai_sessions`

The centrepiece of the repository. A single analysis-ready mart table with **55 columns and one row per voice session**, built from five CTEs following a standard dbt Staging → Intermediate → Mart pattern.

```
users.csv          → stg_users           ─┐
voice_sessions.csv → stg_sessions         ├─→ int_sessions_enriched → fact_voice_ai_sessions
voice_ai_metrics.csv → stg_ai_metrics    ─┤
voice_turns.csv    → int_turns_aggregated ─┤
applications.csv   → int_apps_per_session ─┘
```

The table is organised into 9 blocks:

| Block | Contents |
|---|---|
| 1 — Identifiers | `session_id`, `user_id` |
| 2 — Session Dimensions | channel, language, date, month |
| 3 — User Dimensions | region, disability flag, first-time flag, vulnerability flag |
| 4 — Session Outcome Metrics | completion, abandonment, transfer flags, duration |
| 5 — AI Performance Metrics | ASR confidence, misunderstanding rate, silence rate, recovery flag |
| 6 — Turn-Level Aggregates | error turns, intent counts, turn error rate, repeat intent rate |
| 7 — Application Outcomes | attempts, completions, failures, service codes, submission time |
| 8 — KPI Flags | 12 pre-computed boolean flags for direct use in Metabase dashboards |
| 9 — Composite Score | `ai_quality_score` (0–100) for trend monitoring and anomaly detection |

> Full schema with all 55 columns, data types, and descriptions is documented in the assignment PDF.

---

## Part 1 — KPI Framework

Three KPI categories, each with 5 metrics:

**Accessibility / Inclusivity** — Disability Inclusion Rate, Rural vs Urban Completion Parity, First-Time User Success Rate, Silence Error Rate by Segment, Escalation Rate for Vulnerable Users

**Efficiency** — Session Completion Rate (56.4%, target ≥70%), Error Recovery Rate (51.6%, target ≥75%), Average Time to Complete Application (10.26 min, target <7 min), Turn-Level Error Rate (39.77%, target <20%), Application Failure Rate by Service and Channel

**Adoption** — Monthly Active Users, User Return Rate (44.8%, target ≥40%), Channel Mix Shift Toward Voice, Application Submission Rate per User, New User Acquisition Rate

---

## Part 3 — Key Findings

Three principal friction points identified from the data:

1. **The AI does not understand users** — 39.77% of the 6,500 voice turns contain an error (misunderstanding 12.5%, silence 9.7%, noise 3.0%). The rate has been flat across all four months with no natural improvement.

2. **The AI cannot recover when it fails** — 50.1% of error sessions fail to recover. Abandoned users average 8.4 turns before giving up, nearly the same as the 8.7 turns in a completed session. They are not dropping early; they are hitting a wall at the end.

3. **Escalation logic is inverted** — Escalated sessions have a 43.6% average silence rate but only 13.1% misunderstanding rate. Non-escalated sessions show the opposite pattern (8.8% silence, 21.4% misunderstanding). The AI is escalating the wrong sessions to human agents.

---

## Part 4 — Error Reduction Target

The project target is a **40% reduction in turn-level errors**.

- **Baseline**: 39.6% average `turn_error_rate_pct` across 1,200 sessions (stable, ±2pp across all four months)
- **Target**: ≤ 23.8% (requires eliminating at least 1,034 error turns)
- **Measurement**: Compare `avg(turn_error_rate_pct)` pre vs post any model change, split by deployment date in `fact_voice_ai_sessions`. Require the improvement to hold for at least 4 consecutive weeks.
- **Watch for**: user mix shifts, the broken `had_errors` source column (use `misunderstanding_rate > 0 OR silence_rate > 0` instead), and shorter sessions that arithmetically reduce the rate without genuine improvement.

---

## Part 5 — Data Quality Notes

Two known issues in the source data that affect any analysis:

**Broken `had_errors` flag** — The raw `had_errors` column flags only 12 sessions as having errors. The correct count is 1,026 (85.5% of sessions). Never use the raw column. The fact table corrects this with:
```sql
(misunderstanding_rate > 0 OR silence_rate > 0) AS had_errors
```

**Turn-level aggregation overflow** — 30 sessions (2.5%) have `misunderstanding_turns + silence_turns + noise_turns > total_turns`, which is a pipeline error. These sessions should be excluded from per-session turn-level analysis until fixed upstream.

---

## Part 6 — Additional Metric Proposed

**First-Session Retention Rate** — of users who complete their first voice session, what percentage return within 30 days?

New user acquisition dropped 79% between February (53.5%) and April (11.4% ) 2025. With the pipeline shrinking, converting first-timers into return users is no longer a growth metric; it is a survival metric. This KPI directly links session quality to long-term platform sustainability.

---

## Contact

Built by [@karekezi90](https://github.com/karekezi90)
