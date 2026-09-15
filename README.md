# Real-Time Data Pipeline

Event streaming platform: Kafka, Spark Structured Streaming, PostgreSQL, FastAPI, Grafana,
Prometheus — all wired together and tested end-to-end.

- Apache Kafka
- Spark Structured Streaming
- PostgreSQL
- FastAPI (API-key protected)
- Grafana (dashboards + alerting, with real email delivery via Mailhog)
- Prometheus
- Docker

## Architecture

```
producer (Faker events) --> Kafka topics --> Spark Structured Streaming --> PostgreSQL --> FastAPI --> Grafana
                             user-events        (windowed aggregates          (analytics       /metrics   dashboards
                             orders              + raw event log)             schema)          scraped by  + alerting
                             payments                                                           Prometheus  (-> Mailhog)
                             inventory                                              ^
                             notifications                                          |
                                                                              retention job
                                                                          (daily purge, DATA_RETENTION_DAYS)
```

- **producer** generates fake e-commerce events (`login`, `search`, `add_to_cart`, `checkout`,
  `payment`, `purchase`, `review`, ...) and publishes them as JSON to Kafka.
- **spark** consumes every topic, writes the raw parsed events to `analytics.events`, and computes
  1-minute tumbling-window aggregates (revenue by category/country, device activity, event-type
  funnel) that it upserts into PostgreSQL.
- **api** (FastAPI) reads those tables and exposes REST endpoints (protected by an API key) plus a
  Prometheus `/metrics` endpoint.
- **retention** runs `analytics.cleanup_old_data()` once a day, purging rows older than
  `DATA_RETENTION_DAYS` from every table so the database doesn't grow unbounded.
- **prometheus** scrapes the API; **grafana** ships with a dashboard and two alert rules that
  deliver real emails through **mailhog** (a local SMTP catcher with a web UI — no external mail
  account needed to see alerting actually work).

## Running everything

```bash
cp .env.example .env
docker compose up -d --build
```

This builds and starts: `postgres`, `kafka`, `kafka-ui`, `pgadmin`, `producer`, `spark`, `api`,
`retention`, `prometheus`, `grafana`, `mailhog`.

The Spark image resolves the Kafka/Postgres connector jars via Maven on first start
(`spark-sql-kafka-0-10`, `postgresql`), so the first boot of the `spark` container takes a couple
of minutes before the streaming queries come up — watch it with `docker compose logs -f spark`.

### Ports (see `.env`)

| Service        | URL                              |
|----------------|-----------------------------------|
| API            | http://localhost:8000 (docs at `/docs`) |
| API metrics    | http://localhost:8000/metrics     |
| Grafana        | http://localhost:3000 (admin / see `GRAFANA_USER`/`GRAFANA_PASSWORD`) |
| Prometheus     | http://localhost:9090             |
| Kafka UI       | http://localhost:8080             |
| pgAdmin        | http://localhost:5051             |
| Mailhog (alert emails) | http://localhost:8025      |
| Spark UI       | http://localhost:4040             |
| Postgres       | localhost:5432                    |

### API endpoints

`/health` is open (used for container/monitoring health checks). Everything else requires an
`X-API-Key` header matching `API_KEY` from `.env`:

```bash
curl -H "X-API-Key: $API_KEY" http://localhost:8000/events/recent?limit=5
```

- `GET /health` — DB connectivity check (no key required)
- `GET /events/recent?limit=50`
- `GET /analytics/sales-by-category?minutes=60`
- `GET /analytics/sales-by-country?minutes=60`
- `GET /analytics/device-stats?minutes=60`
- `GET /analytics/event-funnel?minutes=60`
- `GET /analytics/summary?minutes=60`
- `GET /metrics` — Prometheus scrape target (no key required)

## How to see it actually working

**1. Live data end-to-end** — hit the API and watch fresh numbers on every call:

```bash
curl -H "X-API-Key: $API_KEY" http://localhost:8000/analytics/summary?minutes=60 | jq
curl -H "X-API-Key: $API_KEY" http://localhost:8000/events/recent?limit=1 | jq
```
Run the second command twice a few seconds apart — the `event_time` should always be within the
last couple of seconds, proving producer → Kafka → Spark → Postgres → API is live, not replaying
old data.

**2. The dashboard** — open http://localhost:3000 (Grafana, credentials in `.env`) →
**Real-Time Pipeline Overview**. Revenue, orders, and event counts update every 10s.

**3. Alerting, with a real inbox** — open http://localhost:8025 (Mailhog). Then, to actually
trigger an alert instead of waiting:

```bash
docker compose stop api      # simulates an outage
```
Within ~3 minutes (2m alert `for` duration + evaluation lag) an "API is down" email lands in
Mailhog. You'll also see the rule go red under Grafana → Alerting → Pipeline Alerts. Bring it back
with `docker compose start api`; the alert resolves and Mailhog gets a resolved notification too.

**4. Auth actually enforced**:

```bash
curl -i http://localhost:8000/events/recent                       # 401, no key
curl -i -H "X-API-Key: wrong" http://localhost:8000/events/recent  # 401, wrong key
curl -i -H "X-API-Key: $API_KEY" http://localhost:8000/events/recent  # 200
```

**5. Retention, proven non-destructively**:

```bash
docker exec realtime-postgres psql -U admin -d analytics -c \
  "SELECT * FROM analytics.cleanup_old_data(30);"
```
Returns a row per table with how many stale rows it purged (0 on a fresh dataset — everything is
recent). The `retention` container runs this automatically once a day.

**6. Automated tests** — see [Tests](#tests) and [CI](#ci) below; the CI badge/run history on
GitHub is the strongest signal since it's a clean checkout, not this machine's local state.

## Running the producer locally (without Docker)

```bash
cd producer
python -m venv .venv && .venv/Scripts/activate  # or source .venv/bin/activate
pip install -r requirements.txt
python -m app.main
```

It reads `KAFKA_BOOTSTRAP` from `.env` (defaults to `localhost:9092`, which matches Kafka's
host-exposed port in `docker-compose.yml`).

## Project layout

```
producer/          Kafka event producer (Faker-generated e-commerce events)
spark/              Spark Structured Streaming job (Kafka -> Postgres)
api/                FastAPI analytics API (Postgres -> REST + /metrics), API-key auth
database/           Postgres schema + cleanup_old_data() retention function
scripts/retention.sh  Daily retention loop run by the `retention` service
monitoring/         Prometheus scrape config + Grafana provisioning/dashboards/alerting
tests/integration/  End-to-end smoke test against the live docker-compose stack
```

## Tests

Each service has its own test suite under `<service>/tests/`, runnable independently with no
external services required (Kafka/Postgres calls are mocked; Spark tests run a local `local[1]`
SparkSession).

```bash
pip install -r producer/requirements-dev.txt && pytest producer/tests
pip install -r api/requirements-dev.txt && pytest api/tests
pip install -r spark/requirements-dev.txt && pytest spark/tests   # needs a JDK on PATH
```

There's also a real end-to-end test against a running stack (see `tests/integration/`):

```bash
docker compose up -d --build postgres kafka producer spark api
pip install -r tests/integration/requirements.txt
API_KEY=local-dev-key-change-me pytest tests/integration -v
```

Lint (ruff, config in `pyproject.toml`):

```bash
pip install ruff && ruff check producer/app api/app spark/app
```

## CI

`.github/workflows/ci.yml` runs on every push/PR to `main`:

1. `lint` — ruff
2. `test-producer`, `test-api`, `test-spark` — unit tests, in parallel
3. `compose-validate` — `docker compose config` against `.env.example`
4. `e2e-test` — after the above pass, spins up the real stack (`postgres`, `kafka`, `producer`,
   `spark`, `api`) and runs `tests/integration` against it, so every push is verified against an
   actual running pipeline, not just mocks.

## Alerting

Grafana is provisioned (`monitoring/grafana/provisioning/alerting/`) with two alert rules under
the "Pipeline Alerts" folder, both routed to a `pipeline-oncall` contact point (email):

- **API is down** — Prometheus `up{job="api"} < 1` for 2+ minutes.
- **No events ingested in the last 5 minutes** — queries `analytics.events` directly via the
  PostgreSQL datasource; fires if the Spark pipeline stalls (Kafka, producer, or the streaming job
  itself).

Grafana's SMTP is pointed at the bundled `mailhog` service, so alert emails are actually sent and
viewable at http://localhost:8025 — see step 3 in [How to see it actually working](#how-to-see-it-actually-working).
For a real deployment, swap `GF_SMTP_HOST` for a real SMTP relay (or repoint the contact point at
Slack/PagerDuty/a webhook).

## Security & production-hardening notes

This is tuned for local development speed, not turnkey production deployment. Known gaps, and
what closing them for real would involve:

- **API auth** is a single shared key (`API_KEY`) suitable for service-to-service or demo use —
  not per-user auth. A real deployment would put OAuth2/JWT in front of it.
- **Secrets** live in `.env` (gitignored) with placeholder demo values in `.env.example`
  (`admin/admin123`, `local-dev-key-change-me`). Rotate all of them before exposing anything
  beyond localhost; a real deployment would pull these from a secrets manager, not a `.env` file.
- **No TLS** anywhere — every service listens on plain HTTP/TCP. Fine on localhost; behind a real
  ingress you'd terminate TLS at a reverse proxy/load balancer in front of the API and Grafana.
- **Single Kafka broker** (`replication_factor=1`) — no fault tolerance. Production Kafka would
  run 3+ brokers in the KRaft quorum with `replication_factor=3` and `min.insync.replicas=2`.
- **No CD** — CI tests every push but doesn't deploy anywhere (no cloud target is configured in
  this repo). Wiring up a deploy job needs a real destination (registry + target environment).
