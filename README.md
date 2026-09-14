# Real-Time Data Pipeline

Production-grade event streaming platform using

- Apache Kafka
- Spark Structured Streaming
- PostgreSQL
- FastAPI
- Grafana
- Prometheus
- Docker

## Architecture

```
producer (Faker events) --> Kafka topics --> Spark Structured Streaming --> PostgreSQL --> FastAPI --> Grafana
                             user-events        (windowed aggregates          (analytics       /metrics   dashboards
                             orders              + raw event log)             schema)          scraped by
                             payments                                                           Prometheus
                             inventory
                             notifications
```

- **producer** generates fake e-commerce events (`login`, `search`, `add_to_cart`, `checkout`,
  `payment`, `purchase`, `review`, ...) with `Faker` and publishes them as JSON to Kafka.
- **spark** consumes every topic, writes the raw parsed events to `analytics.events`, and computes
  1-minute tumbling-window aggregates (revenue by category/country, device activity, event-type
  funnel) that it upserts into PostgreSQL.
- **api** (FastAPI) reads those tables and exposes REST endpoints, plus a Prometheus `/metrics`
  endpoint.
- **prometheus** scrapes the API; **grafana** is pre-provisioned with a Prometheus + PostgreSQL
  datasource and a starter dashboard.

## Running everything

```bash
docker compose up -d --build
```

This builds and starts: `postgres`, `kafka`, `kafka-ui`, `pgadmin`, `producer`, `spark`, `api`,
`prometheus`, `grafana`.

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
| Spark UI       | http://localhost:4040             |
| Postgres       | localhost:5432                    |

Copy `.env.example` to `.env` before first run (`.env` itself is gitignored).

### API endpoints

- `GET /health` — DB connectivity check
- `GET /events/recent?limit=50` — latest raw events
- `GET /analytics/sales-by-category?minutes=60`
- `GET /analytics/sales-by-country?minutes=60`
- `GET /analytics/device-stats?minutes=60`
- `GET /analytics/event-funnel?minutes=60`
- `GET /analytics/summary?minutes=60`

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
producer/   Kafka event producer (Faker-generated e-commerce events)
spark/      Spark Structured Streaming job (Kafka -> Postgres)
api/        FastAPI analytics API (Postgres -> REST + /metrics)
database/   Postgres schema (analytics.events + windowed aggregate tables)
monitoring/ Prometheus scrape config + Grafana provisioning/dashboards/alerting
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

Lint (ruff, config in `pyproject.toml`):

```bash
pip install ruff && ruff check producer/app api/app spark/app
```

## CI

`.github/workflows/ci.yml` runs on every push/PR to `main`: lint, the three test suites in
parallel jobs, and `docker compose config` to validate the compose file against `.env.example`.

## Alerting

Grafana is provisioned (`monitoring/grafana/provisioning/alerting/`) with two alert rules under
the "Pipeline Alerts" folder:

- **API is down** — Prometheus `up{job="api"} < 1` for 2+ minutes.
- **No events ingested in the last 5 minutes** — queries `analytics.events` directly via the
  PostgreSQL datasource; fires if the Spark pipeline stalls (Kafka, producer, or the streaming job
  itself).

Both route to a `pipeline-oncall` contact point (email). No SMTP server is configured in
`docker-compose.yml`, so alerts won't actually send anywhere out of the box — the rules are fully
functional and visible under Grafana's Alerting UI (state, history), but you'll want to either set
`GF_SMTP_*` env vars on the `grafana` service or repoint the contact point at a Slack/webhook
receiver to get real notifications.
