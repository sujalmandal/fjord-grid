# FjordGrid — Build Specification

> Concrete, buildable spec derived from `DESIGN.md`.

**Stack:** Java 21 · Spring Boot 3 · Kafka (optional profile) · PostgreSQL/TimescaleDB
(roadmap) · Ollama/LangChain4j (optional) · Docker.

---

## 1. Functional requirements

### FR-1 Market data ingestion
- `EntsoeFetcher` polls ENTSO-E day-ahead prices (documentType=A44) for zones NO1–NO5
  (+ optional DE/SE/NL comparators) at 15-/60-min cadence.
- Optional: generation-by-type, cross-border physical flows.
- Normalizes responses into `PriceUpdate` / `GenerationUpdate` events published to the feed.
- Mock fetcher replays a committed real-day snapshot when no `ENTSOE_KEY` is set.

### FR-2 Price board & windows
- `GET /api/v1/prices` returns latest clearing price per zone + change vs previous interval.
- `GET /api/v1/prices/{zone}` returns a rolling window (e.g., 96 points) for sparklines.
- Per-zone EWMA baseline + z-score stored alongside the window.

### FR-3 Spread signal
- `GET /api/v1/spreads` returns pairwise spreads (NO1−NO2, NO1−NO3, …) and flags where the
  spread exceeds a configurable capacity-fee threshold (binding interconnector likely).

### FR-4 Generation mix
- `GET /api/v1/generation` returns stacked generation by type per zone over the window.

### FR-5 Anomaly detection
- EWMA residual + rolling σ; an anomaly fires when |residual| > k·σ (k configurable).
- Every anomaly event exposes its components (observed, baseline, σ, residual) — explainable.

### FR-6 LLM market briefing
- `GET /api/v1/briefing` returns a natural-language summary of the last window, grounded in
  the actual numbers (zones, deltas, anomalies, spreads).
- Statistical/templated fallback when no LLM; Ollama-backed summary when enabled.

### FR-7 Streaming
- `GET /api/v1/stream/prices` and `/stream/signals` push events over SSE.
- With the Kafka profile, events are produced to `fjordgrid.prices` / `fjordgrid.signals`
  and the SSE path tails them (demonstrating pub/sub decoupling).

---

## 2. ENTSO-E integration contract

```java
interface MarketFeed {
    void publish(PriceUpdate u);
    void publish(SignalEvent e);
    Flux<MarketEvent> stream();   // SSE source
}

interface MarketDataFetcher {
    List<PriceUpdate> fetchPrices(Instant from, Instant to, List<Zone> zones);
}
```

Implementations: `EntsoeFetcher` (WebClient, live), `ReplayFetcher` (offline default).
Selected by `fjordgrid.source=entsoe|replay`.

**ENTSO-E request shape:**
```
GET https://web-api.tp.entsoe.eu/api
    ?securityToken=$KEY&documentType=A44
    &in_Domain=10YNO-0--------C&out_Domain=10YNO-0--------C
    &periodStart=YYYYMMDDHHMM&periodEnd=YYYYMMDDHHMM
```
Domain EIC codes: NO1=10YNO-1--------2, NO2=10YNO-2--------T, NO3=10YNO-3--------J,
NO4=10YNO-4--------9, NO5=10YNO-5--------H.

---

## 3. Signal math (interpretable by design)

- **EWMA baseline:** `s_t = α·x_t + (1−α)·s_{t−1}`, α ≈ 0.3.
- **Residual:** `r_t = x_t − s_t`.
- **Anomaly flag:** `|r_t| > k·σ_t`, σ from rolling window, k ≈ 3 (configurable).
- **Spread bind flag:** `spread_ij > threshold_ij` (per-pair, hand-tunable).

All constants exposed via config so a reviewer can tune them in the demo.

---

## 4. Data model

```sql
price_updates(id, zone, interval_start, price_eur_per_mwh, currency, fetched_at)
generation_updates(id, zone, interval_start, psr_type, quantity_mwh, fetched_at)
signals(id, zone_pair, type SPREAD|ANOMALY, payload jsonb, severity, created_at)
briefings(id, window_start, window_end, summary_text, mode, created_at)
```
Roadmap: TimescaleDB hypertables on `price_updates`/`generation_updates` with continuous
aggregates for hourly/daily rollups.

---

## 5. Non-functional requirements

| NFR | Target |
|---|---|
| Latency (read) | p99 < 150 ms |
| Fetch resilience | Retry + backoff on ENTSO-E 5xx; never crash on a bad interval |
| Rate limit | Cache day-ahead response (it's per-day); poll cadence ≥ ENTSO-E limits |
| Observability | last-successful-fetch timestamp in health; alert counter metric |
| Security | API key via env only; no key in repo or image |

---

## 6. Acceptance criteria

1. `docker compose up --build` (no key) starts with replay data; dashboard shows NO1–NO5
   prices and sparklines.
2. `ENTSOE_KEY=... docker compose up` pulls live day-ahead prices and they appear on the
   board within one poll cycle.
3. Forcing a synthetic spike in the replay triggers an anomaly signal that exposes its EWMA
   math in the response.
4. A large injected NO1−NO3 spread flips the bind flag on `/api/v1/spreads`.
5. `GET /api/v1/briefing` returns a templated summary offline; with Ollama enabled returns an
   LLM summary referencing the same numbers.
6. SSE streams push price + signal events live to the dashboard.
7. `mvn test` passes unit (math, parsing) + integration (fetcher→feed→REST) suites.
8. `GET /swagger-ui.html` reflects the full contract.

---

## 7. Definition of done (recruiter-readiness)

- README quickstart + architecture diagram (synced with DESIGN.md).
- CI: build + test on push; green badge.
- OpenAPI spec; live `/swagger-ui.html`.
- A committed real-day ENTSO-E snapshot for the offline replay (proves the parser works on
  real XML/JSON, not hand-written fixtures).
- EWMA/anomaly unit tests with a known divergent series.
- Multi-stage Dockerfile; image < 250 MB.
