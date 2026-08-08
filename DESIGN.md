# FjordGrid — Design Document

> Real-time Nordic electricity market monitor built on ENTSO-E Transparency Platform data,
> with an anomaly + flexibility-signal layer and an LLM summarizer.

**Status:** Designed · code to be built separately. This document is the build spec.
**Audience:** Engineering hiring managers in Norway's energy sector (Statnett, Equinor,
Kongsberg, Elvia, Norgesnett, Linja, BKK, Lyse, Quorum Software) and the consulting firms
that staff them.

---

## 1. Why this project exists

Norway's power grid is the country's strategic infrastructure, and the companies running it
are hiring Java engineers right now. The research surfaced a near-perfect archetype:

> **Statnett — Systemutvikler (FINN 470477480, Oslo, deadline 24.08.2026):**
> Java/Kotlin, Apache Kafka, Spring Boot, PostgreSQL/Oracle, OpenShift/Azure, Kubernetes,
> CI/CD, "KI-støtte" (AI support), and "interest in the energy industry is a plus."

FjordGrid is scoped to hit that ad directly: **Kafka streaming of real Nordic power-market
data, time-series windows, an anomaly/flexibility signal, and an LLM summarizer** — all on
data a Statnett or Equinor reviewer will immediately recognize as their own domain.

It replaces an earlier generic stock-ticker demo (OsloBørs). The pivot matters: a
stock-ticker project is one of the named **anti-signals** in the senior-hiring research
("tutorial project: todo/stock-ticker/generic-RAG = negative for senior"), whereas an
energy-market monitor built on the EU's official transparency dataset demonstrates domain
curiosity about *Norway's* economy, not a copy-paste widget.

---

## 2. The domain — why electricity data is a great senior demo

Norway's grid is unusual and interesting:

- **5 bidding zones (NO1–NO5)** with different supply/demand, connected by interconnectors
  that bottleneck — so prices *diverge* between zones in real time. This is a genuine
  streaming + windowing problem, not synthetic.
- **Heavy hydro** generation with reservoir levels that swing seasonally.
- **Massive cross-border cables** (NordLink to Germany, North Sea Link to the UK,
  Skagerrak/SwePol to the continent) — exports/imports flip direction by the hour.
- **Green transition** making flexibility (battery, demand-response) a hot engineering area.

A reviewer at Statnett sees, instantly, that the candidate understands bidding zones,
day-ahead vs intraday, and why a price-spread signal between NO1 and NO3 is meaningful.
That domain literacy is the differentiator.

---

## 3. The data source — ENTSO-E Transparency Platform

- **What:** The EU's official, free, open-data platform for pan-European electricity
  generation, load, transmission, and prices (Regulation (EU) 543/2013).
- **Access:** Free API key — register at `transparency.entsoe.eu`, then email
  `transparency@entsoe.eu` to activate the key. Verified live Aug 2026.
- **Key documents used:**
  - **12.1.D Energy Prices → Day-ahead prices** — covers NO1–NO5 and all EU zones;
    15-minute and 60-minute intervals.
  - Generation by type (production_type), load/forecast, cross-border physical flows,
    installed capacity.
- **Base URL:** `https://web-api.tp.entsoe.eu/api?securityToken=...&documentType=A44`
  (A44 = day-ahead prices).
- **Rate limit:** reasonable; caching handles it for a demo.

**Why this matters to a reviewer:** the data is *real Norwegian infrastructure data*, not
`Math.random()` price ticks. The earlier OsloBørs demo generated synthetic quotes; FjordGrid
pulls actual day-ahead clearing prices for the zones a Statnett engineer balances every day.

---

## 4. What the demo does

1. **Live price board** — day-ahead prices for NO1–NO5 (plus optional EU comparators:
   DE, SE, NL), streaming in as new intervals publish.
2. **Zone-spread signal** — computes the spread between zones (e.g., NO1 − NO3) and flags
   when interconnector constraints likely bind. This is the "flexibility/opportunity" signal
   a battery or demand-response operator would watch.
3. **Generation mix** — stacked generation by type (hydro, wind, thermal) per zone over a
   rolling window.
4. **Anomaly detection** — z-score / EWMA on the price stream flags sudden spikes or dips
   (a storm, a cable trip, a cold snap) — explainable, not a black box.
5. **LLM market summary** — a periodic natural-language briefing ("Prices in NO2 spiked
   34% on low wind + export to NL; NO1 stable") grounded in the actual numbers, with an
   honest lexicon/statistical fallback when no LLM is configured.
6. **Event streaming** — every tick and signal flows through Kafka topics; the dashboard
   receives updates over SSE.

---

## 5. Architecture

```
        ┌──────────────────────────────────────────────┐
        │  Dashboard UI (static, served at / )         │
        │  price board · spreads · generation · alerts │
        └──────────────────────┬───────────────────────┘
                               │ SSE  /api/v1/stream/prices .../signals
                               ▼
        ┌──────────────────────────────────────────────┐
        │  Spring Boot 3 backend (:8082)               │
        │  PriceService      — rolling price windows   │
        │  SpreadService     — zone arbitrage signal   │
        │  GenerationService — mix by type             │
        │  AnomalyService    — EWMA / z-score detector │
        │  BriefingService   — LLM market summary      │
        └──────┬──────────────────┬────────────────────┘
               │ MarketFeed (port)│
        ┌──────▼──────┐    ┌──────▼──────────┐
        │ InMemory    │    │ KafkaMarketFeed │
        │ (offline)   │    │ fjordgrid.prices│
        └─────────────┘    │ fjordgrid.signals│
        ┌──────────────────┴────────────────┐
        │ EntsoeFetcher (Scheduled)         │
        │  polls day-ahead + flows @15/60m  │
        └───────────────────────────────────┘
        Briefing: StatsSummary (offline) ↔ OllamaBriefing (LLM)
```

### 5.1 Port/adapter for the feed
- `MarketFeed` is an interface; offline it broadcasts in-process; with Kafka it publishes to
  `fjordgrid.prices` / `fjordgrid.signals` — zero UI changes between modes. This reuses the
  proven seam from the earlier streaming project (≈80% of the streaming/time-series/SSE
  scaffolding carries over; what changes is the *domain meaning* of the events).

### 5.2 Fetcher — the real-data edge
- `EntsoeFetcher` is a scheduled WebClient poller that calls the ENTSO-E API for NO1–NO5
  day-ahead prices + generation + flows, normalizes to internal events, and publishes.
- A **mock fetcher** replays a captured snapshot (committed CSV/JSON of a real day) so the
  demo runs without an API key. Default = mock; `ENTSOE_KEY=...` switches to live.

### 5.3 Time-series hot cache
- Per-zone rolling price window (e.g., 96 points for a day at 15-min granularity) for the
  sparkline + EWMA baseline. This is the "hot cache" layer of a TSDB-backed design;
  TimescaleDB is the roadmap persistence layer.

### 5.4 Explainable signals (not a black-box AI)
- **Spread signal**: `spread_ij = price_i − price_j`; flag when |spread| > capacity-fee
  threshold (a hand-tunable constant so the signal is *interpretable*).
- **Anomaly**: EWMA residual; alert when |residual| > k·σ. Every alert exposes its
  components so a reviewer sees the math, not magic.
- **LLM briefing** with an honest fallback: a templated statistical summary when no model is
  available; real LLM (Ollama) when enabled. The same "LLM + honest fallback" pattern used
  across the portfolio — consistency is itself a signal.

---

## 6. Why this beats the stock-ticker demo it replaces

| | OsloBørs (old) | FjordGrid (new) |
|---|---|---|
| Data | Synthetic `Math.random()` ticks | Real ENTSO-E Nordic market data |
| Domain signal | Generic finance | Norway's #1 strategic infrastructure |
| Target ads | "fintech" (vague) | Statnett/Equinor/Kongsberg (concrete, hiring now) |
| Senior perception | Tutorial anti-signal | Domain curiosity + real integration |
| Differentiation | Low (everyone builds a ticker) | High (few candidates touch energy data) |

The research was blunt: stock-ticker projects are a *negative* for senior candidates.
FjordGrid keeps the streaming/time-series/SSE engineering value but attaches it to a domain
where Norwegian employers are actively hiring and where most candidates never go.

---

## 7. Technology choices & rationale

| Choice | Rationale |
|---|---|
| **Java 21 + Spring Boot 3** | Matches Statnett/Equinor stacks (Java/Kotlin, Spring Boot). |
| **Kafka** | Explicit requirement in the Statnett ad and near-universal in Oslo Java roles. |
| **WebClient** | Non-blocking ENTSO-E polling; virtual threads option on Java 21. |
| **TimescaleDB / Postgres** (roadmap) | Time-series partitioning for price history — a strong talking point for grid/SCADA-style systems. |
| **Ollama + LangChain4j** | "KI-støtte" is literally in the Statnett ad; an LLM briefing demonstrates it. |
| **Docker / Compose** | One-command demo; containerization is a baseline expectation. |

---

## 8. API surface

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/prices` | Latest prices for NO1–NO5 (+ optional EU zones) |
| GET | `/api/v1/prices/{zone}` | Window + anomaly flag for one zone |
| GET | `/api/v1/spreads` | Current inter-zone spreads + bind flags |
| GET | `/api/v1/generation` | Generation mix by type |
| GET | `/api/v1/signals` | Active spread/anomaly signals |
| GET | `/api/v1/briefing` | Latest LLM/statistical market summary |
| GET | `/api/v1/stream/prices` | SSE: price updates |
| GET | `/api/v1/stream/signals` | SSE: signal events |
| GET | `/swagger-ui.html` | OpenAPI docs |

---

## 9. Testing strategy

- **Unit:** EWMA/z-score detector with known series; spread computation; ENTSO-E response
  parsing against captured fixtures.
- **Integration (Testcontainers):** fetcher → feed → REST/SSE end-to-end over the mock
  replay; Kafka path publishes/consumes idempotently.
- **Contract:** mock fetcher and live fetcher satisfy the same port.

---

## 10. Observability

- Metrics: fetch latency, API error rate, stream lag, anomaly-alert counter.
- JSON logs with zone + interval trace context.
- Health check reflecting ENTSO-E reachability + last-successful-fetch timestamp.

---

## 11. Deployment topology

### Local (per-folder)
- `projects/fjord-grid/docker-compose.yml`: backend + (optional) Kafka. Mock replay by
  default; `ENTSOE_KEY` enables live data.

### VPS (single deployable)
- Root `docker-compose.yml` runs `fjord-grid-backend` on internal port 8082.
- nginx routes `grid.sujalmandal.dev` → `fjord-grid-backend:8082`.

See root `DEPLOYMENT.md`.

---

## 12. Roadmap

1. TimescaleDB hypertables for price history + continuous aggregates.
2. Cross-border flow direction visualization (import vs export per cable).
3. Hydro reservoir level correlation (seasonal context in the briefing).
4. Backtest the spread/anomaly signal against historical ENTSO-E data.
5. Flexibility/battery dispatch simulator (charge when spread negative, discharge on spike).

---

## 13. What this signals to a reviewer

A Statnett or Equinor hiring manager sees a candidate who went to the *actual* European
power-market dataset, understood bidding zones, and built an explainable streaming + signal
system rather than a generic ticker. Combined with the DESIGN.md arguing the trade-offs
(Kafka vs in-process, real data vs synthetic, LLM with honest fallback), this is the
"judgment + domain curiosity" signal the research says separates a senior hire from the
pack — and it opens the energy-sector door that most Java candidates never knock on.
