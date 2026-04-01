# PlaySight - Real-time Game Analytics System

A real-time game data analytics platform built with Flask, ClickHouse, and ECharts.

**Live Demo**: [http://112.126.59.73/](http://112.126.59.73/)

## Features

- **Real-time Overview**: Monitor DAU (Daily Active Users), match counts, skin sales, and total revenue.
- **Player Distribution**: Visualize player level distribution with interactive charts.
- **Retention Analytics**: Track day-1, day-3, and day-7 retention rates with trend visualization.
- **SQL Console**: Execute custom ClickHouse SQL queries directly from the dashboard.
- **Continuous Data Generation**: Automated event simulator running 24/7 with realistic intervals.
- **Data Visualization**: Beautiful dashboard powered by ECharts for clear data insights.

<img width="2166" height="1217" alt="image" src="https://github.com/user-attachments/assets/55500df1-d7e4-48dd-a092-23b49b985acd" />

## Project Structure

```
playsight/
├── main.py                   # Flask application entry
├── game_analytics/           # Main package
│   ├── app.py               # Flask app factory
│   ├── config.py            # Configuration
│   ├── models/              # Data models (Event)
│   ├── repositories/        # Data access layer (ClickHouse)
│   ├── services/            # Business logic
│   └── routes/              # API routes
├── scripts/                  # Executable scripts
│   ├── simulate.py          # Event simulator (runs continuously)
│   └── consume.py           # Kafka consumer
├── templates/               # HTML templates
├── docker-compose.yml       # Docker services
└── pyproject.toml           # Python dependencies
```

## Tech Stack

- **Backend**: Flask (Python 3.14+)
- **Database**: ClickHouse
- **Message Queue**: Kafka
- **Frontend**: HTML5, CSS3, JavaScript, ECharts
- **Containerization**: Docker & Docker Compose
- **Package Manager**: [uv](https://github.com/astral-sh/uv)

## Quick Start

### Prerequisites

- Docker and Docker Compose
- [uv](https://github.com/astral-sh/uv) installed

### Installation & Running

1. **Start Infrastructure**:
   ```bash
   docker-compose up -d
   ```

2. **Install Dependencies**:
   ```bash
   uv sync
   ```

3. **Run Data Simulator** (generate events to Kafka, runs continuously):
   ```bash
   uv run scripts/simulate.py
   ```
   - Generates ~500 events per hour with realistic 5-20 second intervals
   - Simulates real player behavior patterns

4. **Run Kafka Consumer** (write to ClickHouse):
   ```bash
   uv run scripts/consume.py
   ```

5. **Start Flask Application**:
   ```bash
   uv run main.py
   ```

6. **Access the Dashboard**:
   Open your browser and navigate to `http://127.0.0.1:5000`

## API Endpoints

### Overview
- `GET /`: The analytics dashboard
- `GET /api/overview`: Returns daily statistics (DAU, matches, revenue)
- `GET /api/level-distribution`: Returns player level distribution data

### Retention Analytics
- `GET /api/retention/<date>`: Get retention data for a specific date
  - Returns: total_new_users, day1_retained, day3_retained, day7_retained, retention rates
  - Example: `/api/retention/2026-03-30`
- `GET /api/retention/trend?days=7`: Get daily retention trend for recent days

### SQL Console
- `POST /api/query-sql`: Execute custom SQL queries
  - Body: `{"sql": "SELECT * FROM game_events LIMIT 10"}`

## Dashboard Features

### 1. Real-time Metrics
- Daily Active Users (DAU)
- Match counts and revenue
- Skin sales tracking

### 2. Retention Analytics
- **Statistics Cards**: Yesterday's new users and retention rates (day-1, day-3, day-7)
- **Trend Chart**: 7-day retention trend with dual Y-axes (retention rate + new users)

### 3. SQL Console
- Execute custom ClickHouse queries
- View results in formatted tables
- Supports all ClickHouse SQL features

## Data Generation

The simulator (`scripts/simulate.py`) runs continuously and:
- Generates ~500 events per hour
- Uses realistic intervals (5-20 seconds between events)
- Simulates player state transitions (offline → online → in-match → offline)
- Produces diverse event types: login, match_start, match_end, skin_buy, battle_pass_buy

## Environment Variables

- `CLICKHOUSE_HOST` - ClickHouse host (default: localhost)
- `CLICKHOUSE_PORT` - ClickHouse port (default: 8123)
- `CLICKHOUSE_DATABASE` - Database name (default: game)
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka brokers (default: localhost:9092)
- `FLASK_PORT` - Flask server port (default: 5000)
- `FLASK_DEBUG` - Debug mode (default: true)

## Architecture

- **Models**: Data model definitions (Event dataclass)
- **Repositories**: ClickHouse data access with singleton pattern and retention queries
- **Services**: Business logic layer with event simulation
- **Routes**: Flask blueprint-based API routes

## Data Flow

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐
│   Simulator     │────▶│    Kafka    │────▶│    Consumer     │
│  (Continuous)   │     │   (Queue)   │     │  (ClickHouse)   │
└─────────────────┘     └─────────────┘     └─────────────────┘
                                                      │
                                                      ▼
┌─────────────────────────────────────────────────────────────┐
│                      ClickHouse Database                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ game_events │  │    users    │  │ users_auto_mv (MV)  │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                      │
                      ▼
            ┌─────────────────┐
            │  Flask Backend  │
            └─────────────────┘
                      │
                      ▼
            ┌─────────────────┐
            │   Dashboard UI  │
            └─────────────────┘
```

## License

MIT License
