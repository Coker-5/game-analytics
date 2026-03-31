# AGENTS.md - Game Analytics Dashboard

Guidelines for AI coding agents working in this repository.

## Project Overview

A real-time game data analytics platform with:
- **Python (Flask)**: Web API and dashboard backend
- **Database**: ClickHouse for analytics storage
- **Message Queue**: Kafka for event streaming
- **Frontend**: HTML/CSS/JS with ECharts visualization

## Build & Run Commands

### Python (Primary Stack)

```bash
# Install dependencies
uv sync

# Run Flask application
uv run main.py

# Run data simulator (generates events to Kafka continuously)
uv run scripts/simulate.py

# Run Kafka consumer (writes to ClickHouse)
uv run scripts/consume.py
```

### Infrastructure

```bash
# Start ClickHouse and Kafka
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f
```

### Testing

```bash
# Run all tests (when tests are added)
uv run pytest

# Run a single test file
uv run pytest tests/test_file.py

# Run a specific test function
uv run pytest tests/test_file.py::test_function_name

# Run tests with coverage
uv run pytest --cov=game_analytics
```

### Code Quality

```bash
# Format code with ruff (if available)
uv run ruff format .

# Check code style
uv run ruff check .

# Auto-fix style issues
uv run ruff check . --fix
```

## Code Style Guidelines

### Python

- **Python version**: 3.14+
- **Package manager**: uv (not pip) - use `uv sync` and `uv run`
- **Imports order**:
  1. Standard library imports (e.g., `json`, `datetime`, `pathlib`)
  2. Third-party imports (e.g., `flask`, `kafka`, `clickhouse_connect`)
  3. Local imports (e.g., `from game_analytics.models import Event`)
  - Use blank lines between each group
- **Formatting**: Follow PEP 8, use 4-space indentation
- **Quotes**: Use double quotes for strings, single quotes acceptable for internal use
- **Functions**: Use snake_case
- **Classes**: Use PascalCase
- **Constants**: UPPER_CASE at module level (e.g., `SERVERS`, `RANKS`, `DEVICES`)
- **Comments**: Use Chinese comments for business logic, English for technical notes
- **Error handling**: Use try/except blocks with specific error messages
  - Always catch specific exceptions, not bare `except:`
  - Log errors with context using print() or logging
- **Flask routes**: Use @app.route decorators, return jsonify responses
- **API responses**: Use make_response() helper for standardized format (code, data, msg)
- **Dataclasses**: Use @dataclass for data models (Event, Player, etc.)
- **Type hints**: Optional but encouraged, especially for function return types

### General

- Keep constants in UPPER_CASE (SERVERS, RANKS, DEVICES, etc.)
- Use dataclasses for data models with proper type annotations
- Prefer explicit error handling over silent failures
- Use meaningful variable names, avoid abbreviations unless obvious
- Use pathlib.Path for file operations
- Follow singleton pattern for repository classes

## Testing

No formal test suite is currently configured. Manual testing via:
1. Run `docker-compose up -d`
2. Start Flask app: `uv run main.py`
3. Generate events: `uv run scripts/simulate.py`
4. Access dashboard at `http://127.0.0.1:5000`
5. Verify API endpoints:
   - `curl http://127.0.0.1:5000/api/overview`
   - `curl http://127.0.0.1:5000/api/level-distribution`
   - `curl http://127.0.0.1:5000/api/retention/2026-03-30`
   - `curl "http://127.0.0.1:5000/api/retention/trend?days=7"`

## API Endpoints

- `GET /` - Dashboard UI (renders index.html)
- `GET /api/overview` - Daily statistics (DAU, matches, revenue)
- `GET /api/level-distribution` - Player level distribution
- `GET /api/retention/<date>` - Retention data for specific date (e.g., 2026-03-30)
- `GET /api/retention/trend?days=7` - Retention trend for recent days
- `POST /api/query-sql` - Execute custom SQL queries
  - Body: `{"sql": "SELECT * FROM game_events LIMIT 10"}`
  - Returns: `{code, data: {columns, rows}, msg}`

## Project Structure

```
/Users/zyq/my-projects/Playsight/
├── game_analytics/       # Main Flask package
│   ├── __init__.py      # Package init with create_app
│   ├── app.py           # Flask app factory, make_response helper
│   ├── config.py        # Configuration class
│   ├── models/          # Data models (Event dataclass)
│   ├── repositories/    # ClickHouse data access (singleton pattern)
│   ├── services/        # Business logic (AnalyticsService, EventSimulator)
│   └── routes/          # API routes (blueprints)
│       ├── overview.py  # Dashboard and overview API
│       ├── distribution.py  # Level distribution API
│       ├── retention.py # Retention analytics API
│       └── query.py     # SQL console API
├── scripts/             # Executable scripts
│   ├── simulate.py      # Event simulator (runs 24/7)
│   └── consume.py       # Kafka consumer
├── main.py              # Application entry point
├── templates/           # HTML templates
│   └── index.html       # Dashboard UI
├── docker-compose.yml   # ClickHouse + Kafka services
├── pyproject.toml       # Python dependencies (uv)
├── requirements.txt     # Additional deps (kafka-python-ng)
└── uv.lock             # Locked dependencies
```

## Dependencies

### Python (pyproject.toml)
- clickhouse-connect >= 0.14.0
- flask >= 3.1.3
- kafka-python-ng

### Infrastructure
- ClickHouse Server 23.x
- Apache Kafka (latest)
- Docker & Docker Compose

## Environment Variables

- `CLICKHOUSE_HOST` - ClickHouse host (default: localhost)
- `CLICKHOUSE_PORT` - ClickHouse port (default: 8123)
- `CLICKHOUSE_DATABASE` - Database name (default: game)
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka brokers (default: localhost:9092)
- `KAFKA_TOPIC_EVENTS` - Kafka topic (default: tp_game_events)
- `FLASK_PORT` - Flask port (default: 5000)
- `FLASK_DEBUG` - Debug mode (default: true)
- `SIMULATE_SPEED_UP` - Simulation speed multiplier (default: 60)

## Git

- No pre-commit hooks configured
- Standard .gitignore for Python, Java, IDE files, and OS files
- Commit messages: Use English, describe the "why" not just the "what"
- Follow conventional commits format if possible
