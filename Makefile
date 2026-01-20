.PHONY: local prod env run db-up db-down db-reset db-seed install test

# Environment switching
local:
	@cp .env.local .env
	@echo "Switched to local PostgreSQL environment"

prod:
	@cp .env.prod .env
	@echo "Switched to production Supabase environment"

env:
	@echo "Current environment:"
	@grep -E "^DB_MODE=" .env 2>/dev/null || echo "DB_MODE not set"

# Database commands (local only)
db-up:
	docker compose up -d
	@echo "PostgreSQL is running on localhost:5432"

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d
	@echo "Database reset complete"

db-logs:
	docker compose logs -f postgres

db-seed:
	python scripts/setup_local_db.py --seed

db-shell:
	docker compose exec postgres psql -U polymarket -d polymarket

# Application commands
install:
	pip install -e ".[dev,local]"

run:
	python src/main.py

test:
	pytest tests/ -v

# Combined workflows
local-setup: local db-up
	@sleep 2
	python scripts/setup_local_db.py --seed
	@echo "\nLocal environment ready! Run 'make run' to start."
