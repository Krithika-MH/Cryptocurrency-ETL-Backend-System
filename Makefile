.PHONY: up down test clean logs restart help

help:
	@echo "Kasparro Backend - Available Commands:"
	@echo "  make up       - Start all services (PostgreSQL + API)"
	@echo "  make down     - Stop all services"
	@echo "  make test     - Run test suite"
	@echo "  make clean    - Remove all containers and volumes"
	@echo "  make logs     - View service logs"
	@echo "  make restart  - Restart all services"

up:
	@echo " Starting Kasparro Backend..."
	docker-compose up -d
	@echo " Services started successfully!"
	@echo ""
	@echo " Access points:"
	@echo "   API:          http://localhost:8000"
	@echo "   API Docs:     http://localhost:8000/docs"
	@echo "   Health Check: http://localhost:8000/health"
	@echo ""
	@echo " View logs: make logs"

down:
	@echo " Stopping all services..."
	docker-compose down
	@echo " Services stopped"

test:
	@echo " Running test suite..."
	docker-compose up -d postgres
	@echo " Waiting for database to be ready..."
	@sleep 5
	pytest tests/ -v --tb=short
	@echo " Tests completed"

clean:
	@echo " Cleaning up containers and volumes..."
	docker-compose down -v
	docker system prune -f
	@echo " Cleanup completed"

logs:
	docker-compose logs -f

restart:
	@echo " Restarting services..."
	docker-compose restart
	@echo " Services restarted"
