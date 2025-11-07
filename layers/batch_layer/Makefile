# ========================================
# Batch Layer Makefile
# ========================================
# Commands for managing Docker Compose stack
# ========================================

.PHONY: help up down restart logs ps clean build test backfill ui

# Default target
.DEFAULT_GOAL := help

# Colors
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Docker Compose file
COMPOSE_FILE := docker-compose.batch.yml
PROJECT_NAME := batch_layer

# Docker command (use sudo if needed)
DOCKER := $(shell if docker ps >/dev/null 2>&1; then echo "docker"; else echo "sudo docker"; fi)
DOCKER_COMPOSE := $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; else echo "sudo docker compose"; fi)

help: ## Show this help message
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(GREEN)Batch Layer - Docker Commands$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

preflight: ## Run pre-flight checks before starting
	@./preflight-check.sh

smoke-test: ## Run smoke tests to verify services are working
	@./smoke-test.sh

up: ## Start all services
	@echo "$(GREEN)Starting Batch Layer services...$(NC)"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d
	@echo "$(GREEN)Services started!$(NC)"
	@echo "$(YELLOW)Waiting for services to be healthy...$(NC)"
	@sleep 20
	@$(MAKE) ps
	@$(MAKE) ui

down: ## Stop and remove all services
	@echo "$(RED)Stopping Batch Layer services...$(NC)"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down
	@echo "$(GREEN)Services stopped!$(NC)"

down-v: ## Stop and remove all services with volumes
	@echo "$(RED)Stopping Batch Layer services and removing volumes...$(NC)"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down -v
	@echo "$(GREEN)Services stopped and volumes removed!$(NC)"

restart: ## Restart all services
	@echo "$(YELLOW)Restarting Batch Layer services...$(NC)"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) restart
	@echo "$(GREEN)Services restarted!$(NC)"

logs: ## Follow logs for all services
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f

logs-airflow: ## Follow Airflow logs
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f airflow-webserver airflow-scheduler

logs-spark: ## Follow Spark logs
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f spark-master spark-worker-1 spark-worker-2

logs-hdfs: ## Follow HDFS logs
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f namenode datanode1 datanode2 datanode3

logs-mongo: ## Follow MongoDB logs
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f mongo

ps: ## Show service status
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(GREEN)Service Status$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) ps
	@echo ""

clean: down-v ## Clean all containers, volumes, and networks
	@echo "$(RED)Cleaning up Docker resources...$(NC)"
	$(DOCKER) system prune -f
	@echo "$(GREEN)Cleanup complete!$(NC)"

build: ## Build all images
	@echo "$(GREEN)Building Docker images...$(NC)"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) build --no-cache
	@echo "$(GREEN)Build complete!$(NC)"

rebuild: build up ## Rebuild and start services

ui: ## Show service URLs
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(GREEN)Service URLs$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(YELLOW)Airflow UI:$(NC)        http://localhost:8080"
	@echo "                      User: admin | Pass: admin"
	@echo "$(YELLOW)Spark Master UI:$(NC)   http://localhost:8081"
	@echo "$(YELLOW)HDFS Namenode UI:$(NC)  http://localhost:9870"
	@echo "$(YELLOW)Mongo Express:$(NC)     http://localhost:8082"
	@echo "                      User: admin | Pass: admin"
	@echo "$(YELLOW)MongoDB:$(NC)           mongodb://localhost:27017"
	@echo "                      User: admin | Pass: password"
	@echo "$(BLUE)========================================$(NC)"
	@echo ""

test-connection: ## Test connectivity to all services
	@echo "$(BLUE)Testing service connectivity...$(NC)"
	@echo "$(YELLOW)HDFS Namenode:$(NC)"
	@curl -s -o /dev/null -w "  Status: %{http_code}\n" http://localhost:9870 || echo "  $(RED)Failed$(NC)"
	@echo "$(YELLOW)Spark Master:$(NC)"
	@curl -s -o /dev/null -w "  Status: %{http_code}\n" http://localhost:8081 || echo "  $(RED)Failed$(NC)"
	@echo "$(YELLOW)Airflow:$(NC)"
	@curl -s -o /dev/null -w "  Status: %{http_code}\n" http://localhost:8080/health || echo "  $(RED)Failed$(NC)"
	@echo "$(YELLOW)Mongo Express:$(NC)"
	@curl -s -o /dev/null -w "  Status: %{http_code}\n" http://localhost:8082 || echo "  $(RED)Failed$(NC)"
	@echo "$(GREEN)Connection test complete!$(NC)"

backfill: ## Run backfill for a date range (make backfill START=2024-01-01 END=2024-01-31)
	@if [ -z "$(START)" ] || [ -z "$(END)" ]; then \
		echo "$(RED)Error: START and END dates are required$(NC)"; \
		echo "$(YELLOW)Usage: make backfill START=YYYY-MM-DD END=YYYY-MM-DD$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Running backfill from $(START) to $(END)...$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec spark-master python /app/layers/batch_layer/master_dataset/ingestion.py \
		--start-date $(START) \
		--end-date $(END) \
		--pages 50
	@echo "$(GREEN)Backfill complete!$(NC)"

trigger-dag: ## Trigger the TMDB batch pipeline DAG in Airflow
	@echo "$(GREEN)Triggering tmdb_batch_pipeline DAG...$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec airflow-webserver airflow dags trigger tmdb_batch_pipeline
	@echo "$(GREEN)DAG triggered! Check Airflow UI for progress.$(NC)"

list-dags: ## List all available DAGs
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec airflow-webserver airflow dags list

hdfs-ls: ## List HDFS directories
	@echo "$(BLUE)HDFS Directory Structure:$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec namenode hdfs dfs -ls / || echo "$(YELLOW)HDFS not ready yet$(NC)"
	@echo ""
	@echo "$(YELLOW)Bronze Layer:$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec namenode hdfs dfs -ls /data/bronze 2>/dev/null || echo "  No data yet"
	@echo ""
	@echo "$(YELLOW)Silver Layer:$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec namenode hdfs dfs -ls /data/silver 2>/dev/null || echo "  No data yet"
	@echo ""
	@echo "$(YELLOW)Gold Layer:$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec namenode hdfs dfs -ls /data/gold 2>/dev/null || echo "  No data yet"

hdfs-create-dirs: ## Create HDFS directories
	@echo "$(GREEN)Creating HDFS directories...$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec namenode hdfs dfs -mkdir -p /data/bronze
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec namenode hdfs dfs -mkdir -p /data/silver
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec namenode hdfs dfs -mkdir -p /data/gold
	@echo "$(GREEN)Directories created!$(NC)"

mongo-shell: ## Open MongoDB shell
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec mongo mongosh -u admin -p password --authenticationDatabase admin moviedb

mongo-query-views: ## Query batch_views collection
	@echo "$(BLUE)Querying batch_views collection...$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec mongo mongosh -u admin -p password --authenticationDatabase admin moviedb \
		--eval "db.batch_views.countDocuments()"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec mongo mongosh -u admin -p password --authenticationDatabase admin moviedb \
		--eval "db.batch_views.find().limit(5).pretty()"

spark-shell: ## Open Spark shell
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec spark-master /opt/spark/bin/spark-shell

airflow-shell: ## Open Airflow container shell
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec airflow-webserver bash

spark-master-shell: ## Open Spark master container shell
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) exec spark-master bash

health: ## Check health of all services
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(GREEN)Service Health Status$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
	@echo ""

validate: ## Validate the Docker Compose file
	@echo "$(GREEN)Validating Docker Compose configuration...$(NC)"
	@$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) config > /dev/null && echo "$(GREEN)✓ Configuration is valid$(NC)" || echo "$(RED)✗ Configuration has errors$(NC)"
