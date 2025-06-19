# Makefile for Conda Environment Setup

# Variables
CONDA_ENV_NAME ?= "Deal_Predection"
PYTHON_VERSION ?= 3.12

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

# Default target
.DEFAULT_GOAL := help

# Check if conda environment exists
CONDA_ENV_EXISTS := $(shell conda info --envs | grep -w $(CONDA_ENV_NAME))

# Check if environment exists before running commands that need it
define require_env
	@if [ -z "$(shell conda info --envs | grep -w $(CONDA_ENV_NAME))" ]; then \
		echo "$(RED)Error: Conda environment '$(CONDA_ENV_NAME)' does not exist.$(NC)"; \
		echo "$(YELLOW)Run 'make env-setup' first.$(NC)"; \
		exit 1; \
	fi
endef

# ==================== ENVIRONMENT SETUP ====================

.PHONY: env-setup
env-setup: ## Create environment if needed and show activation command
	@if [ -z "$(CONDA_ENV_EXISTS)" ]; then \
		echo "$(YELLOW)Environment '$(CONDA_ENV_NAME)' does not exist. Creating...$(NC)"; \
		conda create -n $(CONDA_ENV_NAME) python=$(PYTHON_VERSION) -y; \
		echo "$(GREEN)Environment created successfully!$(NC)"; \
	else \
		echo "$(GREEN)Environment '$(CONDA_ENV_NAME)' already exists.$(NC)"; \
	fi
	@echo "$(GREEN)To activate the environment, run:$(NC)"
	@echo "conda activate $(CONDA_ENV_NAME)"

.PHONY: env-create
env-create: ## Create conda environment (alternative to env-setup)
	@if [ -n "$(CONDA_ENV_EXISTS)" ]; then \
		echo "$(YELLOW)Environment '$(CONDA_ENV_NAME)' already exists.$(NC)"; \
	else \
		echo "$(GREEN)Creating conda environment '$(CONDA_ENV_NAME)'...$(NC)"; \
		conda create -n $(CONDA_ENV_NAME) python=$(PYTHON_VERSION) -y; \
	fi

.PHONY: env-remove
env-remove: ## Remove conda environment
	@echo "$(YELLOW)Removing conda environment '$(CONDA_ENV_NAME)'...$(NC)"
	conda env remove -n $(CONDA_ENV_NAME) -y

.PHONY: env-info
env-info: ## Show conda environment info
	@echo "$(GREEN)Available conda environments:$(NC)"
	conda info --envs
	@echo ""
	@echo "Current environment: $${CONDA_DEFAULT_ENV:-base}"

# ==================== POETRY SETUP ====================

.PHONY: poetry-install
poetry-install: ## Install poetry in conda environment
	$(require_env)
	@echo "$(GREEN)Installing Poetry in conda environment...$(NC)"
	pip install poetry

.PHONY: poetry-init
poetry-init: ## Initialize poetry project
	$(require_env)
	@echo "$(GREEN)Initializing Poetry project...$(NC)"
	poetry init

.PHONY: poetry-config
poetry-config: ## Configure poetry to use conda environment
	$(require_env)
	@echo "$(GREEN)Configuring Poetry to use conda environment...$(NC)"
	conda run -n $(CONDA_ENV_NAME) poetry config virtualenvs.create false
	conda run -n $(CONDA_ENV_NAME) poetry env use $(conda run -n $(CONDA_ENV_NAME) which python)

# ==================== DEPENDENCIES ====================

.PHONY: install
install: ## Install project dependencies
	$(require_env)
	@echo "$(GREEN)Installing dependencies...$(NC)"
	conda run -n $(CONDA_ENV_NAME) poetry install

.PHONY: install-dev
install-dev: ## Install all dependencies including development
	$(require_env)
	@echo "$(GREEN)Installing all dependencies...$(NC)"
	conda run -n $(CONDA_ENV_NAME) poetry install --with dev

# ==================== QUICK SETUP ====================

.PHONY: setup
setup: env-setup  ## Complete environment setup
	@echo "$(GREEN)Environment setup complete!$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. Activate environment: conda activate $(CONDA_ENV_NAME)"
	@echo "  2. Start coding!"

.PHONY: setup-dev
setup-dev: env-setup poetry-install poetry-config install-dev ## Complete development environment setup
	@echo "$(GREEN)Development environment setup complete!$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. Activate environment: conda activate $(CONDA_ENV_NAME)"
	@echo "  2. Start coding!"

# ==================== STATUS & INFO ====================

.PHONY: status
status: ## Show environment status
	@echo "$(GREEN)=== Environment Status ===$(NC)"
	@echo "Project: $(shell basename $(CURDIR))"
	@echo "Environment: $(CONDA_ENV_NAME)"
	@echo "Python: $(PYTHON_VERSION)"
	@echo ""
	@if [ -n "$(CONDA_ENV_EXISTS)" ]; then \
		echo "$(GREEN)✓ Conda environment exists$(NC)"; \
		if conda run -n $(CONDA_ENV_NAME) poetry --version >/dev/null 2>&1; then \
			echo "$(GREEN)✓ Poetry configured$(NC)"; \
		else \
			echo "$(YELLOW)⚠ Poetry not installed$(NC)"; \
		fi; \
	else \
		echo "$(RED)✗ Conda environment missing$(NC)"; \
	fi

# ==================== EXPORT ====================

.PHONY: export-env
export-env: ## Export conda environment to environment.yml
	$(require_env)
	@echo "$(GREEN)Exporting conda environment...$(NC)"
	conda env export -n $(CONDA_ENV_NAME) > environment.yml

.PHONY: export-requirements
export-requirements: ## Export poetry requirements to requirements.txt
	$(require_env)
	@echo "$(GREEN)Exporting poetry requirements...$(NC)"
	conda run -n $(CONDA_ENV_NAME) poetry export -f requirements.txt --output requirements.txt

# ==================== HELP ====================

.PHONY: help
help: ## Show this help message
	@echo "$(GREEN)Conda Environment Setup$(NC)"
	@echo ""
	@echo "$(YELLOW)Usage:$(NC) make [target]"
	@echo ""
	@echo "$(YELLOW)Targets:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-18s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)