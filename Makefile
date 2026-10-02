.PHONY: init
init: # Create virtual environment and install all requirements
	uv venv --clear
	uv sync

.PHONY: jupyter
jupyter:
	uv run jupyter lab
