#! /usr/bin/env bash

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Quarto
curl -LO https://quarto.org/download/latest/quarto-linux-arm64.deb && sudo dpkg -i quarto-linux-arm64.deb && rm quarto-linux-arm64.deb
# Install Dependencies
uv sync

# Install pre-commit hooks
uv run pre-commit install --install-hooks
