"""Garante que o repo root está no sys.path para `from ingestion... import`
e `from transform... import` funcionarem.

`pytest` (o console script, diferente de `python -m pytest`) não insere o
diretório de trabalho no sys.path automaticamente -- sem isso, `pytest
tests/ -v` a partir da raiz do repo falha com `ModuleNotFoundError:
ingestion`/`transform`, mesmo estando tudo no lugar certo."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
