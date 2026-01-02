# test_config.py
from pathlib import Path
import os
from config import Config

print("Current dir:", os.getcwd())
print("Script location:", Path(__file__).parent)
print("Parent:", Path(__file__).parent.parent)
print("Env file:", Path(__file__).parent.parent / '.env')
print("Env exists:", (Path(__file__).parent.parent / '.env').exists())

print("LLM_API_KEY loaded:", bool(Config.LLM_API_KEY))