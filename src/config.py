import os

SAVE_DIR = os.path.join(os.path.dirname(__file__), "../data")
os.makedirs(SAVE_DIR, exist_ok=True)