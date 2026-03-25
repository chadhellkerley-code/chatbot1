# scripts/smoke_playwright_login.py
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import os
from src.playwright_service import launch_persistent, get_page, shutdown


if __name__ == "__main__":
    account_id = os.getenv("TEST_ACCOUNT_ID", "demo_account")
    pw, ctx = launch_persistent(account_id, proxy=None, headful=True)
    page = get_page(ctx)
    page.goto("https://www.instagram.com/")
    print("OK: se abrió Instagram con perfil persistente:", account_id)
    shutdown(pw, ctx)
