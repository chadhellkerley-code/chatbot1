from playwright.sync_api import sync_playwright
from src.playwright_service import resolve_playwright_executable

def check():
    print("Iniciando Playwright check...")
    try:
        with sync_playwright() as p:
            print("Lanzando navegador...")
            executable = resolve_playwright_executable(headless=True)
            browser = p.chromium.launch(
                headless=True,
                executable_path=str(executable) if executable else None,
            )
            print("Navegador lanzado OK")
            page = browser.new_page()
            page.goto("https://google.com")
            print(f"Page title: {page.title()}")
            browser.close()
            print("Playwright funciona correctamente.")
    except Exception as e:
        print(f"ERROR CRÍTICO PLAYWRIGHT: {e}")

if __name__ == "__main__":
    check()
