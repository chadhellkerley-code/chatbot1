from src.playwright_service import resolve_playwright_executable
from src.runtime.playwright_runtime import launch_sync_browser

def check():
    print("Iniciando Playwright check...")
    try:
        print("Lanzando navegador...")
        executable = resolve_playwright_executable(headless=True)
        browser = launch_sync_browser(
            headless=True,
            executable_path=executable,
            visible_reason="check_playwright",
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
