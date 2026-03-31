import json
import os
import sys
from pathlib import Path

from paths import accounts_root

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.auth.onboarding import onboard_accounts_from_csv  # noqa: E402


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Uso: python scripts/onboard_from_csv.py <ruta_csv>")

    csv_path = sys.argv[1]
    results = onboard_accounts_from_csv(csv_path, headless=True, concurrency=2)
    print(json.dumps(results, ensure_ascii=False, indent=2))
    print(f"Resultados guardados en {accounts_root(BASE_DIR) / 'onboarding_results.csv'}")


if __name__ == "__main__":
    main()
