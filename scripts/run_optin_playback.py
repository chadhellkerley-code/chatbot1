import argparse

from optin_browser import playback
from optin_browser.utils import pairs_from_cli


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reproducir un flujo grabado.")
    parser.add_argument("--alias", required=True, help="Nombre del flujo a reproducir.")
    parser.add_argument("--account", help="Alias de la cuenta que usará la sesión guardada.")
    parser.add_argument("--var", action="append", default=[], help="Variables para reemplazar placeholders (KEY=VALUE).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    variables, _ = pairs_from_cli(args.var)
    playback.cli_play(alias=args.alias, variables=variables, account=args.account)


if __name__ == "__main__":
    main()
