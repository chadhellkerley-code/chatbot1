import argparse

from optin_browser import dm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enviar un DM usando la sesión guardada.")
    parser.add_argument("--account", required=True, help="Alias de la cuenta con sesión guardada.")
    parser.add_argument("--to", required=True, help="Usuario destino (username de Instagram).")
    parser.add_argument("--text", help="Texto del mensaje, si se omite se pedirá manualmente.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    text = args.text or input("Mensaje a enviar: ")
    dm.cli_send_dm(account=args.account, to_username=args.to, message=text)


if __name__ == "__main__":
    main()
