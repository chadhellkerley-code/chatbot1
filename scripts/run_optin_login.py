import argparse
from getpass import getpass

from optin_browser import login


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Realiza login con Playwright y guarda la sesión.")
    parser.add_argument("--account", required=True, help="Alias de la cuenta para guardar la sesión.")
    parser.add_argument("--user", required=True, help="Usuario de Instagram.")
    parser.add_argument("--password", help="Contraseña (si no se provee se pedirá de forma oculta).")
    parser.add_argument("--totp", help="Secreto TOTP para generar códigos de autenticación.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    password = args.password or getpass("Contraseña: ")
    login.cli_login(account=args.account, username=args.user, password=password, totp_secret=args.totp)


if __name__ == "__main__":
    main()
