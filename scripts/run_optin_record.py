import argparse

from optin_browser import recorder


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Grabar un flujo manual una sola vez.")
    parser.add_argument("--alias", required=True, help="Nombre del flujo a guardar.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    recorder.cli_record(alias=args.alias)


if __name__ == "__main__":
    main()
