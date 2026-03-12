import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.opt_in.browser_tools import recorder


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Grabar un flujo manual una sola vez.")
    parser.add_argument("--alias", required=True, help="Nombre del flujo a guardar.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    recorder.cli_record(alias=args.alias)


if __name__ == "__main__":
    main()
