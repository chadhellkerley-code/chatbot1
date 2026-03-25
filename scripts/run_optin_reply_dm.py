import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.opt_in.browser_tools import replies


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Responder mensajes no leídos.")
    parser.add_argument("--account", required=True, help="Alias de la cuenta con sesión guardada.")
    parser.add_argument("--reply", required=True, help="Respuesta a enviar en los chats no leídos.")
    parser.add_argument("--limit", type=int, default=5, help="Cantidad máxima de respuestas (default 5).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    total = replies.reply_unread(account=args.account, text=args.reply, limit=args.limit)
    print(f"Total respondido: {total}")


if __name__ == "__main__":
    main()
