from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import random
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from playwright.async_api import async_playwright

from src.actions.dm_actions import send_dm_to_user, pick_random_message
from src.playwright_service import (
    BASE_PROFILES,
    DEFAULT_ARGS,
    DEFAULT_TIMEZONE,
    DEFAULT_USER_AGENT,
    DEFAULT_VIEWPORT,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def load_accounts(path: str) -> List[Dict]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_usernames(path: str) -> List[str]:
    p = Path(path)
    if p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        return [u.strip() for u in data.get("usernames", []) if u.strip()]
    if p.suffix.lower() == ".csv":
        with p.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [row["username"].strip() for row in reader if row.get("username")]
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def load_messages(path: str) -> List[str]:
    p = Path(path)
    if p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        return [m.strip() for m in data if m.strip()]
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def round_robin_split(items: List[str], buckets: int) -> List[List[str]]:
    if buckets <= 0:
        return []
    parts = [[] for _ in range(buckets)]
    for idx, item in enumerate(items):
        parts[idx % buckets].append(item)
    return parts


def build_proxy(proxy_cfg):
    if not proxy_cfg:
        return None
    if isinstance(proxy_cfg, dict):
        return proxy_cfg
    if isinstance(proxy_cfg, str):
        return {"server": proxy_cfg}
    return None


@asynccontextmanager
async def account_context(account_cfg: Dict):
    username = account_cfg["username"]
    headless = False  # batch send: force headful for debugging/monitoring
    proxy = build_proxy(account_cfg.get("proxy"))
    pw = await async_playwright().start()
    profile_dir = BASE_PROFILES / username
    profile_dir.mkdir(parents=True, exist_ok=True)
    context = await pw.chromium.launch_persistent_context(
        user_data_dir=str(profile_dir),
        headless=headless,
        proxy=proxy,
        viewport=DEFAULT_VIEWPORT,
        user_agent=DEFAULT_USER_AGENT,
        locale="en-US",
        timezone_id=DEFAULT_TIMEZONE,
        args=DEFAULT_ARGS,
    )
    try:
        yield context
    finally:
        await context.close()
        await pw.stop()


async def run_for_account(
    account_cfg: Dict,
    usernames: List[str],
    messages: List[str],
    outdir: Path,
    time_interval: float,
    max_messages: int,
) -> None:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir.mkdir(parents=True, exist_ok=True)
    outfile = outdir / f"send_status_{account_cfg['username']}_{stamp}.csv"
    with outfile.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["account", "username", "sent_text", "status", "error", "started_at", "ended_at", "elapsed_ms"])

        async with account_context(account_cfg) as ctx:
            page = await ctx.new_page()
            sent_count = 0
            for username in usernames:
                if max_messages and sent_count >= max_messages:
                    break
                template = random.choice(messages)
                t0 = time.time()
                started = datetime.now().isoformat()
                result = await send_dm_to_user(page, username, template, {"typing": {"min_delay": 0.04, "max_delay": 0.18}})
                ended = datetime.now().isoformat()
                elapsed = int((time.time() - t0) * 1000)
                status = "Completed" if result.get("ok") else "Failed"
                error = result.get("error", "")
                writer.writerow([account_cfg["username"], username, template, status, error, started, ended, elapsed])
                f.flush()
                logging.info("[%s] %s: %s (%s ms) %s", account_cfg["username"], username, status, elapsed, error)
                if not result.get("ok") and result.get("screenshot"):
                    logging.info("[%s] Screenshot: %s", account_cfg["username"], result["screenshot"])
                if result.get("ok"):
                    sent_count += 1
                else:
                    # retry once on transient errors
                    if isinstance(error, str) and (error in {"timeout", "send_failed"} or error.startswith("exception")):
                        await asyncio.sleep(2.0)
                        retry = await send_dm_to_user(
                            page,
                            username,
                            template,
                            {"typing": {"min_delay": 0.04, "max_delay": 0.18}},
                        )
                        status = "Completed" if retry.get("ok") else "Failed"
                        error = retry.get("error", error)
                        writer.writerow([account_cfg["username"], username, template, status, error, started, ended, elapsed])
                        f.flush()
                        logging.info(
                            "[%s] Retry %s: %s %s",
                            account_cfg["username"],
                            username,
                            status,
                            error,
                        )
                        if not retry.get("ok") and retry.get("screenshot"):
                            logging.info("[%s] Screenshot: %s", account_cfg["username"], retry["screenshot"])
                        if retry.get("ok"):
                            sent_count += 1
                            continue
                base_interval = time_interval or 10.0
                jitter = random.uniform(0.85, 1.35)
                await asyncio.sleep(base_interval * jitter)


async def main() -> int:
    ap = argparse.ArgumentParser(description="Envio masivo de DMs con múltiples cuentas.")
    ap.add_argument("--accounts", default="accounts.json")
    ap.add_argument("--recipients", required=True)
    ap.add_argument("--messages", required=True)
    ap.add_argument("--outdir", default="out")
    ap.add_argument("--max-concurrent", type=int, default=2)
    ap.add_argument("--per-account-interval", type=float, default=12.0)
    ap.add_argument("--per-account-max", type=int, default=0)
    args = ap.parse_args()

    accounts = load_accounts(args.accounts)
    usernames = load_usernames(args.recipients)
    messages = load_messages(args.messages)
    if not accounts or not usernames or not messages:
        raise SystemExit("Se requieren accounts, recipients y messages.")

    parts = round_robin_split(usernames, len(accounts))
    outdir = Path(args.outdir)
    semaphore = asyncio.Semaphore(max(1, args.max_concurrent))

    async def runner(acc_cfg, users):
        async with semaphore:
            await run_for_account(
                account_cfg=acc_cfg,
                usernames=users,
                messages=messages,
                outdir=outdir,
                time_interval=args.per_account_interval,
                max_messages=acc_cfg.get("max_messages", args.per_account_max),
            )

    await asyncio.gather(*(runner(acc, users) for acc, users in zip(accounts, parts)))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
