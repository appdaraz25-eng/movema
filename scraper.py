import aiohttp
import asyncio
import os
import re
import time
from bs4 import BeautifulSoup

BASE_URL = "https://bollydrive.mom/new/file/{}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://bollydrive.mom/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.5",
}

# ─── Tier config ─────────────────────────────────────────────────────────────
TIERS = [
    {"name": "FAST",   "size": 10_000, "concurrency": 30, "delay": 0.0},
    {"name": "MEDIUM", "size": 10_000, "concurrency": 15, "delay": 0.1},
    {"name": "SLOW",   "size": 10_000, "concurrency":  5, "delay": 0.3},
]

FOUND_FILE    = "1found.txt"
NOTFOUND_FILE = "notfound.txt"
ERROR_FILE    = "error.txt"

ALL_FILES = [FOUND_FILE, NOTFOUND_FILE, ERROR_FILE]

# ─── Block-detection thresholds ──────────────────────────────────────────────
BLOCK_WINDOW     = 50
BLOCK_ERROR_RATE = 0.80
BLOCK_CODES      = {403, 429, 503}

# ─── ID regex: first token before ' | ' or end-of-line ───────────────────────
_ID_RE = re.compile(r"^(\d+)")


# ════════════════════════════════════════════════════════════════════════════
# Deduplication — load ALL seen serial numbers from ALL txt files at startup
# ════════════════════════════════════════════════════════════════════════════

def _load_seen_ids() -> set[int]:
    """
    Read every existing .txt output file and collect every serial number
    that has ever been stored.  Works across runs — previous data is kept
    and new duplicates are rejected.
    """
    seen: set[int] = set()
    for path in ALL_FILES:
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                m = _ID_RE.match(line)
                if m:
                    seen.add(int(m.group(1)))
    return seen


# ─── Globals ─────────────────────────────────────────────────────────────────
lock          = asyncio.Lock()
blocked_event = asyncio.Event()
recent_results: list[bool] = []
stats         = {"found": 0, "not_found": 0, "errors": 0, "skipped": 0, "total": 0}
stats_lock    = asyncio.Lock()
start_time    = time.time()

# Populated once at startup; protected by `lock` for writes
seen_ids: set[int] = set()


# ════════════════════════════════════════════════════════════════════════════
# I/O helpers
# ════════════════════════════════════════════════════════════════════════════

async def save_unique(file: str, file_id: int, text: str) -> bool:
    """
    Write `text` to `file` ONLY if `file_id` has never been stored before
    (in any file, in any previous or current run).
    Returns True if written, False if it was a duplicate.
    """
    async with lock:
        if file_id in seen_ids:
            return False          # duplicate — skip silently
        seen_ids.add(file_id)
        with open(file, "a", encoding="utf-8") as f:
            f.write(text + "\n")
        return True


async def record_result(ok: bool):
    async with stats_lock:
        recent_results.append(ok)
        if len(recent_results) > BLOCK_WINDOW:
            recent_results.pop(0)
        if len(recent_results) == BLOCK_WINDOW:
            error_rate = recent_results.count(False) / BLOCK_WINDOW
            if error_rate >= BLOCK_ERROR_RATE:
                print(
                    f"\n🚫  BLOCK DETECTED — {error_rate:.0%} error rate "
                    f"over last {BLOCK_WINDOW} requests. Stopping all workers.\n"
                )
                blocked_event.set()


async def update_stats(key: str, file_id: int = 0):
    async with stats_lock:
        stats[key]    += 1
        stats["total"] += 1
        elapsed = time.time() - start_time
        rps = stats["total"] / elapsed if elapsed > 0 else 0
        print(
            f"\r[{key.upper():9s}] #{file_id or '?':>10} | "
            f"found={stats['found']} nf={stats['not_found']} "
            f"err={stats['errors']} skip={stats['skipped']} "
            f"total={stats['total']} rps={rps:.1f}",
            end="", flush=True,
        )


# ════════════════════════════════════════════════════════════════════════════
# Core fetch
# ════════════════════════════════════════════════════════════════════════════

async def fetch(session: aiohttp.ClientSession,
                file_id: int,
                semaphore: asyncio.Semaphore,
                delay: float):

    if blocked_event.is_set():
        return

    # ── Fast skip: already processed in a previous run ───────────────────
    async with lock:
        already = file_id in seen_ids
    if already:
        await update_stats("skipped", file_id)
        return

    url = BASE_URL.format(file_id)

    if delay:
        await asyncio.sleep(delay)

    async with semaphore:
        if blocked_event.is_set():
            return

        try:
            async with session.get(
                url, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as response:

                # ── Instant block via HTTP status ─────────────────────
                if response.status in BLOCK_CODES:
                    line = f"{file_id} | BLOCKED HTTP {response.status} | {url}"
                    await save_unique(ERROR_FILE, file_id, line)
                    print(
                        f"\n🚫  HTTP {response.status} on {file_id} "
                        f"— server blocking detected. Stopping.\n"
                    )
                    await record_result(False)
                    await update_stats("errors", file_id)
                    blocked_event.set()
                    return

                if response.status != 200:
                    line = f"{file_id} | STATUS {response.status} | {url}"
                    await save_unique(ERROR_FILE, file_id, line)
                    await record_result(False)
                    await update_stats("errors", file_id)
                    return

                html = await response.text(errors="ignore")

                # ── Parse ─────────────────────────────────────────────
                soup = BeautifulSoup(html, "html.parser")
                h4   = soup.find("h4", class_="m-0 font-weight-bold text-primary")

                # NOT FOUND
                if h4 and "File not found" in h4.text:
                    line = f"{file_id} | File not found | {url}"
                    await save_unique(NOTFOUND_FILE, file_id, line)
                    await record_result(True)
                    await update_stats("not_found", file_id)
                    return

                # FOUND (valid media file)
                if h4 and h4.text.strip():
                    name = h4.text.strip()
                    if any(ext in name for ext in (".mkv", ".mp4", ".avi")):
                        line = f"{file_id} | {name} | {url}"
                        await save_unique(FOUND_FILE, file_id, line)
                        await record_result(True)
                        await update_stats("found", file_id)
                        return

                # Fallback → not found
                line = f"{file_id} | NOT FOUND | {url}"
                await save_unique(NOTFOUND_FILE, file_id, line)
                await record_result(True)
                await update_stats("not_found", file_id)

        except asyncio.TimeoutError:
            line = f"{file_id} | TIMEOUT | {url}"
            await save_unique(ERROR_FILE, file_id, line)
            await record_result(False)
            await update_stats("errors", file_id)

        except Exception as e:
            line = f"{file_id} | ERROR {e} | {url}"
            await save_unique(ERROR_FILE, file_id, line)
            await record_result(False)
            await update_stats("errors", file_id)


# ════════════════════════════════════════════════════════════════════════════
# Tier runner
# ════════════════════════════════════════════════════════════════════════════

async def run_tier(session: aiohttp.ClientSession,
                   ids: list[int],
                   tier: dict):

    if blocked_event.is_set():
        print(f"⏭  Skipping tier {tier['name']} — blocked.")
        return

    concurrency = tier["concurrency"]
    delay       = tier["delay"]
    sem         = asyncio.Semaphore(concurrency)

    print(f"\n{'═'*65}")
    print(f"  Tier: {tier['name']}  |  IDs: {ids[0]}–{ids[-1]}  |  "
          f"concurrency={concurrency}  delay={delay}s  count={len(ids)}")
    print(f"{'═'*65}")

    tasks = [fetch(session, fid, sem, delay) for fid in ids]

    chunk = concurrency * 4
    for i in range(0, len(tasks), chunk):
        if blocked_event.is_set():
            print("\n⛔  Block detected — aborting tier early.")
            break
        await asyncio.gather(*tasks[i : i + chunk])


# ════════════════════════════════════════════════════════════════════════════
# Main entry
# ════════════════════════════════════════════════════════════════════════════

async def run_scraper(start: int, end: int):
    global seen_ids

    # ── Load all previously stored IDs (dedup across runs) ───────────────
    seen_ids = _load_seen_ids()
    print(f"♻️   Loaded {len(seen_ids):,} already-seen IDs from disk "
          f"(duplicates will be skipped automatically).")

    all_ids   = list(range(start, end + 1))
    total_ids = len(all_ids)
    fresh     = sum(1 for i in all_ids if i not in seen_ids)
    print(f"📋  Range: {start}–{end}  |  total={total_ids:,}  "
          f"new={fresh:,}  already-done={total_ids - fresh:,}\n")

    connector = aiohttp.TCPConnector(limit=100, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:

        offset = 0
        for tier in TIERS:
            if blocked_event.is_set():
                break
            batch = all_ids[offset : offset + tier["size"]]
            if not batch:
                break
            await run_tier(session, batch, tier)
            offset += tier["size"]

        # Remaining IDs (beyond 30 k) → SLOW tier
        if not blocked_event.is_set() and offset < total_ids:
            remaining = all_ids[offset:]
            slow_tier = {**TIERS[-1], "name": "SLOW(remainder)"}
            await run_tier(session, remaining, slow_tier)

    elapsed = time.time() - start_time
    print(f"\n\n{'═'*65}")
    print(f"  Done in {elapsed:.1f}s")
    print(f"  Found:     {stats['found']:,}")
    print(f"  Not found: {stats['not_found']:,}")
    print(f"  Errors:    {stats['errors']:,}")
    print(f"  Skipped:   {stats['skipped']:,}  (duplicates from previous runs)")
    print(f"  Blocked:   {'YES ⛔' if blocked_event.is_set() else 'No ✅'}")
    print(f"{'═'*65}\n")


if __name__ == "__main__":
    START = 21895004 
    END   = 21896074 
    asyncio.run(run_scraper(START, END))
