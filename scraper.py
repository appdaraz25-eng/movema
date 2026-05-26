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

# ─── Tier profiles (sizes computed dynamically — 1/3 of total range each) ────
TIER_PROFILES = [
    {"name": "FAST",   "concurrency": 30, "delay": 0.0},
    {"name": "MEDIUM", "concurrency": 15, "delay": 0.1},
    {"name": "SLOW",   "concurrency":  5, "delay": 0.3},
]

def build_tiers(total: int) -> list[dict]:
    """Split total IDs into 3 equal parts; last tier absorbs remainder."""
    base  = total // 3
    sizes = [base, base, total - 2 * base]
    return [{**TIER_PROFILES[i], "size": sizes[i]} for i in range(3)]


FOUND_FILE    = "1found.txt"
NOTFOUND_FILE = "notfound.txt"
ERROR_FILE    = "error.txt"
STATE_FILE    = "state.txt"       # persists last processed ID across runs

ALL_FILES = [FOUND_FILE, NOTFOUND_FILE, ERROR_FILE]

# ─── Block-detection thresholds ──────────────────────────────────────────────
BLOCK_WINDOW     = 50
BLOCK_ERROR_RATE = 0.80
BLOCK_CODES      = {403, 429, 503}

_ID_RE = re.compile(r"^(\d+)")


# ════════════════════════════════════════════════════════════════════════════
# State — resume from where last run ended
# ════════════════════════════════════════════════════════════════════════════

def load_start_id(override_start: int | None = None) -> int:
    """
    Returns the ID to start from:
    - If override_start is given (manual workflow_dispatch), use it.
    - Otherwise read state.txt for the next ID after the last run.
    - If state.txt doesn't exist yet, start from 1.
    """
    if override_start is not None:
        return override_start

    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            content = f.read().strip()
            if content.isdigit():
                return int(content)

    return 1   # very first ever run


def save_state(last_id: int):
    """Write the next-start ID so the following run resumes correctly."""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        f.write(str(last_id + 1))


# ════════════════════════════════════════════════════════════════════════════
# Deduplication
# ════════════════════════════════════════════════════════════════════════════

def _load_seen_ids() -> set[int]:
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
deadline_event= asyncio.Event()   # set when time limit is reached
recent_results: list[bool] = []
stats         = {"found": 0, "not_found": 0, "errors": 0, "skipped": 0, "total": 0}
stats_lock    = asyncio.Lock()
start_time    = time.time()

seen_ids: set[int] = set()
last_processed_id:  int = 0      # updated after every successful fetch


# ════════════════════════════════════════════════════════════════════════════
# I/O helpers
# ════════════════════════════════════════════════════════════════════════════

async def save_unique(file: str, file_id: int, text: str) -> bool:
    async with lock:
        if file_id in seen_ids:
            return False
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
    global last_processed_id
    async with stats_lock:
        stats[key]     += 1
        stats["total"] += 1
        if file_id:
            last_processed_id = max(last_processed_id, file_id)
        elapsed = time.time() - start_time
        rps = stats["total"] / elapsed if elapsed > 0 else 0
        remaining = max(0, TIME_LIMIT_SECONDS - elapsed)
        print(
            f"\r[{key.upper():9s}] #{file_id or '?':>10} | "
            f"found={stats['found']} nf={stats['not_found']} "
            f"err={stats['errors']} skip={stats['skipped']} "
            f"total={stats['total']} rps={rps:.1f} "
            f"time_left={int(remaining)}s",
            end="", flush=True,
        )


# ════════════════════════════════════════════════════════════════════════════
# Deadline watcher — stops everything when time limit is hit
# ════════════════════════════════════════════════════════════════════════════

async def deadline_watcher(limit_seconds: float):
    await asyncio.sleep(limit_seconds)
    print(f"\n⏰  Time limit of {limit_seconds/3600:.2f}h reached — stopping gracefully.\n")
    deadline_event.set()
    blocked_event.set()   # reuse blocked_event so all workers exit


# ════════════════════════════════════════════════════════════════════════════
# Core fetch
# ════════════════════════════════════════════════════════════════════════════

async def fetch(session: aiohttp.ClientSession,
                file_id: int,
                semaphore: asyncio.Semaphore,
                delay: float):

    if blocked_event.is_set():
        return

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

                if response.status in BLOCK_CODES:
                    line = f"{file_id} | BLOCKED HTTP {response.status} | {url}"
                    await save_unique(ERROR_FILE, file_id, line)
                    print(f"\n🚫  HTTP {response.status} on {file_id} — server blocking. Stopping.\n")
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
                soup = BeautifulSoup(html, "html.parser")
                h4   = soup.find("h4", class_="m-0 font-weight-bold text-primary")

                if h4 and "File not found" in h4.text:
                    line = f"{file_id} | File not found | {url}"
                    await save_unique(NOTFOUND_FILE, file_id, line)
                    await record_result(True)
                    await update_stats("not_found", file_id)
                    return

                if h4 and h4.text.strip():
                    name = h4.text.strip()
                    if any(ext in name for ext in (".mkv", ".mp4", ".avi")):
                        line = f"{file_id} | {name} | {url}"
                        await save_unique(FOUND_FILE, file_id, line)
                        await record_result(True)
                        await update_stats("found", file_id)
                        return

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
        print(f"⏭  Skipping tier {tier['name']} — stopped.")
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
            print("\n⛔  Stopped — aborting tier early.")
            break
        await asyncio.gather(*tasks[i : i + chunk])


# ════════════════════════════════════════════════════════════════════════════
# Main entry
# ════════════════════════════════════════════════════════════════════════════

# 4 hours 55 minutes in seconds
TIME_LIMIT_SECONDS = (4 * 60 + 55) * 60   # 17700 seconds

# How many IDs to process per session.
# At ~30 req/s FAST tier this is ~500k; we use a large cap and rely on
# the deadline_watcher to stop us at exactly 4h55m.
IDS_PER_SESSION = 5_000_000


async def run_scraper(override_start: int | None = None):
    global seen_ids, last_processed_id

    # ── Load previous state ───────────────────────────────────────────────
    seen_ids = _load_seen_ids()
    start    = load_start_id(override_start)

    print(f"♻️   Loaded {len(seen_ids):,} already-seen IDs from disk.")
    print(f"▶️   Starting from ID: {start:,}")

    end     = start + IDS_PER_SESSION - 1
    all_ids = list(range(start, end + 1))
    total   = len(all_ids)
    fresh   = sum(1 for i in all_ids[:10_000] if i not in seen_ids)  # sample

    tiers = build_tiers(total)

    print(f"📋  Range: {start:,}–{end:,}  |  total={total:,}")
    print(f"📊  Tier split: "
          + "  |  ".join(f"{t['name']}={t['size']:,}" for t in tiers))
    print(f"⏱️   Time limit: {TIME_LIMIT_SECONDS/3600:.2f}h (stops at 4h 55m)\n")

    last_processed_id = start - 1

    connector = aiohttp.TCPConnector(limit=100, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Start the deadline watcher as a background task
        watcher = asyncio.create_task(deadline_watcher(TIME_LIMIT_SECONDS))

        offset = 0
        for tier in tiers:
            if blocked_event.is_set():
                break
            batch = all_ids[offset : offset + tier["size"]]
            if not batch:
                break
            await run_tier(session, batch, tier)
            offset += tier["size"]

        watcher.cancel()

    # ── Save state so next run resumes from here ──────────────────────────
    save_state(last_processed_id)

    elapsed = time.time() - start_time
    stopped_reason = "Time limit ⏰" if deadline_event.is_set() else \
                     "Block detected ⛔" if blocked_event.is_set() else \
                     "Completed ✅"

    print(f"\n\n{'═'*65}")
    print(f"  Stopped:   {stopped_reason}")
    print(f"  Done in:   {elapsed:.1f}s  ({elapsed/3600:.2f}h)")
    print(f"  Last ID:   {last_processed_id:,}")
    print(f"  Next run starts at: {last_processed_id + 1:,}  (saved to {STATE_FILE})")
    print(f"  Found:     {stats['found']:,}")
    print(f"  Not found: {stats['not_found']:,}")
    print(f"  Errors:    {stats['errors']:,}")
    print(f"  Skipped:   {stats['skipped']:,}")
    print(f"{'═'*65}\n")


if __name__ == "__main__":
    import sys
    # Optional: pass a start ID as CLI arg for manual override
    # e.g.  python scraper.py 21925074
    override = int(sys.argv[1]) if len(sys.argv) > 1 else None
    asyncio.run(run_scraper(override))
