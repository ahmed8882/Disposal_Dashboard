"""
Disposal Dashboard MTD — refresh script

Runs the 5 BigQuery queries as the currently-authenticated user, builds
dashboard/index.html, commits it, and pushes to GitHub. GitHub Pages
serves the page at your public URL.

Auth: uses Application Default Credentials.
   On first run, do once:   gcloud auth application-default login

Usage (Windows):
   refresh.bat              <-- double-click

This file does the actual work. Don't run it directly; use refresh.bat.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

import queries

# ---------- Config ----------
BQ_PROJECT = "noonbinimuae"
BQ_LOCATION = "EU"

ROOT = Path(__file__).parent.resolve()
TEMPLATE = ROOT / "dashboard_template.html"
OUTPUT_DIR = ROOT / "docs"
OUTPUT_FILE = OUTPUT_DIR / "index.html"


# ---------- Helpers ----------
def step(msg: str):
    print(f"\n=== {msg}")


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a shell command, fail loudly if it errors."""
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, cwd=ROOT, **kwargs)


def _num(v):
    return 0.0 if v is None else float(v)


# ---------- BigQuery ----------
def fetch_payload() -> dict:
    bq = bigquery.Client(project=BQ_PROJECT, location=BQ_LOCATION)
    started = time.time()

    def query(name: str, sql: str):
        t0 = time.time()
        rows = list(bq.query(sql).result())
        print(f"  {name}: {len(rows)} rows in {time.time()-t0:.1f}s")
        return rows

    step("Querying BigQuery (5 queries)...")
    daily_orders = {
        r["date"].isoformat(): int(r["total_orders"])
        for r in query("daily_orders", queries.SQL_DAILY_ORDERS)
    }
    if not daily_orders:
        raise RuntimeError("No data found for current MTD window. Is the table empty?")

    cat_date = [
        {"d": r["d"].isoformat(), "c": r["c"], "dv": round(_num(r["dv"]), 2), "g": round(_num(r["g"]), 2)}
        for r in query("category_date", queries.SQL_CATEGORY_DATE)
    ]
    brands = [
        {"b": r["b"], "c": r["c"], "dv": round(_num(r["dv"]), 2), "gv": round(_num(r["gv"]), 2)}
        for r in query("brands", queries.SQL_BRANDS)
    ]

    l3 = []
    for r in query("sku_l3", queries.SQL_SKU_L3):
        title = (r["t"] or "")
        if len(title) > 70:
            title = title[:67] + "..."
        l3.append({
            "s": r["s"], "b": r["b"] or "", "t": title, "c": r["c"] or "",
            "y": round(_num(r["y"]), 2),
            "d": round(_num(r["d"]), 2),
            "a": round(_num(r["a"]), 2),
        })

    breach = []
    for r in query("sku_breach", queries.SQL_SKU_BREACH):
        title = (r["t"] or "")
        if len(title) > 70:
            title = title[:67] + "..."
        breach.append({
            "s": r["s"], "b": r["b"] or "", "t": title, "c": r["c"] or "",
            "tg": _num(r["tg"]),
            "dv": round(_num(r["dv"]), 2),
            "gv": round(_num(r["gv"]), 2),
            "p": _num(r["p"]),
            "st": int(r["st"] or 0),
            "sb": int(r["sb"] or 0),
            "dvb": round(_num(r["dvb"]), 2),
            "gvb": round(_num(r["gvb"]), 2),
            "cd": _num(r["cd"]),
            "cg": _num(r["cg"]),
        })

    dates_sorted = sorted(daily_orders.keys())
    return {
        "asOf": dates_sorted[-1],
        "monthStart": dates_sorted[0],
        "dailyOrders": daily_orders,
        "categoryDate": cat_date,
        "brands": brands,
        "l3": l3,
        "breach": breach,
        "_meta": {
            "refreshed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "duration_seconds": round(time.time() - started, 1),
            "row_counts": {
                "categoryDate": len(cat_date),
                "brands": len(brands),
                "l3": len(l3),
                "breach": len(breach),
            },
        },
    }


# ---------- Render ----------
def render_html(payload: dict) -> str:
    html = TEMPLATE.read_text(encoding="utf-8")
    payload_json = json.dumps(payload, separators=(",", ":"))
    refreshed_at = payload.get("_meta", {}).get("refreshed_at", "—")
    html = html.replace("{{ payload_json|safe }}", payload_json)
    html = html.replace("{{ refreshed_at }}", refreshed_at)
    return html


# ---------- Git push ----------
def git_push():
    step("Committing and pushing to GitHub...")
    # Stage only the dashboard folder
    run(["git", "add", "docs/index.html"])
    # Skip commit if nothing changed (e.g. quick re-run)
    diff = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=ROOT,
    )
    if diff.returncode == 0:
        print("  No changes to commit (data identical to last refresh).")
        return
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    run(["git", "commit", "-m", f"refresh dashboard {stamp}"])
    run(["git", "push"])


# ---------- Main ----------
def main():
    if not TEMPLATE.exists():
        print(f"ERROR: template not found at {TEMPLATE}", file=sys.stderr)
        sys.exit(1)

    payload = fetch_payload()
    OUTPUT_DIR.mkdir(exist_ok=True)
    OUTPUT_FILE.write_text(render_html(payload), encoding="utf-8")
    rows = payload["_meta"]["row_counts"]
    print(
        f"\n  Wrote {OUTPUT_FILE.name}: "
        f"{rows['categoryDate']} cat×date, {rows['brands']} brands, "
        f"{rows['l3']} L3 SKUs, {rows['breach']} breach SKUs"
    )

    try:
        git_push()
    except subprocess.CalledProcessError as e:
        print("\nGit push failed. Common causes:")
        print("  - You haven't run `git init` / connected this folder to your GitHub repo yet")
        print("  - GitHub credentials not set up — run `git config --global` once")
        print("  - You aren't on the main branch")
        print(f"\nUnderlying error: {e}")
        sys.exit(1)

    print("\n[OK] Dashboard refreshed and pushed. GitHub Pages will serve the new version in ~30 seconds.")


if __name__ == "__main__":
    main()
