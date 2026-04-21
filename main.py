"""
Phase 1 — Multi-Source Data Quality Profiler
============================================
Profiles ALL sources in config/sources.yaml.

SCENARIO 1: Parallel profiling — all sources run simultaneously
            using ThreadPoolExecutor. Safe: profiles in parallel,
            writes to metadata.db sequentially.

Usage:
    python main.py                     # all sources, parallel
    python main.py --source mydb       # one specific source
    python main.py --sequential        # force sequential (debug)
"""
import json
import sys
import yaml
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from profiler.connector import DBConnector
from profiler.profiler import TableProfiler
from profiler.schema_fingerprint import SchemaFingerprinter
from store.metadata_db import MetadataStore

console = Console(width=120)

# Thread-safe write lock for metadata.db
db_lock = threading.Lock()


def load_sources(config_path="config/sources.yaml") -> list:
    with open(config_path) as f:
        data = yaml.safe_load(f) or {}
    return data.get("sources", [])


def profile_one_source(source: dict, connector: DBConnector) -> dict:
    """
    Profiles a single source — runs in its own thread.
    Returns a result dict with all snapshots collected (not yet written).
    Writing happens in the main thread under a lock.
    """
    source_name = source["name"]
    result = {
        "source":     source_name,
        "ok":         False,
        "snapshots":  [],   # list of (source, profile, fingerprint_json)
        "logs":       [],   # printed after thread completes
        "error":      None,
    }

    try:
        engine        = connector.get_engine(source_name)
        profiler      = TableProfiler(engine)
        fingerprinter = SchemaFingerprinter(engine)
        tables        = connector.get_tables(engine)
        result["logs"].append(("green", f"✓ {source_name}: Connected — {len(tables)} table(s)"))
    except Exception as e:
        result["error"] = str(e)
        result["logs"].append(("red", f"✗ {source_name}: Connection failed — {e}"))
        return result

    for table in tables:
        try:
            profile = profiler.profile_table(table)
            new_fp  = fingerprinter.fingerprint(table)
            result["snapshots"].append((source_name, profile, json.dumps(new_fp["columns"])))
            result["logs"].append(("dim", f"  {source_name}.{table}: {profile['row_count']:,} rows profiled"))
        except Exception as e:
            result["logs"].append(("yellow", f"  {source_name}.{table}: Error — {e}"))

    result["ok"] = True
    return result


def run_parallel(sources, connector, store):
    """Profiles all sources in parallel, writes results sequentially."""
    console.print("\n[bold cyan]Mode: Parallel profiling[/bold cyan] "
                  f"[dim]({len(sources)} sources running simultaneously)[/dim]\n")

    all_results = []
    with ThreadPoolExecutor(max_workers=min(len(sources), 8)) as executor:
        futures = {
            executor.submit(profile_one_source, src, connector): src["name"]
            for src in sources
        }
        for future in as_completed(futures):
            result = future.result()
            all_results.append(result)

            # Print logs in order as each source completes
            for color, msg in result["logs"]:
                if color == "green":   console.print(f"[green]{msg}[/green]")
                elif color == "red":   console.print(f"[red]{msg}[/red]")
                elif color == "yellow":console.print(f"[yellow]{msg}[/yellow]")
                else:                  console.print(f"[dim]{msg}[/dim]")

    # Write to DB sequentially under lock
    console.print("\n[dim]Writing snapshots to metadata.db...[/dim]")
    for result in all_results:
        with db_lock:
            for source_name, profile, fp_json in result["snapshots"]:
                store.save_snapshot(source_name, profile, fp_json)

    return all_results


def run_sequential(sources, connector, store):
    """Original sequential mode — profiles one source at a time."""
    console.print("\n[bold]Mode: Sequential profiling[/bold]\n")
    all_results = []
    for source in sources:
        result = profile_one_source(source, connector)
        for color, msg in result["logs"]:
            if color == "green":   console.print(f"[green]{msg}[/green]")
            elif color == "red":   console.print(f"[red]{msg}[/red]")
            elif color == "yellow":console.print(f"[yellow]{msg}[/yellow]")
            else:                  console.print(f"[dim]{msg}[/dim]")
        for source_name, profile, fp_json in result["snapshots"]:
            store.save_snapshot(source_name, profile, fp_json)
        all_results.append(result)
    return all_results


def main():
    target_source = None
    force_sequential = "--sequential" in sys.argv

    if "--source" in sys.argv:
        idx = sys.argv.index("--source")
        if idx + 1 < len(sys.argv):
            target_source = sys.argv[idx + 1]

    sources   = load_sources()
    store     = MetadataStore()
    connector = DBConnector("config/sources.yaml")

    if target_source:
        sources = [s for s in sources if s["name"] == target_source]
        if not sources:
            console.print(f"[red]Source '{target_source}' not found in sources.yaml[/red]")
            sys.exit(1)

    import time
    start = time.time()

    console.print(Panel(
        f"[bold cyan]DQ Monitor — Multi-Source Profiler[/bold cyan]\n"
        f"Sources: [yellow]{len(sources)}[/yellow]  "
        f"([dim]{', '.join(s['name'] for s in sources)}[/dim])",
        expand=False
    ))

    # Use parallel unless only 1 source or forced sequential
    if len(sources) > 1 and not force_sequential:
        results = run_parallel(sources, connector, store)
    else:
        results = run_sequential(sources, connector, store)

    elapsed = round(time.time() - start, 1)

    # Summary
    console.print("\n[bold white]━━━ SUMMARY ━━━[/bold white]")
    for r in results:
        icon   = "[green]✓[/green]" if r["ok"] else "[red]✗[/red]"
        tables = len(r["snapshots"])
        status = f"{tables} tables profiled" if r["ok"] else f"FAILED: {r['error']}"
        console.print(f"  {icon} {r['source']}: {status}")

    passed = sum(1 for r in results if r["ok"])
    console.print(f"\n  [bold]{passed}/{len(results)} sources profiled in {elapsed}s[/bold]")
    console.print(f"  [dim]Snapshots saved to metadata.db[/dim]\n")


if __name__ == "__main__":
    main()
