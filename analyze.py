"""Compare what the producer saw acked with what the topic holds after a failover.

Usage: python analyze.py results/NAME   (one trial)
       python analyze.py --table        (every trial under results/, as Markdown)
"""
import gzip
import json
import pathlib
import sys


def read(path):
    """A trial file, or its .gz copy (what the repository keeps)."""
    if path.exists():
        return path.read_text()
    return gzip.open(str(path) + ".gz", "rt").read()


def trial(out):
    out = pathlib.Path(out)
    meta = json.loads((out / "meta.json").read_text())
    acked, ok_ts, err_ts, failed, tool = [], [], [], [], {}
    for line in read(out / "producer.jsonl").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        rec = json.loads(line)
        if rec.get("name") == "producer_send_success":
            acked.append(rec["value"])
            ok_ts.append(rec["timestamp"])
        elif rec.get("name") == "producer_send_error":
            failed.append(rec["value"])
            err_ts.append(rec["timestamp"])
        elif rec.get("name") == "tool_data":
            tool = rec
    consumed = [v.strip() for v in read(out / "consumed.txt").splitlines() if v.strip()]
    have = set(consumed)
    lost = sorted((v for v in set(acked) if v not in have), key=int)

    # The stall: from the last ack before the first failed send (the kill)
    # to the first ack after it. None if no send failed, or none was acked after.
    stall = None
    if err_ts:
        k = min(err_ts)
        before = [t for t in ok_ts if t <= k]
        after = [t for t in ok_ts if t > k]
        if before and after:
            stall = min(after) - max(before)

    return {
        "trial": out.name,
        "acks": meta["acks"],
        "mode": meta.get("mode", "sigkill"),
        "leader": meta["leader"],
        "killed_at": meta["killed_at_acked"],
        "sent": tool.get("sent", len(acked) + len(failed)),
        "acked": len(acked),
        "failed": len(failed),
        "failed_but_written": sum(v in have for v in failed),
        "consumed": len(consumed),
        "lost": len(lost),
        "lost_values": lost[:20],
        "duplicates": len(consumed) - len(have),
        "stall_ms": stall,
    }


def main():
    if sys.argv[1] == "--table":
        rows = [trial(d) for d in sorted(pathlib.Path("results").iterdir()) if (d / "meta.json").exists()]
        cols = ["trial", "acks", "mode", "acked", "lost", "duplicates", "failed", "failed_but_written", "stall_ms"]
        print("| " + " | ".join(cols) + " |")
        print("|" + "---|" * len(cols))
        for r in rows:
            print("| " + " | ".join("-" if r[c] is None else str(r[c]) for c in cols) + " |")
        return
    r = trial(sys.argv[1])
    (pathlib.Path(sys.argv[1]) / "summary.json").write_text(json.dumps(r, indent=2))
    print(json.dumps({k: v for k, v in r.items() if k != "lost_values"}))
    if r["lost"]:
        print("first lost values:", r["lost_values"])


if __name__ == "__main__":
    main()
