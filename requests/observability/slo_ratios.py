#!/usr/bin/env python3
"""Reads the matchmaking scrape, and the ratios of the window between two reads.

The SLO scenario needs two things a shell cannot do with a text exposition: a correct parse
(the label `le="10"` is a prefix of `le="100"`, so a substring match would sum the wrong
buckets) and the arithmetic of a window. Both live here, so the scenario asserts the objective
instead of re-implementing it.

`read` prints one JSON object with the counters and gauges the four SLOs are built from.
`window` prints, as `key=value` lines, the ratios of the traffic between two `read` snapshots -
the hand-computed value the recorded rule has to agree with.

Usage:
    slo_ratios.py read <scrape-url> > /tmp/before.json
    slo_ratios.py window /tmp/before.json /tmp/after.json
"""
import json
import sys
import urllib.request

# The SL-1 boundary, the one the rule's `le` bucket counts. Named, because the number is the
# objective, not a detail of the exposition - and compared as a number, because micrometer
# renders the edge `le="10.0"` while the objective is written `10`.
LATENCY_BUCKET_LE = 10.0


def bucket_edge(raw):
    """The `le` label as a number, or None when it is not one (the +Inf bucket)."""
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def scrape(url):
    """Every series of one exposition, summed by what it measures."""
    body = urllib.request.urlopen(url, timeout=5).read().decode()
    counters = {
        "wait_within_10_s": 0.0,
        "wait_total": 0.0,
        "formed": 0.0,
        "confirmed": 0.0,
        "dissolved_timeout": 0.0,
        "queue_depth": 0.0,
        "oldest_wait_s": 0.0,
        "outbox_pending": 0.0,
        "outbox_parked": 0.0,
        "outbox_oldest_pending_s": 0.0,
    }
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        head, _, value = line.rpartition(" ")
        name, _, labels_raw = head.partition("{")
        labels = {}
        for pair in labels_raw.rstrip("}").split(","):
            key, _, raw = pair.partition("=")
            labels[key.strip()] = raw.strip().strip('"')
        try:
            number = float(value)
        except ValueError:
            continue
        if name == "matchmaking_match_wait_seconds_bucket" and bucket_edge(labels.get("le")) == LATENCY_BUCKET_LE:
            counters["wait_within_10_s"] += number
        elif name == "matchmaking_match_wait_seconds_count":
            counters["wait_total"] += number
        elif name == "matchmaking_matches_formed_total":
            counters["formed"] += number
        elif name == "matchmaking_matches_confirmed_total":
            counters["confirmed"] += number
        elif name == "matchmaking_matches_dissolved_total" and labels.get("reason") == "timeout":
            counters["dissolved_timeout"] += number
        elif name == "matchmaking_queue_depth":
            counters["queue_depth"] += number
        elif name == "matchmaking_queue_oldest_wait_seconds":
            counters["oldest_wait_s"] = max(counters["oldest_wait_s"], number)
        elif name == "matchmaking_outbox_backlog" and labels.get("status") == "PENDING":
            counters["outbox_pending"] += number
        elif name == "matchmaking_outbox_backlog" and labels.get("status") == "PARKED":
            counters["outbox_parked"] += number
        elif name == "matchmaking_outbox_oldest_pending_seconds":
            counters["outbox_oldest_pending_s"] = max(counters["outbox_oldest_pending_s"], number)
    return counters


def window(before, after):
    """The SLO ratios of one window, from counters that only move forward."""
    matched = after["wait_total"] - before["wait_total"]
    within = after["wait_within_10_s"] - before["wait_within_10_s"]
    formed = after["formed"] - before["formed"]
    confirmed = after["confirmed"] - before["confirmed"]
    return {
        "latency_within_10_ratio": within / matched if matched else None,
        "confirm_ratio": confirmed / formed if formed else None,
        "matched_tickets": matched,
        "matched_within_10_s": within,
        "formed": formed,
        "confirmed": confirmed,
        "dissolved_timeout": after["dissolved_timeout"] - before["dissolved_timeout"],
    }


def main():
    if len(sys.argv) < 2:
        print(__doc__.strip())
        return 2
    command = sys.argv[1]
    if command == "read":
        print(json.dumps(scrape(sys.argv[2]), indent=2))
        return 0
    if command == "window":
        with open(sys.argv[2]) as handle:
            before = json.load(handle)
        with open(sys.argv[3]) as handle:
            after = json.load(handle)
        for key, value in window(before, after).items():
            print(f"{key}={value if value is not None else 'n/a'}")
        return 0
    print(f"unknown command: {command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
