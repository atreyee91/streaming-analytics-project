"""Inspect and validate generated clickstream events."""

import json
import os

import pandas as pd

EVENTS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw", "events"
)

VALID_EVENT_TYPES = {"page_view", "add_to_cart", "purchase"}


def load_events(events_dir: str) -> pd.DataFrame:
    """Load all JSON line files from the events directory into a DataFrame."""
    records = []
    for filename in sorted(os.listdir(events_dir)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(events_dir, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return pd.DataFrame(records)


def inspect(df: pd.DataFrame) -> None:
    """Print statistics, samples, and validation results."""
    print("=" * 60)
    print("EVENT INSPECTION REPORT")
    print("=" * 60)

    # Total events
    print(f"\nTotal events: {len(df)}")

    # Events by type
    print("\nEvents by type:")
    type_counts = df["event_type"].value_counts()
    for event_type, count in type_counts.items():
        pct = count / len(df) * 100
        print(f"  {event_type}: {count} ({pct:.1f}%)")

    # Sample of 5 events
    print("\nSample of 5 events:")
    print(df.sample(min(5, len(df))).to_string(index=False))

    # Time range
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    min_ts = df["timestamp"].min()
    max_ts = df["timestamp"].max()
    duration = max_ts - min_ts
    print(f"\nTime range:")
    print(f"  Start: {min_ts}")
    print(f"  End:   {max_ts}")
    print(f"  Duration: {duration}")

    # Unique users and sessions
    print(f"\nUnique users:    {df['user_id'].nunique()}")
    print(f"Unique sessions: {df['session_id'].nunique()}")

    # Validation checks
    print("\n" + "-" * 60)
    print("VALIDATION CHECKS")
    print("-" * 60)

    null_timestamps = df["timestamp"].isnull().sum()
    print(f"  Null timestamps:     {null_timestamps} {'PASS' if null_timestamps == 0 else 'FAIL'}")

    invalid_types = df[~df["event_type"].isin(VALID_EVENT_TYPES)]
    print(f"  Invalid event types: {len(invalid_types)} {'PASS' if len(invalid_types) == 0 else 'FAIL'}")

    null_event_ids = df["event_id"].isnull().sum()
    print(f"  Null event IDs:      {null_event_ids} {'PASS' if null_event_ids == 0 else 'FAIL'}")

    duplicate_ids = df["event_id"].duplicated().sum()
    print(f"  Duplicate event IDs: {duplicate_ids} {'PASS' if duplicate_ids == 0 else 'FAIL'}")

    all_passed = null_timestamps == 0 and len(invalid_types) == 0 and null_event_ids == 0 and duplicate_ids == 0
    print(f"\nOverall: {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
    print("=" * 60)


if __name__ == "__main__":
    if not os.path.exists(EVENTS_DIR):
        print(f"No events directory found at {EVENTS_DIR}")
        print("Run test_generator.py first to generate events.")
    else:
        df = load_events(EVENTS_DIR)
        if df.empty:
            print("No events found. Run test_generator.py first.")
        else:
            inspect(df)
