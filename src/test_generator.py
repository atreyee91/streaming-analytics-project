"""Test script for the ClickstreamGenerator."""

import json
import os
from collections import Counter

from event_generator import ClickstreamGenerator

OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "raw", "events"
)


def print_statistics(output_dir: str) -> None:
    """Read all generated event files and print statistics."""
    total_events = 0
    type_counts: Counter = Counter()
    sample_event = None

    for filename in sorted(os.listdir(output_dir)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                total_events += 1
                type_counts[event["event_type"]] += 1
                if sample_event is None:
                    sample_event = event

    print("\n" + "=" * 50)
    print("GENERATION STATISTICS")
    print("=" * 50)
    print(f"Total events: {total_events}")
    print(f"\nEvents by type:")
    for event_type, count in type_counts.most_common():
        pct = (count / total_events * 100) if total_events else 0
        print(f"  {event_type}: {count} ({pct:.1f}%)")
    print(f"\nSample event:")
    if sample_event:
        print(json.dumps(sample_event, indent=2))
    print("=" * 50)


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    generator = ClickstreamGenerator(events_per_second=20)
    print("Starting event generation for 60 seconds...")
    generator.start_streaming(duration_seconds=60, output_dir=OUTPUT_DIR)

    print_statistics(OUTPUT_DIR)
