"""Clickstream event generator for streaming analytics simulation."""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

fake = Faker()

CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books"]


class ClickstreamGenerator:
    """Generates realistic clickstream events for streaming analytics."""

    def __init__(self, events_per_second: int = 50) -> None:
        self.events_per_second = events_per_second
        self.events: List[Dict[str, Any]] = []

    def generate_user_session(self) -> List[Dict[str, Any]]:
        """Creates a realistic user journey through a purchase funnel."""
        user_id = f"user_{random.randint(1, 10000)}"
        session_id = str(uuid.uuid4())
        session_events: List[Dict[str, Any]] = []

        # 1-5 page views
        num_page_views = random.randint(1, 5)
        for _ in range(num_page_views):
            event = self._create_event(user_id, session_id, "page_view")
            session_events.append(event)
            time.sleep(random.uniform(0.5, 3.0) / self.events_per_second)

        # 30% chance of add_to_cart
        if random.random() < 0.3:
            event = self._create_event(user_id, session_id, "add_to_cart")
            session_events.append(event)
            time.sleep(random.uniform(0.5, 3.0) / self.events_per_second)

            # 50% of add_to_cart leads to purchase
            if random.random() < 0.5:
                event = self._create_event(user_id, session_id, "purchase")
                session_events.append(event)

        self.events.extend(session_events)
        return session_events

    def _create_event(
        self, user_id: str, session_id: str, event_type: str
    ) -> Dict[str, Any]:
        """Creates an individual clickstream event."""
        event: Dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "product_id": f"prod_{random.randint(1, 1000)}",
            "category": random.choice(CATEGORIES),
            "user_agent": fake.user_agent(),
            "ip_address": fake.ipv4(),
        }

        if event_type == "purchase":
            event["price"] = round(random.uniform(10, 500), 2)
            event["quantity"] = random.randint(1, 3)

        return event

    def inject_anomaly(self) -> List[Dict[str, Any]]:
        """Creates bot-like traffic with rapid repeated events."""
        bot_user_id = f"bot_{random.randint(1, 100)}"
        session_id = str(uuid.uuid4())
        product_id = f"prod_{random.randint(1, 1000)}"
        anomaly_events: List[Dict[str, Any]] = []

        for _ in range(100):
            event: Dict[str, Any] = {
                "event_id": str(uuid.uuid4()),
                "user_id": bot_user_id,
                "session_id": session_id,
                "event_type": "page_view",
                "timestamp": datetime.utcnow().isoformat(),
                "product_id": product_id,
                "category": random.choice(CATEGORIES),
                "user_agent": fake.user_agent(),
                "ip_address": fake.ipv4(),
            }
            anomaly_events.append(event)

        self.events.extend(anomaly_events)
        logger.warning("Anomaly injected: 100 bot events from %s", bot_user_id)
        return anomaly_events

    def write_events_to_file(self, output_path: str) -> None:
        """Writes accumulated events as JSON lines to the given path."""
        if not self.events:
            return

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            for event in self.events:
                f.write(json.dumps(event) + "\n")

        logger.info("Wrote %d events to %s", len(self.events), output_path)
        self.events.clear()

    def start_streaming(
        self, duration_seconds: int, output_dir: str
    ) -> None:
        """Main loop that generates events and writes rotating files."""
        os.makedirs(output_dir, exist_ok=True)
        start_time = time.time()
        last_write_time = start_time
        last_log_time = start_time
        total_events = 0

        logger.info(
            "Starting stream generation for %d seconds at %d events/sec",
            duration_seconds,
            self.events_per_second,
        )

        while time.time() - start_time < duration_seconds:
            self.generate_user_session()
            now = time.time()

            # Write to file every 10 seconds
            if now - last_write_time >= 10:
                # Inject anomaly with 5% chance per batch
                if random.random() < 0.05:
                    self.inject_anomaly()

                filename = f"events_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
                output_path = os.path.join(output_dir, filename)
                total_events += len(self.events)
                self.write_events_to_file(output_path)
                last_write_time = now

            # Print progress every 30 seconds
            if now - last_log_time >= 30:
                elapsed = now - start_time
                logger.info(
                    "Progress: %.0f/%d seconds | Total events: %d",
                    elapsed,
                    duration_seconds,
                    total_events,
                )
                last_log_time = now

        # Write any remaining events
        if self.events:
            filename = f"events_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            output_path = os.path.join(output_dir, filename)
            total_events += len(self.events)
            self.write_events_to_file(output_path)

        logger.info(
            "Stream complete. Total events generated: %d", total_events
        )


if __name__ == "__main__":
    generator = ClickstreamGenerator(events_per_second=50)
    generator.start_streaming(
        duration_seconds=60,
        output_dir="data/raw",
    )
