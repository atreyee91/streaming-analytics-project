🏗️ Project Architecture Overview
We're building a real-time e-commerce clickstream analytics pipeline - essentially a mini version of what companies like Amazon, Shopify, or Netflix use to analyze user behavior in real-time.

📊 What We've Built (Component by Component)
1. Event Generator (src/event_generator.py)
What it does: Simulates realistic user behavior on an e-commerce website
Key features:

Generates 4 event types: page_view, add_to_cart, purchase, search
Creates realistic user journeys (sessions with conversion funnels)
Injects anomalies (bot traffic) for testing detection
Writes events to JSON files every 10 seconds (simulates real-time data stream)

Output: Files in data/raw/events/ like:
events_20250207_172501.json
events_20250207_172511.json
...
Real-world equivalent: This simulates data coming from your website tracking (Google Analytics, Segment, Snowplow)

2. Stream Processor (src/stream_processor.py)
What it does: Processes events in real-time using PySpark Structured Streaming
Architecture - Medallion Pattern (Bronze/Silver/Gold):
Raw JSON Files
    ↓
┌─────────────────────────────────────┐
│  BRONZE LAYER (data/bronze/events)  │
│  - Raw ingestion                    │
│  - Minimal transformation           │
│  - Add ingestion timestamp          │
│  - Partition by date                │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│  SILVER LAYER (data/silver/events)  │
│  - Cleansed & validated             │
│  - Deduplicated by event_id         │
│  - Type conversions                 │
│  - Add derived columns (date, hour) │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│  GOLD LAYER (Business Metrics)      │
│                                     │
│  1. Anomalies (data/gold/anomalies) │
│     - Detects bot traffic           │
│     - Users with >50 events/min     │
│                                     │
│  2. Revenue (data/gold/revenue)     │
│     - 5-min windows                 │
│     - Revenue by product            │
│     - Purchase counts               │
│                                     │
│  3. Conversion (data/gold/conv...)  │
│     - 10-min session windows        │
│     - Funnel metrics                │
│     - Conversion rates              │
└─────────────────────────────────────┘
Key Streaming Concepts Used:

Watermarking: Handles late-arriving data (events that arrive out of order)
Windows: Time-based aggregations (tumbling windows of 1min, 5min, 10min)
Checkpointing: Ensures exactly-once processing (can resume after failures)
Delta Lake: ACID transactions, versioning, time travel

Real-world equivalent: This is like Spark Streaming jobs at Uber, Netflix, or LinkedIn processing clickstreams

3. Inspection & Testing Scripts
src/test_generator.py

Runs the event generator for testing
Generates 60 seconds of data
Prints statistics

src/inspect_events.py

Validates raw JSON events
Checks data quality
Shows event distribution

src/run_pipeline.py

Orchestrates the entire streaming pipeline
Manages multiple streaming queries
Handles graceful shutdown

src/inspect_delta_tables.py (you're about to create this)

Reads Delta tables
Shows processed results
Validates pipeline output
