🏗️ Project Architecture Overview
A real-time e-commerce clickstream analytics pipeline - essentially a mini version of what companies like Amazon, Shopify, or Netflix use to analyze user behavior in real-time.

📊 Component by Component
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
Real-world equivalent: This simulates data coming from website tracking (Google Analytics, Segment, Snowplow)

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


🎯 Data Flow Example
How a single user journey flows through the system:

1. User arrives on website (T=0s)
json{
  "event_type": "page_view",
  "user_id": "user_1234",
  "session_id": "sess_abc",
  "product_id": "prod_42",
  "timestamp": "2025-02-07T17:25:01"
}
↓ Written to data/raw/events/events_20250207_172501.json
2. Stream Processor reads file
↓ Bronze Layer: Stores exactly as-is + adds ingestion_timestamp
3. User adds to cart (T=15s)
json{
  "event_type": "add_to_cart",
  "user_id": "user_1234",
  "session_id": "sess_abc",
  "product_id": "prod_42",
  "timestamp": "2025-02-07T17:25:16"
}
↓ Silver Layer: Deduplicates, validates, adds is_purchase=False
4. User completes purchase (T=30s)
json{
  "event_type": "purchase",
  "user_id": "user_1234",
  "product_id": "prod_42",
  "price": 199.99,
  "quantity": 1,
  "timestamp": "2025-02-07T17:25:31"
}
```

**5. Gold Layer aggregations trigger:**

**Revenue Table** (5-min window):
```
window: [17:25:00 - 17:30:00]
product_id: prod_42
total_revenue: 199.99
purchase_count: 1
```

**Conversion Funnel** (10-min window):
```
session_id: sess_abc
page_views: 1
add_to_carts: 1
purchases: 1
conversion_rate: 100%  (1/1)

🔍 Why This Medallion Architecture?
Bronze (Raw)

Purpose: Audit trail, can always reprocess
Benefit: If bugs in transformation logic, you can fix and replay

Silver (Cleansed)

Purpose: Single source of truth for clean data
Benefit: Analysts don't deal with dirty data

Gold (Business Metrics)

Purpose: Pre-aggregated for performance
Benefit: Dashboards query Gold (fast), not raw events (slow)


💡 Key Technologies & Concepts

✅ Modern Stack: PySpark, Delta Lake (used at top tech companies)
✅ Real-Time: Streaming, not batch
✅ Scalable Design: Partitioning, windowing, checkpointing
✅ Production Patterns: Bronze/Silver/Gold, error handling, monitoring
✅ Anomaly Detection: Shows ML/analytics thinking
✅ End-to-End: Data generation → Processing → Storage → (Next: Visualization)

🚀 What's Next :
1. Real-Time Dashboard (Streamlit)

Live metrics updating every 5 seconds
Charts for revenue trends
Anomaly alerts
Conversion funnel visualization

2. Advanced Features (Optional)

Replace file-based streaming with Kafka
Add ML-based fraud detection
Implement data quality monitoring (Great Expectations)
Deploy to cloud (AWS/GCP)
