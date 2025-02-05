# DEV NOTES

pip cache purge

## Three Levels of Abstraction

TOP Level (producer_earthquake.py)

-Application-Specific → Fetches earthquake data and sends it to Kafka.
-Calls mid-level functions → Does not interact with Kafka directly.

MID Level (utils_producer.py and utils_consumer.py)

- Platform-Independent → Provides a general interface 
- Abstracts message production and consumption logic (e.g., retries, topic validation).
- Does NOT directly manage Kafka connections → Calls utils_connector.py.

LOW Level (utils_connector.py) - PLATFORM SPECIFIC

- Handles environment variables (KAFKA_PROVIDER, KAFKA_BROKER).
- Contains the lowest-level Kafka logic (creating instances, checking status).
- Includes all platform-specific Kafka functions
- Handles Kafka producer/consumer creation, topic management, and broker readiness checks
- Provides a reusable foundation for producers and consumers
- Handles a general send (send/produce) as it is platform specific