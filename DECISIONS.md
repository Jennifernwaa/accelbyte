# Webhook Delivery Service Decisions


This service uses a pragmatic at-least-once delivery model. Webhooks are  asynchronous and delivered to third-party endpoints outside our control, so we guarantee that an event is persisted and retried until it is either delivered or reaches a terminal failure state. We do not claim exactly-once semantics; instead, we rely on a durable event ID and receiver-side idempotency to make duplicates safe when the endpoint is retried after a transient outage.

Retry behavior is exponential backoff with jitter, capped at a bounded retry budget. A delivery is considered terminal when the endpoint returns a permanent client error (for example 4xx such as 400, 404, or 410) or when the retry budget is exhausted. We keep the event record for inspection and replay rather than dropping it silently.

For long outages, the service isolates work per event and endpoint by processing queued deliveries independently. That prevents a slow or dead customer endpoint from blocking every other customer’s delivery pipeline. In the current single-process implementation, this is simplified by a durable store plus a fair worker loop, which can be expanded later into dedicated per-customer queues.

Ordering is preserved in a simple, single-process sense for the service’s own event processing, but no global ordering guarantee is made across all customers. We intentionally avoid a full sequential global queue because that creates head-of-line blocking and hurts throughput. The system is designed to preserve a sensible per-customer workflow while keeping the implementation small and understandable.
