# Concept reference

Notes on the streaming concepts this pipeline demonstrates. They are
implementation-independent and apply equally to Kafka, Kinesis or Pulsar.

## The log

An event streaming system is an append-only log. Producers append; consumers
read forward and track their own position. Nothing is removed when read, so the
same events can be consumed independently by many readers and replayed from any
point. Placing this log between producers and consumers decouples them: a new
consumer can be added without modifying anything upstream.

## Topics and partitions

A topic is a named stream. It is divided into partitions, each an independently
ordered log. Partitions are the unit of parallelism, and ordering is guaranteed
within a partition rather than across the topic.

## Keys and ordering

A message key is hashed to select a partition, so all messages sharing a key
land on the same partition and remain mutually ordered. Keying orders by
restaurant and pings by driver preserves per-entity ordering while allowing
parallel consumption. A key with too few distinct values concentrates traffic on
one partition and removes that parallelism.

## Offsets

Each message has an offset, its position within a partition. Consumers commit
the offset they have reached, which makes consumption resumable after a crash
and allows a new consumer group to replay history from the beginning.

## Consumer groups

A consumer group shares a topic's partitions among its members, with each
partition assigned to exactly one member. Adding members distributes load;
losing one triggers reassignment to the survivors. Separate groups are fully
independent, each with its own offsets, which is how one topic can serve
persistence, aggregation and alerting simultaneously.

The number of partitions caps useful parallelism. With three partitions, a
fourth consumer in the same group receives nothing.

## Delivery semantics

Delivery is at-least-once by default: a message may be delivered more than once
after a failure, but is not lost. Consumers should therefore be idempotent, so
that reprocessing produces no additional effect.

## Event time and watermarks

Event time is when something happened; processing time is when it was handled.
Aggregations should use event time, otherwise delays move results into the wrong
interval. Because events can arrive out of order, a watermark defines how long
to wait for stragglers before finalising an interval, trading latency against
completeness.

## Windows

A window bounds an unbounded stream so it can be aggregated. Tumbling windows
are fixed and non-overlapping, so each event is counted once. Sliding windows
overlap for moving averages; session windows group activity separated by gaps.

## State and checkpointing

Aggregations spanning multiple events require the processor to retain
information between them. That state is checkpointed to durable storage together
with stream positions so processing can resume after failure.

## Storage selection

Relational databases provide ACID transactions, making them appropriate wherever
correctness matters, such as monetary records. Key-value stores provide
microsecond single-key access with no joins, suiting current-state lookups.
Using each for what it does best is the practical conclusion, and the log makes
it straightforward because every store is a derived view of the same events.
