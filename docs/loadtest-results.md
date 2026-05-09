# Nanotrace Load Test Results

Date: 2026-05-11 15:18 PDT

Run id:

```text
load-modal4-batches-1778535611
```

Target:

```text
http://nanotrace-prod-alb-25e097e-2002277432.us-west-1.elb.amazonaws.com
```

Current stack:

```text
HTTP clients
-> ALB
-> ASG EC2/EBS ingest server
-> upload processor
-> S3
-> SQS
-> Rust loader
-> loader processor
-> ClickHouse Cloud
```

Deployment notes:

```text
ASG desired capacity: 1
Active instance during recent testing: i-0923e3f95b05bf572
The active stack uses nanotrace-loader.
S3 object-created notifications go to SQS.
nanotrace-loader consumes SQS messages, fetches S3 objects, optionally runs the
loader processor, and inserts rows into ClickHouse with JSONEachRow.
```

## Test Shape

Purpose:

```text
Avoid client-side I/O limits by running four independent Modal sandboxes per
step. Each sandbox generated one quarter of the aggregate target request rate.
Batch size is the number of events in each POST /events request.
```

Processor setup:

```text
Processor name: loadtest-processors
Stages: upload, loader
Upload processor: adds processor_upload_stage = upload-ok
Loader processor: adds processor_loader_stage = loader-ok
```

Search behavior:

```text
Fixtures: fixtures/events/*.json
Mix: 10% log fixture, 90% non-log fixtures
Batch sizes: 2, 4, 8, 16, 32, 64 events/request
Generators: 4 Modal sandboxes per step
Step duration: 30 seconds
Pass criteria: aggregate errorRate <= 0.01 and max generator p95Ms <= 2000
Target request rate is aggregate req/s across all four generators.
```

Important measurement detail:

```text
POST /events does not execute processors synchronously.
The request path writes the local NDJSON event and returns a receipt.
The upload processor runs later before S3 upload.
The loader processor runs later before ClickHouse insert.
Therefore request latency measures ingest-path throughput while processors are
active in the background pipeline.
```

## Results

| Events/request | Best target req/s | Best target events/s | Achieved req/s | Achieved events/s | Max generator p95 ms | Error rate | Next failed/high req/s |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 2500 | 5000 | 2474.15 | 4948.30 | 817.8 | 0 | 2531 |
| 4 | 1484 | 5936 | 1480.82 | 5923.30 | 167.1 | 0 | 1500 |
| 8 | 1000 | 8000 | 980.72 | 7845.72 | 855.8 | 0 | 1015 |
| 16 | 492 | 7872 | 487.85 | 7805.70 | 689.6 | 0 | 500 |
| 32 | 296 | 9472 | 293.46 | 9390.68 | 792.6 | 0 | 304 |
| 64 | 125 | 8000 | 122.73 | 7854.22 | 1417.1 | 0 | 132 |

Best observed event throughput:

```text
batchSize=32
targetEventsPerSec=9472
achievedEventsPerSec=9390.68
```

## Failure Modes

| Events/request | Failed target req/s | Symptoms |
|---:|---:|---|
| 2 | 2531 | p95 exceeded 2s even with no request errors. |
| 8 | 1015 | p95 exceeded 5s; higher probes saw client backpressure, disconnects, and 502s. |
| 32 | 304 | 8.9% error rate with HTTP 500s. |
| 64 | 132 | 2.22% error rate with HTTP 500s. |

## ClickHouse Visibility

```text
visibleRows=7729416
firstIngestedAt=2026-05-11 21:40:15.402
lastIngestedAt=2026-05-11 22:18:56.455
```

Processor marker sample:

```text
Sampled rows had processor_upload_stage = upload-ok.
Sampled rows had processor_loader_stage = loader-ok.
```

The full ClickHouse marker-count query over `toJSONString(data)` timed out
because it scanned many JSON rows. A cheaper visibility count completed quickly,
and a sample marker query confirmed both processor fields on sampled rows.

## Post-Run State

```text
Test processor deleted after the run.
GET /processors returned {"processors":[]}.
ALB target remained healthy.
```

After the heavier failed saturation probes, `/metrics` counters were observed at
zero while the ALB target was healthy. That suggests the server process restarted
or metrics reset during/after the overload portion of the test.

## Takeaways

1. With four Modal generators, processor-enabled batch throughput peaked at batchSize=32: about 9.4k achieved events/s with zero request errors at the passing point.
2. Larger batches are not automatically better: batchSize=64 passed around 7.9k achieved events/s and started returning HTTP 500s above the passing target.
3. BatchSize=8, 16, and 64 all clustered around 7.8k achieved events/s at their passing points, while batchSize=32 was the best observed setting.
4. The current tested path includes upload and loader processors operationally, but request latency remains ingest-path latency because processors run asynchronously after receipt.
