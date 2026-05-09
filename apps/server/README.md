Build

POST /events writes events to local append-only files.
A separate uploader ships closed files to S3.
`apps/loader` ingests the S3 files into observatory.events.

S3 is the raw log. ClickHouse is the query index.

Event write path
POST /events
-> read SECRET_KEY from env
-> validate Authorization: Bearer <SECRET_KEY>
-> parse JSON
-> build event envelope
-> append one NDJSON line to current local file
-> record byte offset + byte length
-> return 200 after local write durability policy is satisfied

Requests without a matching bearer token are rejected before parsing or writing the event.

No ClickHouse call is made on the request path.
No S3 call is made on the request path.

Client event contract

Clients send one JSON event object:

event_id: non-empty string
timestamp: non-empty string
data: JSON object

POST /events also accepts a non-empty JSON array of the same event objects.
Single-event requests return one write receipt. Batch requests return an array
of write receipts in request order.

The server fills:

source_file
source_offset
source_length

`observed_timestamp` is optional. If omitted, ClickHouse defaults it to
`timestamp`. ClickHouse fills `ingested_timestamp`.

Runtime configuration

The server uses standard AWS SDK configuration for object storage. In the
normal AWS case this means AWS_REGION, AWS_ACCESS_KEY_ID, and
AWS_SECRET_ACCESS_KEY are enough when an instance role is not used. For EC2,
prefer an instance role and set only AWS_REGION.

Application settings:

SECRET_KEY
PORT
NANOTRACE_DATA_DIR
NANOTRACE_S3_BUCKET
S3_PREFIX
CLICKHOUSE_URL
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
CLICKHOUSE_DATABASE
CLICKHOUSE_TABLE
CLICKHOUSE_MAX_RESULT_ROWS
CLICKHOUSE_MAX_EXECUTION_SECS
CLICKHOUSE_MAX_BYTES_TO_READ
MAX_REQUEST_BYTES
MAX_EVENT_BYTES
NANOTRACE_PART_MAX_BYTES
NANOTRACE_PART_MAX_AGE_SECS
UPLOAD_POLL_INTERVAL_MS
PROCESSOR_POLL_INTERVAL_SECS
NANOTRACE_DONE_RETENTION_MINS
NANOTRACE_DONE_CLEANUP_INTERVAL_SECS
NANOTRACE_WRITER_LANES
NANOTRACE_WRITER_QUEUE_CAPACITY
NANOTRACE_WRITER_FLUSH_INTERVAL_MS
NANOTRACE_WRITER_FLUSH_BYTES
NANOTRACE_COMPACT_BATCH_RECEIPTS

The writer uses group commit. A request is acknowledged only after its lane has
appended the event rows to the active `.tmp` file and completed `flush + fsync`
for that group. Groups commit when `NANOTRACE_WRITER_FLUSH_INTERVAL_MS` ticks or
`NANOTRACE_WRITER_FLUSH_BYTES` is reached.

`GET /metrics` exposes Prometheus text metrics for request body read time,
queue wait, serialization, file writes, flush/sync work, rotations, bytes, and
error counters.

Read APIs

`POST /query` executes a constrained read-only ClickHouse query. It requires the
same bearer token as ingest, accepts only one `SELECT`/`WITH` statement, adds
`FORMAT JSON`, and sends `readonly=1` plus query resource limits.

```json
{
  "query": "SELECT count() FROM observatory.events WHERE timestamp >= {from:DateTime64(3, 'UTC')}",
  "parameters": {
    "from": "2026-05-10T00:00:00Z"
  }
}
```

`GET /events/{event_id}` uses ClickHouse only as the pointer index:

```sql
SELECT source_file, source_offset, source_length
FROM observatory.events
WHERE event_id = ?
ORDER BY timestamp ASC, source_file ASC, source_offset ASC
LIMIT 1
```

Then it fetches the exact accepted NDJSON line from S3 with a byte range,
validates the line's `event_id`, and returns those bytes as JSON. If multiple
rows share an `event_id`, the earliest row by `timestamp`, then
`source_file/source_offset`, is used as the canonical event.

Local files

Each writer lane owns one current file. Lanes avoid a global append mutex under
concurrent ingest.

NANOTRACE_DATA_DIR/events/dt=2026-05-10/hour=12/host=i-abc123/lane=0/part-000001.ndjson.tmp

When file is big enough or old enough:

flush
fsync
close
rename part-000001.ndjson.tmp -> part-000001.ndjson.ready

Only .ready files may be uploaded.

On startup, leftover `.tmp` files are truncated to the last complete NDJSON line,
fsynced, and recovered as `.ready`.

Uploader owns:

.ready -> .uploading -> .done
-> .failed

Local retention cleanup owns uploaded files:

.done older than NANOTRACE_DONE_RETENTION_MINS -> deleted

Set NANOTRACE_DONE_RETENTION_MINS=0 to disable .done cleanup.
S3 layout
s3://observatory-raw/events/
dt=2026-05-10/
hour=12/
host=i-abc123/
lane=0/
part-000001.ndjson

Use uncompressed NDJSON if source_offset/source_length must support exact S3 byte-range fetches.

If we compress whole files, byte offsets are not useful for direct S3 range lookup.

Raw file format

Each line is one event:

{"event_id":"...","timestamp":"...","source_file":"...","source_offset":0,"source_length":123,"data":{...}}

Each uploaded row already includes:

source_file = S3 object key
source_offset = byte offset of this line in the object
source_length = byte length of this line

ClickHouse fills `observed_timestamp` and `ingested_timestamp` when omitted.

So any ClickHouse row can point back to the exact raw bytes in S3.

Minimal pseudocode
on POST /events:
if request.Authorization != "Bearer " + env.SECRET_KEY:
return 401

event = normalize(request.json)
line = json(event) + "\n"

    lock current_file:
        offset = current_file.size
        write line
        length = len(line)

        remember source_offset = offset
        remember source_length = length

        if file too large or too old:
            fsync current_file
            close current_file
            rename .tmp to .ready
            open new .tmp

    return 200

uploader loop:
for each \*.ready:
rename .ready to .uploading
if an upload processor exists:
    run it over the closed NDJSON object
    restamp source_file/source_offset/source_length
upload file to S3
if all rows were dropped:
    skip S3 upload
if success:
rename .uploading to .done
else:
rename .uploading to .failed
loader:
read S3 NDJSON file
for each line:
insert row into ClickHouse with:
event fields from JSON
source_file = S3 key
source_offset = byte offset
source_length = byte length
ClickHouse table

Use the provided table as the append-only query index:
@schema.sql

This table does not dedupe.
It stores what was accepted.
Dedupe is a separate derived table or query concern.

Invariants
.tmp files are incomplete.
.ready files are complete and immutable.
S3 objects are immutable.
ClickHouse rows point back to raw S3 bytes.
Request path never depends on S3 or ClickHouse.
Raw data is never destroyed before S3 upload succeeds.
