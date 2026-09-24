# Jobs and clients

Inputs are signed 32-bit integers. MAX requires at least one value. PRIMECOUNT
accepts an empty list and counts repeated prime entries individually. Negatives,
zero and one are not prime. PRIMESUM has inclusive bounds and rejects start > end.
Its loop uses a long index to avoid overflow at the maximum integer. Results and
aggregation use BigInteger. Primality uses exact trial division; very large ranges
are slow and can exceed configured RMI timeouts.

List lengths/range lengths are divided by the number of workers. The first
remainder chunks get one extra item, so sizes differ by at most one. Empty chunks
are omitted. An empty PRIMECOUNT sends one empty chunk. Each allocated worker gets
at most one chunk per client job. Splitting counts input elements/range widths,
not estimated prime-testing CPU cost. Aggregation takes maximum or sum only after
all assigned chunks return successfully.

CSV is UTF-8, with optional BOM, comma-separated numeric cells and any number of
rows. Blank rows are ignored; blank cells, headers, decimal values and out-of-range
integers are rejected. A numeric cell may have double quotes. PRIMESUM requires
exactly two cells across the file. General text CSV fields are not supported.

The Swing GUI captures input per submission. Parsing, file reads and RMI work run
in SwingWorker background tasks; table updates run on the event-dispatch thread.
SwingWorker queues beyond its default thread pool capacity, so many submissions
remain responsive. Each client process is independent. No transport failure is
silently retried. The ClientBatch command submits six checked jobs concurrently
for headless integration testing; sample inputs are in `samples/`.
