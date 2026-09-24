# DistriLab — CS324 Assignment 1

A runnable Java 21 / Maven project with separate bootstrap, worker and client
processes communicating through Java RMI. No runtime dependencies beyond the JDK.
JUnit is used for tests. Code was implemented with AI assistance; commits do not
represent separate human group-member contributions.

## Build in VS Code on Windows

1. Put a JDK 21 and Maven on PATH.
2. Open this folder in VS Code. All commands below run in its PowerShell terminal.
3. Check `java -version` and `mvn -version` (both should use Java 21).
4. Run `mvn clean verify -Pintegration`.

This builds `target/distrilab-1.0.0.jar`, runs unit/RMI tests, and starts/cleans up
separate JVMs for the integration test. The first build downloads Maven plugins
and test dependencies. Run `mvn test` for just unit and in-process RMI tests.

## Start a local cluster

Run each command in a **separate terminal**, with this project as the working
directory. Keep each server terminal open. Start bootstrap first, then workers in
order, waiting for `WORKER READY` before starting the next one. Start clients after
all four workers are ready. Join and restart operations should happen while jobs
are idle; dynamic membership during a job is outside this implementation's scope.

Bootstrap (arguments: advertised host, registry port, RMI object port):

```powershell
java -Dsun.rmi.transport.tcp.responseTimeout=60000 -cp target/distrilab-1.0.0.jar edu.usp.cs324.network.BootstrapNode 127.0.0.1 1099 1199
```

Workers (arguments: unique ID, advertised host, registry port, object port,
bootstrap host, bootstrap registry port):

```powershell
java -Dsun.rmi.transport.tcp.responseTimeout=60000 -cp target/distrilab-1.0.0.jar edu.usp.cs324.network.WorkerNode 1 127.0.0.1 2001 3001 127.0.0.1 1099
```

```powershell
java -Dsun.rmi.transport.tcp.responseTimeout=60000 -cp target/distrilab-1.0.0.jar edu.usp.cs324.network.WorkerNode 2 127.0.0.1 2002 3002 127.0.0.1 1099
```

```powershell
java -Dsun.rmi.transport.tcp.responseTimeout=60000 -cp target/distrilab-1.0.0.jar edu.usp.cs324.network.WorkerNode 3 127.0.0.1 2003 3003 127.0.0.1 1099
```

```powershell
java -Dsun.rmi.transport.tcp.responseTimeout=60000 -cp target/distrilab-1.0.0.jar edu.usp.cs324.network.WorkerNode 4 127.0.0.1 2004 3004 127.0.0.1 1099
```

Run this command in **two more terminals** to start two independent GUI clients:

```powershell
java -Dsun.rmi.transport.tcp.responseTimeout=60000 -cp target/distrilab-1.0.0.jar edu.usp.cs324.client.ClientGui 127.0.0.1 1099
```

Choose an operation, enter comma-separated numbers (or use **Load CSV**), and
click **Submit job**. Each submission gets its own table row. You can submit more
while earlier jobs run, in either client. Double-click a row for full error text.

| Operation | Input / sample file | Expected result |
|---|---|---|
| MAX | `8,3,11,2,-7` / `samples/numbers.csv` | `11` |
| PRIMESUM | `1,1000` / `samples/range.csv` | `76127` |
| PRIMECOUNT | `2,2,3,4,5,-1,0,1,11` / `samples/primes.csv` | `5` |

Observe leaders, neighbours, term counts and JAC with:

```powershell
java -Dsun.rmi.transport.tcp.responseTimeout=60000 -cp target/distrilab-1.0.0.jar edu.usp.cs324.network.ClusterStatus 127.0.0.1 1099
```

With fresh workers 1–4, leader 4 wins because all JAC values are zero. Submit five
jobs with at least four input elements each. After they finish, leader 4 has JAC
15 (three remote allocations per job), and leader 3 wins the next term. A sixth
job is assigned in that new term. Logs show `ELECTION`, `COORDINATOR` and `ASSIGN`.
Each normal join also triggers an election, so term numbers include startup elections.

For a checked headless client, run this in two terminals; each process submits six
concurrent jobs:

```powershell
java -Dsun.rmi.transport.tcp.responseTimeout=60000 -cp target/distrilab-1.0.0.jar edu.usp.cs324.client.ClientBatch 127.0.0.1 1099
```

Stop processes with Ctrl+C after jobs finish (clients first, workers, bootstrap).
Restart the whole cluster to recover from failed neighbour links.

## Design and assumptions

Bootstrap tracks/probes membership and connects each joining worker to a random
active neighbour in both directions. Normal joins form a sparse tree. ELECTION
and COORDINATOR messages traverse worker-to-worker neighbour links, including
cycle deduplication. All reached workers are ranked by lowest JAC, then highest ID.

The smallest-ID worker serializes elections and gates requests; it is not
necessarily the elected coordinator. The elected coordinator divides, dispatches
and aggregates the work. A term accepts at most five client jobs. Concurrent work
drains before the next election. JAC counts remote chunk allocations, not client
jobs or local chunks. A failed remote allocation still counts; no partial answer
is returned. See [network protocol](docs/network.md) for agreement assumptions,
partition behaviour and limitations, and [job/client rules](docs/jobs-client.md)
for validation, CSV format, splitting and threading.

Inputs are signed 32-bit integers; results use BigInteger. MAX rejects empty lists;
PRIMECOUNT(empty) returns zero; reversed ranges are invalid. Negative values are
valid but not prime. CSV accepts numeric cells only (no header), optional numeric
quotes, commas/newlines, and UTF-8 BOM. Empty cells are errors; blank rows are ignored.

## Verification and troubleshooting

`mvn clean verify -Pintegration` covers calculations, validation, boundaries,
balanced splitting/aggregation, ranking, deduplication, concurrent admission,
cyclic RMI traversal, and failures. The process integration test starts bootstrap,
four workers and two headless clients; checks duplicate-ID rejection, 12 correct
jobs, two term rotations and total JAC 36; then stops its child processes. It uses
temporary local ports. Logs and a status summary are under `target/integration/`.
JUnit XML/text reports are under `target/surefire-reports/` and
`target/failsafe-reports/`.

The GUI still requires a manual visual/interaction check. Tests use loopback, not
multiple physical machines or network partitions. The protocol assumes stable,
trusted membership during jobs/elections and does not provide durable recovery.

- **Port already in use:** stop the old demo or choose distinct registry/object ports.
  Worker IDs must also be unique.
- **Connection refused / not bound:** start bootstrap first; wait for readiness;
  check host/port values and rebuild after switching branches.
- **Firewall / multiple computers:** replace `127.0.0.1` with each machine's
  reachable address and allow its configured registry and RMI object ports.
- **JobEngine missing:** build integrated `main`; the network feature alone has
  contracts but intentionally no calculation provider.
- **Timeout / failed neighbour:** no incomplete result is returned. A timed-out
  job may already have been accepted; retries are manual. Large prime ranges can
  be slow; increase the timeout or use smaller ranges. Restore/restart the cluster.
- **Maven cannot download:** check network access to Maven Central. Java 21 is required.

## File map and Git

- `api/`: shared RMI interfaces and immutable serializable job/term types.
- `network/`: bootstrap, workers, election/JAC state and status command.
- `jobs/`: pure calculation, balanced splitting and aggregation provider.
- `client/`: CSV parser, RMI connection, Swing GUI and checked headless client.
- `src/test/`: job tests, cyclic RMI tests and separate-process integration test.

The Java packages above live under `src/main/java/edu/usp/cs324/`.
Both feature branches originate from the shared-contract commit on `main`;
implementation and verification are separate genuine milestones. Repository-local
commits identify the coding agent as `Codex` without changing your global Git
identity. No remote was provided or inferred.

To connect your own repository, this PowerShell command asks for its actual clone URL:

```powershell
git remote add origin (Read-Host 'Paste your GitHub repository clone URL')
git fetch origin
git branch -a
git log --graph --oneline --decorate --all
```

Inspect existing remote history before integration. If the repository is empty:

```powershell
git push -u origin feature/network-election
git push -u origin feature/jobs-client
git push -u origin main
```

Do not force-push. If the remote has existing commits or team branches, reconcile
them first; these commands do not authorize overwriting that work. Local history
cannot reproduce an initial remote push that never occurred.
