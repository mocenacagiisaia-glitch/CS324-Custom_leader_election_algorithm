# Verification record

Verified locally on Windows with Temurin OpenJDK 21.0.11 and Maven 3.9.12 on
2026-09-24. Source was developed on the two requested feature branches from the
shared-contract baseline, then integrated without conflicts.

Commands executed during development:

- `mvn -q test` / `mvn test`: baseline compilation, network tests, job tests.
- `mvn -q package` / `mvn -q clean package`: independent feature compilation.
- `mvn clean verify -Pintegration`: combined clean build and full verification.

Combined verification passed 11 unit/RMI tests and one separate-process integration
test, with zero failures, errors or skipped tests. Integration started one bootstrap,
four workers, an intentionally rejected duplicate-ID worker, and two independent
headless clients. Each client submitted six concurrent jobs and checked results:
MAX=11, PRIMESUM=76127, PRIMECOUNT=5. All workers agreed after two term rotations;
the final leader was worker 2 and total JAC was 36. The harness stopped its processes.

The RMI network test separately checked cyclic traversal, simultaneous election
requests, 20 concurrent jobs/four rotations, JAC total 60, and failure instead of a
partial answer when a neighbour was removed. Unit tests check atomic five-job
admission, JAC retention, message deduplication, ranking, CSV validation, arithmetic
boundaries, immutable input data, and balanced splitting/aggregation.

Machine-generated evidence is under `target/surefire-reports/`,
`target/failsafe-reports/`, and `target/integration/`. Those generated files are
intentionally ignored by Git and are recreated by the verification command.

Not verified: manual GUI interaction/appearance, multiple physical machines,
partition recovery, membership changes during active jobs, or durable restart.
These limitations are described in the README and network protocol document.

No Git remote was configured or supplied. No branches were pushed and no GitHub
repository URL was inferred. All commits use the local coding-agent identity;
there are no invented human contributions, empty commits, backdated commits,
history rewrites or force pushes. See the README for connection/push commands.
