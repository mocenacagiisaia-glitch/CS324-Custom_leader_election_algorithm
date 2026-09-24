# Network protocol

Bootstrap only owns the live membership directory, probes worker status, rejects
duplicate IDs and chooses a random existing neighbour during registration. A join
adds both directions of the edge before publishing membership. Elections never
rank bootstrap's directory: ELECTION traverses neighbour edges depth first and
returns JAC candidates along those edges. A process-lifetime UUID set suppresses
duplicates, including cycles. COORDINATOR traverses the same edges.

For a stable membership, the smallest-ID worker is the election gate. Any worker
can initiate an election by asking this gate. It serializes election attempts with
a write lock, gathers all reachable candidates, selects minimum JAC / maximum ID,
and announces a term greater than all observed terms. Older announcements are
ignored. Requests for an already usable term return that term. The gate is a
worker, not the bootstrap, and is not necessarily the elected coordinator.

Client requests enter through any worker and route through the gate to the elected
coordinator. A read lock spans each accepted job, permitting concurrent jobs;
the election write lock drains accepted work. Admission is atomic at the leader.
The sixth request retries only a pre-admission rejection. Five accepted client
jobs close the term; a new election follows completion. JAC increases once per
remote chunk allocated at admission, including failed dispatch attempts; local
chunks do not count. JAC survives terms but is not persisted across process restarts.

Agreement assumes trusted processes, one bootstrap, stable membership during a
job/election, reliable communication, and no gate restart during outstanding
requests. RMI endpoints are not authenticated: use only a trusted lab network.
For this assignment implementation, topology changes during jobs, automatic
reconnection after a failed tree edge, and partition availability are unsupported.
An unreachable neighbour aborts traversal. A reachable-set/directory mismatch
aborts election. A failed dispatch returns an error, never a partial aggregate.
Transport errors are not automatically retried because acceptance may be unknown.
Restore/restart the cluster after failures; there is no durable recovery or
consensus guarantee across partitions. No claim of production fault tolerance.

Use `-Dsun.rmi.transport.tcp.responseTimeout=10000` in demo processes to bound
unresponsive RMI calls (OpenJDK implementation property). Individual computation
may exceed this timeout; increase it for large inputs. UUID memory grows with
election count. Job execution uses Java 21 virtual threads with no admission cap.
