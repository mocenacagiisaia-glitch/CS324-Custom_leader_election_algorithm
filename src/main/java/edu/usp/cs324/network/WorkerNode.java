package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.ServerException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class WorkerNode extends UnicastRemoteObject implements WorkerRemote {
    private final Peer self;
    private final BootstrapRemote bootstrap;
    private final Map<Integer, Peer> neighbours = new ConcurrentSkipListMap<>();
    private final MessageTracker elections = new MessageTracker();
    private final MessageTracker announcements = new MessageTracker();
    private final TermBudget budget = new TermBudget();
    private final ExecutorService execution = Executors.newVirtualThreadPerTaskExecutor();
    private final ReentrantReadWriteLock gateLock = new ReentrantReadWriteLock(true);
    private final JobEngine engine;
    private volatile Term term;
    private volatile List<Peer> termMembers = List.of();
    private volatile RemoteException uncertainAssignment;

    public WorkerNode(Peer self, BootstrapRemote bootstrap, int exportPort, JobEngine engine)
            throws RemoteException {
        super(exportPort);
        this.self = self;
        this.bootstrap = bootstrap;
        this.engine = engine;
    }

    // Share the admission/announcement monitor so term and counters describe one local state.
    @Override public synchronized Status status() {
        return new Status(self, budget.jac(), budget.jobs(), term, List.copyOf(neighbours.values()));
    }
    @Override public void addNeighbour(Peer peer) {
        if (peer.id() != self.id()) neighbours.put(peer.id(), peer);
    }

    @Override public List<Candidate> election(UUID id) throws RemoteException {
        if (!elections.first(id)) return List.of();
        System.out.println("ELECTION " + id + " worker=" + self.id());
        List<Candidate> result = new ArrayList<>();
        result.add(new Candidate(self, budget.jac()));
        // Do not hold a monitor during recursive RMI calls: cycles return through deduplication.
        for (Peer peer : neighbours.values()) result.addAll(peer.connect().election(id));
        return result;
    }

    @Override public void coordinator(Term incoming) throws RemoteException {
        synchronized (this) {
            if (term != null && incoming.number() <= term.number()) return;
            if (!announcements.first(incoming.electionId())) return;
            term = incoming;
            budget.newTerm();
        }
        System.out.println("COORDINATOR term=" + incoming.number() + " leader=" + incoming.leader().id()
                + " worker=" + self.id() + " JAC=" + budget.jac());
        for (Peer peer : neighbours.values()) peer.connect().coordinator(incoming);
    }

    private Peer gate(List<Peer> members) throws RemoteException {
        return members.stream().min(Comparator.comparingInt(Peer::id))
                .orElseThrow(() -> new RemoteException("No active workers"));
    }

    @Override public Term elect() throws RemoteException {
        requireKnownCompletion();
        List<Peer> members = bootstrap.active();
        Peer gate = gate(members);
        if (!gate.equals(self)) return gate.connect().elect();
        if (term != null && members.equals(termMembers)
                && term.leader().connect().status().assignedJobs() < 5) return term;
        gateLock.writeLock().lock();
        try {
            requireKnownCompletion();
            members = bootstrap.active();
            if (term != null && members.equals(termMembers)
                    && term.leader().connect().status().assignedJobs() < 5) return term;
            UUID id = UUID.randomUUID();
            List<Candidate> candidates = election(id);
            Set<Peer> reached = new HashSet<>();
            candidates.forEach(c -> reached.add(c.peer()));
            if (!reached.equals(new HashSet<>(members))) {
                throw new RemoteException("Network is partitioned or membership changed; election aborted");
            }
            long previous = 0;
            for (Peer peer : members) {
                Term known = peer.connect().status().term();
                if (known != null) previous = Math.max(previous, known.number());
            }
            Term next = new Term(Math.addExact(previous, 1), id, ElectionRules.winner(candidates).peer());
            coordinator(next);
            termMembers = List.copyOf(members);
            return next;
        } finally { gateLock.writeLock().unlock(); }
    }

    @Override public BigInteger submit(Job job) throws RemoteException {
        Objects.requireNonNull(job);
        Peer gate = gate(bootstrap.active());
        if (!gate.equals(self)) return gate.connect().submit(job);
        for (;;) {
            elect();
            BigInteger result = null;
            boolean accepted = false;
            RemoteException failure = null;
            gateLock.readLock().lock();
            try {
                requireKnownCompletion();
                try {
                    result = term.leader().connect().assign(job, term, termMembers);
                    accepted = true;
                } catch (RemoteException e) {
                    Throwable cause = remoteCause(e);
                    if (!(cause instanceof RetryException)) {
                        failure = e;
                        if (!(cause instanceof JobFailedException)) {
                            // A returned RMI call does not prove its remote task has stopped.
                            uncertainAssignment = e;
                            throw new RemoteException("Assignment outcome unknown; restart the whole cluster", e);
                        }
                    }
                }
            } finally { gateLock.readLock().unlock(); }
            // Known failed jobs also consume a slot and must allow five-job rotation.
            try { elect(); }
            catch (RemoteException electionFailure) {
                if (failure != null) failure.addSuppressed(electionFailure);
                else if (accepted) System.err.println("Job completed, but election failed: " + electionFailure);
                else throw electionFailure;
            }
            if (failure != null) throw failure;
            if (accepted) return result;
        }
    }

    private void requireKnownCompletion() throws RemoteException {
        if (uncertainAssignment != null) {
            throw new RemoteException("Remote work may still be running; restart the whole cluster before new jobs or elections",
                    uncertainAssignment);
        }
    }

    private static Throwable remoteCause(Throwable error) {
        // Unwrap only RMI envelopes, not application causes inside a completed failure.
        while (error instanceof ServerException && error.getCause() != null) error = error.getCause();
        return error;
    }

    @Override public BigInteger assign(Job job, Term expected, List<Peer> workers) throws RemoteException {
        if (engine == null) throw new JobFailedException("JobEngine provider is not installed", null);
        List<Job> parts;
        try { parts = engine.split(job, workers.size()); }
        catch (RuntimeException e) { throw new JobFailedException("Invalid job; no work started", e); }
        synchronized (this) {
            if (!expected.equals(term) || !self.equals(term.leader())) throw new RetryException("Stale leader");
            int remoteAssignments = 0;
            for (int i = 0; i < parts.size(); i++) if (!workers.get(i).equals(self)) remoteAssignments++;
            budget.admit(remoteAssignments);
        }
        System.out.println("ASSIGN leader=" + self.id() + " term=" + expected.number()
                + " jobs=" + budget.jobs() + " JAC=" + budget.jac());
        List<Future<BigInteger>> futures = new ArrayList<>();
        RuntimeException dispatchFailure = null;
        try {
            for (int i = 0; i < parts.size(); i++) {
                Peer peer = workers.get(i);
                Job part = parts.get(i);
                futures.add(execution.submit(() -> peer.connect().compute(part)));
            }
        } catch (RuntimeException e) { dispatchFailure = e; }
        List<BigInteger> results = awaitTasks(futures, true, dispatchFailure);
        try { return engine.aggregate(job.type(), results); }
        catch (RuntimeException e) { throw new JobFailedException("Aggregation failed; no partial answer", e); }
    }

    private static List<BigInteger> awaitTasks(List<Future<BigInteger>> futures, boolean remoteCalls,
                                               Throwable failure) throws RemoteException {
        List<BigInteger> results = new ArrayList<>();
        boolean interrupted = false;
        boolean uncertain = false;
        try {
            for (Future<BigInteger> future : futures) {
                boolean finished = false;
                while (!finished) {
                    try {
                        results.add(future.get());
                        finished = true;
                    } catch (InterruptedException e) {
                        interrupted = true; // get() cleared the flag; restore only after draining.
                        if (failure == null) failure = e;
                    } catch (ExecutionException e) {
                        Throwable cause = remoteCause(e.getCause());
                        if (remoteCalls && !(cause instanceof JobFailedException)) uncertain = true;
                        if (failure == null) failure = cause;
                        finished = true;
                    } catch (CancellationException e) {
                        // Future cancellation is not confirmation that execution has stopped.
                        uncertain = true;
                        if (failure == null) failure = e;
                        finished = true;
                    }
                }
            }
            if (uncertain) throw new RemoteException("Remote completion unknown; no partial answer", failure);
            if (failure != null) throw new JobFailedException("Job failed after started work finished; no partial answer", failure);
            return results;
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    @Override public BigInteger compute(Job part) throws RemoteException {
        if (engine == null) throw new JobFailedException("JobEngine provider is not installed", null);
        Future<BigInteger> task;
        try { task = execution.submit(() -> engine.compute(part)); }
        catch (RuntimeException e) { throw new JobFailedException("Computation could not start", e); }
        return awaitTasks(List.of(task), false, null).getFirst();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException(
                "WorkerNode id host registryPort exportPort bootstrapHost bootstrapPort");
        System.setProperty("java.rmi.server.hostname", args[1]);
        BootstrapRemote bootstrap = (BootstrapRemote) LocateRegistry.getRegistry(args[4],
                Integer.parseInt(args[5])).lookup("bootstrap");
        Peer self = new Peer(Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2]));
        JobEngine engine = ServiceLoader.load(JobEngine.class).findFirst().orElse(null);
        WorkerNode node = new WorkerNode(self, bootstrap, Integer.parseInt(args[3]), engine);
        LocateRegistry.createRegistry(self.registryPort()).rebind("worker", node);
        try { bootstrap.register(self); node.elect(); }
        catch (Exception e) { System.err.println("Startup failed: " + e); System.exit(1); }
        System.out.println("WORKER READY " + self.id());
    }
}
