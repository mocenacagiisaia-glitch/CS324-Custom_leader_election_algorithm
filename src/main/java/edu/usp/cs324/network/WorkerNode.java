package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import java.math.BigInteger;
import java.rmi.RemoteException;
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

    public WorkerNode(Peer self, BootstrapRemote bootstrap, int exportPort, JobEngine engine)
            throws RemoteException {
        super(exportPort);
        this.self = self;
        this.bootstrap = bootstrap;
        this.engine = engine;
    }

    @Override public Status status() {
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
        List<Peer> members = bootstrap.active();
        Peer gate = gate(members);
        if (!gate.equals(self)) return gate.connect().elect();
        gateLock.writeLock().lock();
        try {
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
            gateLock.readLock().lock();
            try {
                try {
                    result = term.leader().connect().assign(job, term, termMembers);
                    accepted = true;
                } catch (RemoteException e) {
                    if (!isRetry(e)) throw e;
                }
            } finally { gateLock.readLock().unlock(); }
            // Write lock drains all accepted jobs before a new snapshot/election.
            elect();
            if (accepted) return result;
        }
    }

    private static boolean isRetry(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof RetryException) return true;
        }
        return false;
    }

    @Override public BigInteger assign(Job job, Term expected, List<Peer> workers) throws RemoteException {
        if (engine == null) throw new RemoteException("JobEngine provider is not installed");
        List<Job> parts = engine.split(job, workers.size());
        synchronized (this) {
            if (!expected.equals(term) || !self.equals(term.leader())) throw new RetryException("Stale leader");
            int remoteAssignments = 0;
            for (int i = 0; i < parts.size(); i++) if (!workers.get(i).equals(self)) remoteAssignments++;
            budget.admit(remoteAssignments);
        }
        System.out.println("ASSIGN leader=" + self.id() + " term=" + expected.number()
                + " jobs=" + budget.jobs() + " JAC=" + budget.jac());
        List<Future<BigInteger>> futures = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            Peer peer = workers.get(i);
            Job part = parts.get(i);
            futures.add(execution.submit(() -> peer.connect().compute(part)));
        }
        List<BigInteger> results = new ArrayList<>();
        RemoteException failure = null;
        // Drain every dispatch, including after failure, before allowing term rotation.
        for (Future<BigInteger> future : futures) {
            try { results.add(future.get()); }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RemoteException("Interrupted; job outcome unknown", e);
            } catch (ExecutionException e) { failure = new RemoteException("Job failed; no partial answer", e.getCause()); }
        }
        if (failure != null) throw failure;
        return engine.aggregate(job.type(), results);
    }

    @Override public BigInteger compute(Job part) throws RemoteException {
        if (engine == null) throw new RemoteException("JobEngine provider is not installed");
        try { return execution.submit(() -> engine.compute(part)).get(); }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Computation interrupted", e);
        } catch (ExecutionException e) { throw new RemoteException("Computation failed", e.getCause()); }
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
