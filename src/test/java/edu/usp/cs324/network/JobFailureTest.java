package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import edu.usp.cs324.jobs.IntegerJobEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class JobFailureTest {
    private static final Job NORMAL = new Job(Job.Type.PRIMECOUNT, List.of(2, 3), 0, 0);
    private static final Job BLOCKED = new Job(Job.Type.PRIMECOUNT, List.of(99, 99), 0, 0);

    private static final class BlockingEngine implements JobEngine {
        private final JobEngine delegate = new IntegerJobEngine();
        final CountDownLatch started;
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch finished;
        final AtomicReference<Thread> assigning = new AtomicReference<>();
        final AtomicInteger assignments = new AtomicInteger();
        volatile boolean fail;

        BlockingEngine(int tasks) { started = new CountDownLatch(tasks); finished = new CountDownLatch(tasks); }
        public List<Job> split(Job job, int count) {
            assigning.set(Thread.currentThread());
            assignments.incrementAndGet();
            return delegate.split(job, count);
        }
        public BigInteger compute(Job job) {
            if (job.numbers().contains(99)) {
                started.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Test release timed out");
                    // An application cause named RetryException must not replay an accepted job.
                    if (fail) throw new IllegalStateException("Injected computation failure", new RetryException("Not an admission rejection"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                } finally { finished.countDown(); }
            }
            return delegate.compute(job);
        }
        public BigInteger aggregate(Job.Type type, List<BigInteger> results) { return delegate.aggregate(type, results); }
    }

    private static final class Cluster implements AutoCloseable {
        final List<Remote> exported = new ArrayList<>();
        final List<WorkerNode> nodes = new ArrayList<>();
        final List<Registry> registries = new ArrayList<>();
        final BlockingEngine engine;
        Cluster(BlockingEngine engine) throws Exception {
            this.engine = engine;
            try {
                BootstrapNode bootstrap = new BootstrapNode(0);
                exported.add(bootstrap);
                for (int id = 1; id <= 2; id++) {
                    int port;
                    try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
                    Registry registry = LocateRegistry.createRegistry(port);
                    exported.add(registry);
                    registries.add(registry);
                    Peer peer = new Peer(id, "localhost", port);
                    WorkerNode node = new WorkerNode(peer, bootstrap, 0, engine);
                    exported.add(node);
                    nodes.add(node);
                    registry.rebind("worker", node);
                    bootstrap.register(peer);
                }
                nodes.getFirst().elect();
            } catch (Exception e) { close(); throw e; }
        }
        WorkerNode gate() { return nodes.getFirst(); }
        Term fillFourSlots() throws Exception {
            for (int i = 0; i < 4; i++) assertEquals(BigInteger.TWO, gate().submit(NORMAL));
            return gate().status().term();
        }
        public void close() {
            engine.release.countDown();
            for (Remote object : exported.reversed()) {
                try { UnicastRemoteObject.unexportObject(object, true); }
                catch (java.rmi.NoSuchObjectException ignored) { }
            }
        }
    }

    private static boolean causedBy(Throwable error, Class<? extends Throwable> type) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) if (type.isInstance(cause)) return true;
        return false;
    }

    @Test void interruptedFifthAssignmentDrainsBeforeGateUnlockAndRotation() throws Exception {
        BlockingEngine engine = new BlockingEngine(2);
        try (Cluster cluster = new Cluster(engine); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Term before = cluster.fillFourSlots();
            Future<BigInteger> job = pool.submit(() -> cluster.gate().submit(BLOCKED));
            try {
                assertTrue(engine.started.await(5, TimeUnit.SECONDS));
                engine.assigning.get().interrupt();
                assertThrows(TimeoutException.class, () -> job.get(200, TimeUnit.MILLISECONDS));
                Future<Term> election = pool.submit(() -> cluster.gate().elect());
                assertThrows(TimeoutException.class, () -> election.get(200, TimeUnit.MILLISECONDS));
                assertEquals(before, cluster.gate().status().term());
                engine.release.countDown();
                ExecutionException error = assertThrows(ExecutionException.class, () -> job.get(5, TimeUnit.SECONDS));
                assertTrue(causedBy(error, JobFailedException.class));
                assertEquals(0, engine.finished.getCount());
                assertEquals(before.number() + 1, election.get(5, TimeUnit.SECONDS).number());
                assertEquals(BigInteger.TWO, cluster.gate().submit(NORMAL));
            } finally { engine.release.countDown(); }
        }
    }

    @Test void computationFailureOnFifthJobRotatesWithoutRetryOrPartialResult() throws Exception {
        BlockingEngine engine = new BlockingEngine(2);
        engine.fail = true;
        try (Cluster cluster = new Cluster(engine); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Term before = cluster.fillFourSlots();
            Future<BigInteger> job = pool.submit(() -> cluster.gate().submit(BLOCKED));
            try {
                assertTrue(engine.started.await(5, TimeUnit.SECONDS));
                engine.release.countDown();
                ExecutionException error = assertThrows(ExecutionException.class, () -> job.get(5, TimeUnit.SECONDS));
                assertTrue(causedBy(error, JobFailedException.class));
                assertEquals(0, engine.finished.getCount());
                assertEquals(5, engine.assignments.get());
                assertEquals(before.number() + 1, cluster.gate().status().term().number());
                assertEquals(5, cluster.nodes.get(1).status().jac());
                assertEquals(BigInteger.TWO, cluster.gate().submit(NORMAL));
            } finally { engine.release.countDown(); }
        }
    }

    @Test void interruptedComputeWaitsForItsTaskAndRestoresInterruptFlag() throws Exception {
        BlockingEngine engine = new BlockingEngine(1);
        try (Cluster cluster = new Cluster(engine); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            AtomicReference<Thread> caller = new AtomicReference<>();
            Future<Boolean> result = pool.submit(() -> {
                caller.set(Thread.currentThread());
                assertThrows(JobFailedException.class, () -> cluster.gate().compute(BLOCKED));
                return Thread.currentThread().isInterrupted();
            });
            try {
                assertTrue(engine.started.await(5, TimeUnit.SECONDS));
                caller.get().interrupt();
                assertThrows(TimeoutException.class, () -> result.get(200, TimeUnit.MILLISECONDS));
                engine.release.countDown();
                assertTrue(result.get(5, TimeUnit.SECONDS));
                assertEquals(0, engine.finished.getCount());
            } finally { engine.release.countDown(); }
        }
    }

    @Test void lostChunkReplyDrainsOtherCallsButDoesNotPermitRotation() throws Exception {
        BlockingEngine engine = new BlockingEngine(2);
        try (Cluster cluster = new Cluster(engine); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Term before = cluster.fillFourSlots();
            AtomicReference<Future<BigInteger>> remoteWork = new AtomicReference<>();
            CountDownLatch replyLost = new CountDownLatch(1);
            WorkerRemote endpoint = (WorkerRemote) Proxy.newProxyInstance(WorkerRemote.class.getClassLoader(),
                    new Class<?>[]{WorkerRemote.class}, (proxy, method, args) -> {
                        if (method.getName().equals("compute")) {
                            remoteWork.set(pool.submit(() -> cluster.gate().compute((Job) args[0])));
                            if (!engine.started.await(5, TimeUnit.SECONDS)) throw new RemoteException("Test work did not start");
                            replyLost.countDown();
                            throw new RemoteException("Injected lost chunk reply", new SocketTimeoutException("Reply timed out"));
                        }
                        try { return method.invoke(cluster.gate(), args); }
                        catch (InvocationTargetException e) { throw e.getCause(); }
                    });
            WorkerRemote stub = (WorkerRemote) UnicastRemoteObject.exportObject(endpoint, 0);
            cluster.exported.add(endpoint);
            cluster.registries.getFirst().rebind("worker", stub);
            Future<BigInteger> job = pool.submit(() -> cluster.gate().submit(BLOCKED));
            try {
                assertTrue(replyLost.await(5, TimeUnit.SECONDS));
                assertFalse(remoteWork.get().isDone());
                assertThrows(TimeoutException.class, () -> job.get(200, TimeUnit.MILLISECONDS));
                assertEquals(before, cluster.gate().status().term());
                engine.release.countDown();
                ExecutionException error = assertThrows(ExecutionException.class, () -> job.get(5, TimeUnit.SECONDS));
                assertTrue(causedBy(error, SocketTimeoutException.class));
                assertFalse(causedBy(error, JobFailedException.class), "Lost completion must not be labelled settled");
                assertEquals(BigInteger.ZERO, remoteWork.get().get(5, TimeUnit.SECONDS));
                assertThrows(RemoteException.class, () -> cluster.gate().elect());
                assertThrows(RemoteException.class, () -> cluster.gate().submit(NORMAL));
                assertEquals(before, cluster.gate().status().term());
            } finally { engine.release.countDown(); }
        }
    }

    @Test void lostAssignmentReplyBlocksNewWorkAndRotationEvenIfRemoteWorkLaterFinishes() throws Exception {
        BlockingEngine engine = new BlockingEngine(2);
        try (Cluster cluster = new Cluster(engine); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Term before = cluster.fillFourSlots();
            AtomicReference<Future<BigInteger>> remoteWork = new AtomicReference<>();
            WorkerNode leader = cluster.nodes.get(1);
            // Deterministically inject a lost/timed-out reply after remote acceptance.
            WorkerRemote endpoint = (WorkerRemote) Proxy.newProxyInstance(WorkerRemote.class.getClassLoader(),
                    new Class<?>[]{WorkerRemote.class}, (proxy, method, args) -> {
                        if (method.getName().equals("assign")) {
                            @SuppressWarnings("unchecked") List<Peer> workers = (List<Peer>) args[2];
                            remoteWork.set(pool.submit(() -> leader.assign((Job) args[0], (Term) args[1], workers)));
                            if (!engine.started.await(5, TimeUnit.SECONDS)) throw new RemoteException("Test work did not start");
                            throw new RemoteException("Injected lost assignment reply", new SocketTimeoutException("Reply timed out"));
                        }
                        try { return method.invoke(leader, args); }
                        catch (InvocationTargetException e) { throw e.getCause(); }
                    });
            WorkerRemote stub = (WorkerRemote) UnicastRemoteObject.exportObject(endpoint, 0);
            cluster.exported.add(endpoint);
            cluster.registries.get(1).rebind("worker", stub);
            try {
                RemoteException error = assertThrows(RemoteException.class, () -> cluster.gate().submit(BLOCKED));
                assertTrue(causedBy(error, SocketTimeoutException.class));
                assertFalse(remoteWork.get().isDone(), "Remote work is still blocked after the gate call returned");
                assertThrows(RemoteException.class, () -> cluster.gate().elect());
                assertThrows(RemoteException.class, () -> cluster.gate().submit(NORMAL));
                assertEquals(before, cluster.gate().status().term());
                engine.release.countDown();
                assertEquals(BigInteger.ZERO, remoteWork.get().get(5, TimeUnit.SECONDS));
                assertThrows(RemoteException.class, () -> cluster.gate().elect());
                assertEquals(5, engine.assignments.get());
            } finally { engine.release.countDown(); }
        }
    }
}
