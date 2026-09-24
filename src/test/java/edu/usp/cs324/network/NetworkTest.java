package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class NetworkTest {
    @Test void rankUsesLowestJacThenHighestId() {
        Peer a = new Peer(1, "localhost", 1), b = new Peer(2, "localhost", 2);
        assertEquals(b, ElectionRules.winner(List.of(new Candidate(a, 0), new Candidate(b, 0))).peer());
        assertEquals(a, ElectionRules.winner(List.of(new Candidate(a, 0), new Candidate(b, 1))).peer());
    }

    @Test void duplicateMessageIsProcessedOnlyOnce() throws Exception {
        MessageTracker tracker = new MessageTracker();
        UUID id = UUID.randomUUID();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 100; i++) results.add(pool.submit(() -> tracker.first(id)));
            int first = 0;
            for (var result : results) if (result.get()) first++;
            assertEquals(1, first);
        }
    }

    @Test void admissionIsAtomicAndJacSurvivesRotation() throws Exception {
        TermBudget budget = new TermBudget();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 50; i++) results.add(pool.submit(() -> {
                try { budget.admit(3); return true; } catch (RetryException e) { return false; }
            }));
            int accepted = 0;
            for (var result : results) if (result.get()) accepted++;
            assertEquals(5, accepted);
            assertEquals(15, budget.jac());
            budget.newTerm();
            assertEquals(0, budget.jobs());
            assertEquals(15, budget.jac());
        }
    }

    @Test @org.junit.jupiter.api.Timeout(40)
    void cyclicRmiNetworkAgreesAndRotatesUnderConcurrentClients() throws Exception {
        List<Remote> exported = new ArrayList<>();
        try {
            BootstrapNode bootstrap = new BootstrapNode(0);
            exported.add(bootstrap);
            List<WorkerNode> nodes = new ArrayList<>();
            List<Peer> peers = new ArrayList<>();
            JobEngine fixture = new JobEngine() {
                public List<Job> split(Job job, int count) { return Collections.nCopies(count, job); }
                public BigInteger compute(Job job) { return BigInteger.ONE; }
                public BigInteger aggregate(Job.Type type, List<BigInteger> results) {
                    return results.stream().reduce(BigInteger.ZERO, BigInteger::add);
                }
            };
            for (int id = 1; id <= 4; id++) {
                int port;
                try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
                var registry = LocateRegistry.createRegistry(port);
                exported.add(registry);
                Peer peer = new Peer(id, "localhost", port);
                WorkerNode node = new WorkerNode(peer, bootstrap, 0, fixture);
                exported.add(node);
                registry.rebind("worker", node);
                bootstrap.register(peer);
                nodes.add(node);
                peers.add(peer);
            }
            // Add links to ensure recursion encounters cycles, not just the random join tree.
            for (int i = 0; i < 4; i++) {
                nodes.get(i).addNeighbour(peers.get((i + 1) % 4));
                nodes.get((i + 1) % 4).addNeighbour(peers.get(i));
            }
            UUID message = UUID.randomUUID();
            assertEquals(4, nodes.get(0).election(message).size());
            assertTrue(nodes.get(2).election(message).isEmpty());
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<Term>> elections = new ArrayList<>();
                for (WorkerNode node : nodes) elections.add(pool.submit(node::elect));
                Term initial = elections.getFirst().get();
                assertEquals(4, initial.leader().id());
                for (var election : elections) assertEquals(initial, election.get());
                List<Future<BigInteger>> jobs = new ArrayList<>();
                for (int i = 0; i < 20; i++) {
                    Peer clientEntry = peers.get(i % 4);
                    jobs.add(pool.submit(() -> clientEntry.connect().submit(
                            new Job(Job.Type.PRIMECOUNT, List.of(2), 0, 0))));
                }
                for (var job : jobs) assertEquals(BigInteger.valueOf(4), job.get());
            }
            Term finalTerm = nodes.getFirst().status().term();
            assertEquals(5, finalTerm.number());
            assertEquals(60, nodes.stream().mapToLong(n -> n.status().jac()).sum());
            for (WorkerNode node : nodes) assertEquals(finalTerm, node.status().term());
            // A removed neighbour must fail the job, never return an incomplete aggregate.
            UnicastRemoteObject.unexportObject(nodes.get(1), true);
            assertThrows(java.rmi.RemoteException.class, () -> nodes.get(0).submit(
                    new Job(Job.Type.PRIMECOUNT, List.of(2), 0, 0)));
        } finally {
            for (Remote remote : exported.reversed()) {
                try { UnicastRemoteObject.unexportObject(remote, true); }
                catch (java.rmi.NoSuchObjectException ignored) { }
            }
        }
    }
}
