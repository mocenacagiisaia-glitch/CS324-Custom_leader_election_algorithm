package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import edu.usp.cs324.jobs.IntegerJobEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class StatusSnapshotTest {
    @Test @Timeout(40)
    void statusStaysConsistentDuringAdmissionsAndTermChanges() throws Exception {
        List<Remote> exported = new ArrayList<>();
        try {
            int port;
            try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
            var registry = LocateRegistry.createRegistry(port);
            exported.add(registry);
            Peer leader = new Peer(1, "localhost", 0);
            Peer remote = new Peer(2, "localhost", port);
            var engine = new IntegerJobEngine();
            WorkerNode node = new WorkerNode(leader, null, 0, engine);
            exported.add(node);
            WorkerNode worker = new WorkerNode(remote, null, 0, engine);
            exported.add(worker);
            registry.rebind("worker", worker);
            Term initial = new Term(1, UUID.randomUUID(), leader);
            node.coordinator(initial);

            CountDownLatch readersStarted = new CountDownLatch(4);
            CountDownLatch updatesFinished = new CountDownLatch(1);
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<Long>> readers = new ArrayList<>();
                for (int i = 0; i < 4; i++) readers.add(pool.submit(() -> {
                    long reads = 0;
                    readersStarted.countDown();
                    do {
                        Status snapshot = node.status();
                        // Five admissions per completed term, one remote allocation per job.
                        assertEquals((snapshot.term().number() - 1) * 5 + snapshot.assignedJobs(),
                                snapshot.jac(), () -> "Inconsistent local snapshot: " + snapshot);
                        assertTrue(snapshot.assignedJobs() >= 0 && snapshot.assignedJobs() <= 5);
                        reads++;
                        Thread.yield();
                    } while (updatesFinished.getCount() != 0);
                    return reads;
                }));
                try {
                    assertTrue(readersStarted.await(5, TimeUnit.SECONDS));
                    Job job = new Job(Job.Type.PRIMECOUNT, List.of(2), 0, 0);
                    for (int number = 1; number <= 50; number++) {
                        Term term = number == 1 ? initial : new Term(number, UUID.randomUUID(), leader);
                        node.coordinator(term);
                        // Direct calls isolate local admission/announcement state from gate routing.
                        for (int j = 0; j < 5; j++) {
                            assertEquals(BigInteger.ONE, node.assign(job, term, List.of(remote)));
                        }
                    }
                } finally { updatesFinished.countDown(); }
                for (Future<Long> reader : readers) assertTrue(reader.get(5, TimeUnit.SECONDS) > 0);
                Status last = node.status();
                assertEquals(50, last.term().number());
                assertEquals(5, last.assignedJobs());
                assertEquals(250, last.jac());
            }
        } finally {
            for (Remote object : exported.reversed()) UnicastRemoteObject.unexportObject(object, true);
        }
    }
}
