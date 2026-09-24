package edu.usp.cs324.integration;

import edu.usp.cs324.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.net.ServerSocket;
import java.nio.file.*;
import java.rmi.registry.LocateRegistry;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Runs with -Pintegration after both features are merged; uses actual child JVMs. */
class ClusterIT {
    private final List<Process> processes = new ArrayList<>();
    private final Path logs = Path.of("target", "integration").toAbsolutePath();
    private final Set<Integer> allocatedPorts = new HashSet<>();

    private int port() throws Exception {
        for (;;) {
            try (ServerSocket socket = new ServerSocket(0)) {
                int value = socket.getLocalPort();
                if (allocatedPorts.add(value)) return value;
            }
        }
    }

    private Process start(String log, String main, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dsun.rmi.transport.tcp.responseTimeout=10000", "-cp",
                Path.of("target", "classes").toAbsolutePath().toString(), main));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(logs.resolve(log).toFile()).start();
        processes.add(process);
        return process;
    }

    private void ready(Process process, String log, String marker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            String output = Files.readString(logs.resolve(log));
            if (output.contains(marker)) return;
            assertTrue(process.isAlive(), () -> "Process exited before readiness: " + output);
            Thread.sleep(100);
        }
        fail("Readiness timeout: " + log + "\n" + Files.readString(logs.resolve(log)));
    }

    @Test @Timeout(100)
    void fourWorkersAndTwoClientProcessesCompleteConcurrentJobsAndRotate() throws Exception {
        Files.createDirectories(logs);
        try {
            int bootstrapPort = port();
            Process bootstrapProcess = start("bootstrap.log", "edu.usp.cs324.network.BootstrapNode",
                    "127.0.0.1", "" + bootstrapPort, "" + port());
            ready(bootstrapProcess, "bootstrap.log", "BOOTSTRAP READY");
            for (int id = 1; id <= 4; id++) {
                Process worker = start("worker" + id + ".log", "edu.usp.cs324.network.WorkerNode",
                        "" + id, "127.0.0.1", "" + port(), "" + port(), "127.0.0.1", "" + bootstrapPort);
                ready(worker, "worker" + id + ".log", "WORKER READY");
            }
            BootstrapRemote bootstrap = (BootstrapRemote) LocateRegistry.getRegistry("127.0.0.1", bootstrapPort)
                    .lookup("bootstrap");
            List<Peer> members = bootstrap.active();
            assertEquals(4, members.size());
            Term before = members.getFirst().connect().status().term();
            assertEquals(4, before.leader().id());
            int edges = 0;
            for (Peer peer : members) {
                Status status = peer.connect().status();
                assertEquals(before, status.term());
                edges += status.neighbours().size();
                for (Peer neighbour : status.neighbours()) assertTrue(neighbour.connect().status().neighbours().contains(peer));
            }
            assertEquals(6, edges, "Normal joins create a sparse connected tree");

            Process duplicate = start("duplicate.log", "edu.usp.cs324.network.WorkerNode",
                    "4", "127.0.0.1", "" + port(), "" + port(), "127.0.0.1", "" + bootstrapPort);
            assertTrue(duplicate.waitFor(15, TimeUnit.SECONDS));
            assertNotEquals(0, duplicate.exitValue());
            assertTrue(Files.readString(logs.resolve("duplicate.log")).contains("Duplicate worker ID"));
            assertEquals(4, bootstrap.active().size());

            Process clientA = start("clientA.log", "edu.usp.cs324.client.ClientBatch", "127.0.0.1", "" + bootstrapPort);
            Process clientB = start("clientB.log", "edu.usp.cs324.client.ClientBatch", "127.0.0.1", "" + bootstrapPort);
            for (Process client : List.of(clientA, clientB)) {
                assertTrue(client.waitFor(40, TimeUnit.SECONDS), "Client timed out; inspect target/integration");
                assertEquals(0, client.exitValue(), "Client failed; inspect target/integration");
            }
            for (String log : List.of("clientA.log", "clientB.log")) {
                String output = Files.readString(logs.resolve(log));
                assertTrue(output.contains("CLIENT PASS: 6 concurrent jobs"), output);
                assertTrue(output.contains("MAX=11"), output);
                assertTrue(output.contains("PRIMESUM=76127"), output);
                assertTrue(output.contains("PRIMECOUNT=5"), output);
            }
            Term after = members.getFirst().connect().status().term();
            assertEquals(before.number() + 2, after.number());
            assertEquals(2, after.leader().id());
            long jac = 0;
            StringBuilder report = new StringBuilder();
            for (Peer peer : members) {
                Status status = peer.connect().status();
                assertEquals(after, status.term());
                jac += status.jac();
                report.append(status).append(System.lineSeparator());
            }
            assertEquals(36, jac);
            assertEquals(2, after.leader().connect().status().assignedJobs());
            Files.writeString(logs.resolve("summary.txt"), "PASS: 4 workers, 2 clients, 12 jobs, 2 rotations, JAC total 36\n" + report);
        } finally {
            for (Process process : processes.reversed()) {
                process.destroy();
                if (!process.waitFor(3, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(3, TimeUnit.SECONDS);
                }
            }
        }
    }
}
