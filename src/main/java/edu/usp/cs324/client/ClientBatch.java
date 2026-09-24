package edu.usp.cs324.client;

import edu.usp.cs324.api.Job;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;

/** Repeatable headless client: each process submits six concurrent, checked jobs. */
public final class ClientBatch {
    private ClientBatch() { }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("ClientBatch bootstrapHost bootstrapPort");
        ClientConnection connection = new ClientConnection(args[0], Integer.parseInt(args[1]));
        List<Job> jobs = List.of(JobInput.parse(Job.Type.MAX, "8,3,11,2,-7"),
                JobInput.parse(Job.Type.PRIMESUM, "1,1000"),
                JobInput.parse(Job.Type.PRIMECOUNT, "2,2,3,4,5,-1,0,1,11"));
        List<BigInteger> expected = List.of(BigInteger.valueOf(11), BigInteger.valueOf(76127), BigInteger.valueOf(5));
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                int index = i % jobs.size();
                results.add(pool.submit(() -> {
                    BigInteger answer = connection.submit(jobs.get(index));
                    if (!expected.get(index).equals(answer)) throw new AssertionError("Wrong result: " + answer);
                    return jobs.get(index).type() + "=" + answer;
                }));
            }
            for (var result : results) System.out.println(result.get());
        }
        System.out.println("CLIENT PASS: 6 concurrent jobs");
    }
}
