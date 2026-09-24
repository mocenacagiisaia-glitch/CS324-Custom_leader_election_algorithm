package edu.usp.cs324.jobs;

import edu.usp.cs324.api.*;
import java.math.BigInteger;
import java.util.*;

public final class IntegerJobEngine implements JobEngine {
    @Override public List<Job> split(Job job, int workerCount) {
        if (workerCount < 1) throw new IllegalArgumentException("At least one worker is required");
        long size = job.type() == Job.Type.PRIMESUM
                ? (long) job.end() - job.start() + 1 : job.numbers().size();
        if (size == 0) return List.of(job); // PRIMECOUNT(empty) has a valid zero result.
        int count = (int) Math.min(size, workerCount);
        List<Job> parts = new ArrayList<>(count);
        long offset = 0;
        for (int i = 0; i < count; i++) {
            long length = size / count + (i < size % count ? 1 : 0);
            if (job.type() == Job.Type.PRIMESUM) {
                parts.add(new Job(job.type(), List.of(), (int) (job.start() + offset),
                        (int) (job.start() + offset + length - 1)));
            } else {
                parts.add(new Job(job.type(), job.numbers().subList((int) offset,
                        (int) (offset + length)), 0, 0));
            }
            offset += length;
        }
        return List.copyOf(parts);
    }

    public static boolean isPrime(int value) {
        if (value < 2) return false;
        if (value % 2 == 0) return value == 2;
        for (int divisor = 3; divisor <= value / divisor; divisor += 2) {
            if (value % divisor == 0) return false;
        }
        return true;
    }

    @Override public BigInteger compute(Job part) {
        return switch (part.type()) {
            case MAX -> BigInteger.valueOf(part.numbers().stream().mapToInt(Integer::intValue).max().orElseThrow());
            case PRIMECOUNT -> BigInteger.valueOf(part.numbers().stream().filter(IntegerJobEngine::isPrime).count());
            case PRIMESUM -> {
                BigInteger sum = BigInteger.ZERO;
                // long loop variable prevents wraparound at Integer.MAX_VALUE.
                for (long value = Math.max(2, part.start()); value <= part.end(); value++) {
                    if (isPrime((int) value)) sum = sum.add(BigInteger.valueOf(value));
                }
                yield sum;
            }
        };
    }

    @Override public BigInteger aggregate(Job.Type type, List<BigInteger> results) {
        if (results.isEmpty()) throw new IllegalArgumentException("Missing partial results");
        return type == Job.Type.MAX ? Collections.max(results)
                : results.stream().reduce(BigInteger.ZERO, BigInteger::add);
    }
}
