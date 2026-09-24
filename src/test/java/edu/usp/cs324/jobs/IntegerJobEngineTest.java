package edu.usp.cs324.jobs;

import edu.usp.cs324.api.*;
import edu.usp.cs324.client.JobInput;
import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class IntegerJobEngineTest {
    private final JobEngine engine = new IntegerJobEngine();

    @Test void calculatesAllThreeOperations() {
        assertEquals(BigInteger.valueOf(-2), engine.compute(JobInput.parse(Job.Type.MAX, "-9,-2,-7")));
        assertEquals(BigInteger.valueOf(76127), engine.compute(JobInput.parse(Job.Type.PRIMESUM, "1,1000")));
        assertEquals(BigInteger.valueOf(5), engine.compute(JobInput.parse(Job.Type.PRIMECOUNT, "2,2,3,4,5,-1,0,1,11")));
        assertEquals(BigInteger.ZERO, engine.compute(JobInput.parse(Job.Type.PRIMECOUNT, "")));
        assertEquals(BigInteger.ZERO, engine.compute(JobInput.parse(Job.Type.PRIMESUM, "-100,1")));
    }

    @Test void validatesInputAndDefensivelyCopiesLists() {
        assertThrows(IllegalArgumentException.class, () -> JobInput.parse(Job.Type.MAX, ""));
        assertThrows(IllegalArgumentException.class, () -> JobInput.parse(Job.Type.PRIMESUM, "5,1"));
        assertThrows(IllegalArgumentException.class, () -> JobInput.parse(Job.Type.PRIMESUM, "5"));
        for (String invalid : List.of("1,,2", "1,", "abc", "1.5", "2147483648", "\"1", "1;2")) {
            assertThrows(IllegalArgumentException.class, () -> JobInput.parse(Job.Type.MAX, invalid));
        }
        List<Integer> original = new ArrayList<>(List.of(3));
        Job job = new Job(Job.Type.MAX, original, 0, 0);
        original.clear();
        assertEquals(List.of(3), job.numbers());
        assertThrows(UnsupportedOperationException.class, () -> job.numbers().add(4));
    }

    @Test void parsesNumericCsvQuotesNewlinesAndBom() {
        Job job = JobInput.parse(Job.Type.MAX, "\uFEFF\"12\", -3\r\n\n7,\"5\"\n");
        assertEquals(List.of(12, -3, 7, 5), job.numbers());
    }

    @Test void splitsRangesWithoutGapsOrOverlaps() {
        List<Job> parts = engine.split(JobInput.parse(Job.Type.PRIMESUM, "1,1000"), 4);
        for (int i = 0; i < 4; i++) {
            assertEquals(i * 250 + 1, parts.get(i).start());
            assertEquals((i + 1) * 250, parts.get(i).end());
        }
        parts = engine.split(JobInput.parse(Job.Type.PRIMESUM, "-2147483648,2147483647"), 3);
        long total = 0, previous = (long) Integer.MIN_VALUE - 1;
        for (Job part : parts) {
            assertEquals(previous + 1, part.start());
            long length = (long) part.end() - part.start() + 1;
            assertTrue(length == 1431655765L || length == 1431655766L);
            total += length;
            previous = part.end();
        }
        assertEquals(4294967296L, total);
        assertEquals(Integer.MAX_VALUE, previous);
    }

    @Test void balancesListsAndMatchesIndependentPrimeOracle() {
        Random random = new Random(324);
        for (int size = 1; size <= 40; size++) {
            List<Integer> values = random.ints(size, -20, 1000).boxed().toList();
            Job job = new Job(Job.Type.PRIMECOUNT, values, 0, 0);
            List<Job> parts = engine.split(job, 7);
            assertEquals(values, parts.stream().flatMap(p -> p.numbers().stream()).toList());
            int min = parts.stream().mapToInt(p -> p.numbers().size()).min().orElseThrow();
            int max = parts.stream().mapToInt(p -> p.numbers().size()).max().orElseThrow();
            assertTrue(max - min <= 1);
            long expected = values.stream().filter(n -> n >= 2 && BigInteger.valueOf(n).isProbablePrime(40)).count();
            assertEquals(BigInteger.valueOf(expected), engine.aggregate(job.type(), parts.stream().map(engine::compute).toList()));
            Job maxJob = new Job(Job.Type.MAX, values, 0, 0);
            assertEquals(BigInteger.valueOf(Collections.max(values)), engine.aggregate(maxJob.type(),
                    engine.split(maxJob, 7).stream().map(engine::compute).toList()));
        }
    }

    @Test void handlesIntegerBoundaryAndUnboundedSumAggregation() {
        assertTrue(IntegerJobEngine.isPrime(Integer.MAX_VALUE));
        assertFalse(IntegerJobEngine.isPrime(Integer.MIN_VALUE));
        assertEquals(BigInteger.valueOf(Integer.MAX_VALUE), engine.compute(
                JobInput.parse(Job.Type.PRIMESUM, "2147483647,2147483647")));
        BigInteger large = BigInteger.valueOf(Long.MAX_VALUE);
        assertEquals(large.multiply(BigInteger.TWO), engine.aggregate(Job.Type.PRIMESUM, List.of(large, large)));
        assertThrows(IllegalArgumentException.class, () -> engine.aggregate(Job.Type.MAX, List.of()));
        assertThrows(IllegalArgumentException.class, () -> engine.split(JobInput.parse(Job.Type.MAX, "1"), 0));
    }

    @Test void providerIsDiscoverable() {
        assertInstanceOf(IntegerJobEngine.class, ServiceLoader.load(JobEngine.class).findFirst().orElseThrow());
    }
}
