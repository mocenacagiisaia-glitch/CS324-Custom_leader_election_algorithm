package edu.usp.cs324.network;

import edu.usp.cs324.api.Candidate;
import java.util.Comparator;
import java.util.List;

public final class ElectionRules {
    private ElectionRules() { }
    public static Candidate winner(List<Candidate> candidates) {
        return candidates.stream().min(Comparator.comparingLong(Candidate::jac)
                .thenComparing(c -> c.peer().id(), Comparator.reverseOrder())).orElseThrow();
    }
}
