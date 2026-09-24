package edu.usp.cs324.network;

import edu.usp.cs324.api.RetryException;

/** Admission and JAC accounting are atomic, independent of task completion order. */
public final class TermBudget {
    private int jobs;
    private long jac;
    public synchronized void admit(int remoteAssignments) throws RetryException {
        if (jobs >= 5) throw new RetryException("Term has assigned five jobs");
        jac = Math.addExact(jac, remoteAssignments);
        jobs++;
    }
    public synchronized void newTerm() { jobs = 0; }
    public synchronized int jobs() { return jobs; }
    public synchronized long jac() { return jac; }
}
