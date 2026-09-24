package edu.usp.cs324.network;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Retain IDs for process lifetime so delayed duplicates cannot be reprocessed. */
public final class MessageTracker {
    private final Set<UUID> seen = ConcurrentHashMap.newKeySet();
    public boolean first(UUID id) { return seen.add(id); }
}
