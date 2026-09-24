package edu.usp.cs324.client;

import edu.usp.cs324.api.*;
import java.math.BigInteger;
import java.rmi.registry.LocateRegistry;

public final class ClientConnection {
    private final String host;
    private final int port;
    public ClientConnection(String host, int port) { this.host = host; this.port = port; }

    public BigInteger submit(Job job) throws Exception {
        BootstrapRemote bootstrap = (BootstrapRemote) LocateRegistry.getRegistry(host, port).lookup("bootstrap");
        var members = bootstrap.active();
        if (members.isEmpty()) throw new IllegalStateException("No active workers");
        // The entry worker locates the election gate and elected coordinator.
        return members.getFirst().connect().submit(job);
    }
}
