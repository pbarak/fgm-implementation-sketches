package fgm.remote.site.store;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import fgm.common.cfg.implementation.FilePropertiesLocalStore;
import fgm.worker.remoteobject.NodeRMIInterface;
import fgm.worker.remoteobject.NodeRMICollectionInterface;

public class RemoteSiteStore implements NodeRMICollectionInterface {

    // The mutable list of currently-registered nodes (callers from add/remove)
    private static final List<NodeRMIInterface<?>> remoteObjects = new ArrayList<>();

    // A cached snapshot returned to callers never blocks on RMI.
    private static volatile List<NodeRMIInterface<?>> liveSnapshot = Collections.emptyList();

    private static final NodeRMICollectionInterface remoteNodeStore = new RemoteSiteStore();
    private static final FilePropertiesLocalStore filePropertiesLocalStore = FilePropertiesLocalStore.getInstance();

    // Background pinger runs OFF the StreamThread.
    private static final ScheduledExecutorService pinger =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "remote-store-pinger");
                t.setDaemon(true);
                return t;
            });

    static {
        // Ping every 5s. Tune as you like.
        pinger.scheduleWithFixedDelay(RemoteSiteStore::refreshSnapshot, 5, 5, TimeUnit.SECONDS);
    }

    public RemoteSiteStore() { }

    public synchronized void add(NodeRMIInterface<?> site) throws RemoteException {
        remoteObjects.add(site);
        // Make new node visible to callers immediately, even before next ping cycle.
        liveSnapshot = new ArrayList<>(remoteObjects);
    }

    public synchronized void remove(NodeRMIInterface<?> site) throws RemoteException {
        remoteObjects.remove(site);
        liveSnapshot = new ArrayList<>(remoteObjects);
    }

    /**
     * Returns the cached live snapshot. NEVER blocks on RMI.
     * Safe to call from the Kafka StreamThread.
     */
    public List<NodeRMIInterface<?>> getAll() throws RemoteException {
        return liveSnapshot;
    }

    /**
     * Background refresh: pings all registered nodes, removes dead ones,
     * and atomically swaps the snapshot. Runs on the pinger thread.
     */
    private static void refreshSnapshot() {
        List<NodeRMIInterface<?>> candidates;
        synchronized (RemoteSiteStore.class) {
            candidates = new ArrayList<>(remoteObjects);
        }
        List<NodeRMIInterface<?>> alive = new ArrayList<>(candidates.size());
        for (NodeRMIInterface<?> node : candidates) {
            try {
                node.ping();
                alive.add(node);
            } catch (RemoteException e) {
                // Drop silently; will not appear in next snapshot.
            }
        }
        liveSnapshot = alive;
    }

    public static void main(String[] args) {
        try {
            System.setProperty("sun.rmi.transport.tcp.responseTimeout", "2000");
            System.setProperty("sun.rmi.transport.connectionTimeout", "2000");
            System.setProperty("java.security.policy", String.valueOf(
                    RemoteSiteStore.class.getResource("/resources/project.policy")));
            System.setProperty("java.rmi.server.hostname", filePropertiesLocalStore.retrieveString("java.rmi.server.hostname"));
            if (System.getSecurityManager() == null) {
                System.setSecurityManager(new SecurityManager());
            }
            System.setProperty("sun.rmi.registry.registryFilter", "java.util.ArrayList;");

            final NodeRMICollectionInterface stub =
                    (NodeRMICollectionInterface) UnicastRemoteObject.exportObject(
                            remoteNodeStore, filePropertiesLocalStore.retrieveInt("remotestore.port"));
            final Registry registry = LocateRegistry.createRegistry(filePropertiesLocalStore.retrieveInt("remotestore.port"));
            registry.bind(filePropertiesLocalStore.retrieveString("remotestore.bind.name"), stub);
            System.out.println("Server ready (background pinger, no inline RMI in getAll).");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}