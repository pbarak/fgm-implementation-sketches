package fgm.worker.remoteobject;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.*;

public interface NodeRMICollectionInterface extends Remote {
    void add(NodeRMIInterface<?> site) throws RemoteException;
    void remove(NodeRMIInterface<?> site) throws RemoteException;
    List<NodeRMIInterface<?>> getAll() throws RemoteException;
}