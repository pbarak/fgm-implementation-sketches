package fgm.worker.remoteobject;// Write interface for MyCustomStore

import fgm.common.Vector;

import java.rmi.Remote;
import java.rmi.RemoteException;

import fgm.common.GlobalStatistic;
import fgm.streamdata.StreamRecord;

public interface NodeRMIInterface<T extends Vector> extends Remote {

    void initializeFirstRound(GlobalStatistic<T> E, double theta) throws RemoteException;

    void startNewRound(GlobalStatistic<T> E, double theta) throws RemoteException;

    void startNewSubround(double theta) throws RemoteException;

    void subroundEnded() throws RemoteException;

    void roundEnded() throws RemoteException;

    ///void update(StreamRecord streamRecord, double frequency, long timestamp) throws RemoteException;
	
    boolean ping() throws RemoteException;


}


