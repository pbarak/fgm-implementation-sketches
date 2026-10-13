package fgm.coordinator.collectorsinterfaces.test;

import fgm.common.messages.IncrementMessage;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.increment.IncrementCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmCoordinatorState;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.remoteobject.NodeRMICollectionInterface;
import fgm.worker.remoteobject.NodeRMIInterface;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for IncrementCollectionProcessor
 *
 * The processor resolves its remote node store through java.rmi.Naming in the
 * constructor; that lookup is allowed to fail (the constructor swallows the
 * exception) and a Mockito mock is injected into the private field.
 * The FgmCoordinatorState is injected directly, bypassing init() so that no
 * real Kafka StateStore is required.
 */
class IncrementCollectionProcessorTest {

    private IncrementCollectionProcessor processor;
    private NodeRMICollectionInterface remoteNodeStore;
    private FgmCoordinatorState<?> state;

    private static void inject(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static IncrementMessage increment(int round, int subround, int counter) {
        IncrementMessage m = IncrementMessage.getInstance();
        m.setSiteId("siteA");
        m.setCurrentRound(round);
        m.setCurrentSubround(subround);
        m.setIncrementCounter(counter);
        return m;
    }

    @BeforeEach
    void setUp() throws Exception {
        IMessageSender sender = mock(IMessageSender.class);
        processor = new IncrementCollectionProcessor("stateStore", "rmi://invalid:1/none", sender);

        remoteNodeStore = mock(NodeRMICollectionInterface.class);
        state = mock(FgmCoordinatorState.class);

        inject(processor, "remoteNodeStore", remoteNodeStore);
        inject(processor, "fgmCoordinatorState", state);
    }

    @Test
    void matchingRoundAndSubround_belowThreshold_advancesGlobalCounter() {
        when(state.getCurrentRound()).thenReturn(3);
        when(state.getCurrentSubround()).thenReturn(2);
        when(state.getNumberOfNodes()).thenReturn(5);
        when(state.getGlobalC()).thenReturn(0);

        processor.process("test", increment(3, 2, 4)); // 0 + 4 = 4, not > 5

        verify(state).setGlobalC(4);
        verify(state, never()).setCurrentSubround(anyInt());
    }

    @Test
    void mismatchedRound_isIgnored() {
        when(state.getCurrentRound()).thenReturn(3);
        when(state.getCurrentSubround()).thenReturn(2);

        processor.process("test", increment(99, 2, 10));

        verify(state, never()).setGlobalC(anyInt());
        verify(state, never()).setCurrentSubround(anyInt());
    }

    @Test
    void mismatchedSubround_isIgnored() {
        when(state.getCurrentRound()).thenReturn(3);
        when(state.getCurrentSubround()).thenReturn(2);

        processor.process("test", increment(3, 99, 10));

        verify(state, never()).setGlobalC(anyInt());
        verify(state, never()).setCurrentSubround(anyInt());
    }

    @Test
    void crossingThreshold_resetsCounterAndAdvancesSubround() throws Exception {
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);
        when(state.getNumberOfNodes()).thenReturn(3);
        when(state.getGlobalC()).thenReturn(5);         // 3 + 2 = 5 > 3

        doReturn(new ArrayList<>(Arrays.asList(
                mock(NodeRMIInterface.class), mock(NodeRMIInterface.class), mock(NodeRMIInterface.class))))
                .when(remoteNodeStore).getAll();

        processor.process("test", increment(1, 0, 2));

        verify(state).setGlobalC(0);
        verify(state).setCurrentSubround(1);             // 0 -> 1
    }

    @Test
    void subroundEnd_dispatchesToEveryRegisteredNode() throws Exception {
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);
        when(state.getNumberOfNodes()).thenReturn(2);
        when(state.getGlobalC()).thenReturn(7);          // 2 + 5 = 7 > 2

        NodeRMIInterface<?> n1 = mock(NodeRMIInterface.class);
        NodeRMIInterface<?> n2 = mock(NodeRMIInterface.class);
        doReturn(new ArrayList<>(Arrays.asList(n1, n2))).when(remoteNodeStore).getAll();

        processor.process("test", increment(1, 0, 5));

        verify(remoteNodeStore, times(1)).getAll();
        // RMI dispatched asynchronously via RMINetworkDispatcher's per-node executor.
        verify(n1, timeout(2000)).subroundEnded();
        verify(n2, timeout(2000)).subroundEnded();
    }

    @Test
    void counterEqualToNodeCount_doesNotEndSubround() {
        // Condition is strict ">", so equality must NOT trigger the subround end.
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);
        when(state.getNumberOfNodes()).thenReturn(4);
        when(state.getGlobalC()).thenReturn(2);          // 2 + 2 = 4, not > 4

        processor.process("test", increment(1, 0, 2));

        verify(state).setGlobalC(4);
        verify(state, never()).setCurrentSubround(anyInt());
    }

    @Test
    void emptyNodeList_atSubroundEnd_doesNotThrow() throws RemoteException {
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);
        when(state.getNumberOfNodes()).thenReturn(1);
        when(state.getGlobalC()).thenReturn(6);          // 1 + 5 = 6 > 1
        doReturn(new ArrayList<NodeRMIInterface<?>>()).when(remoteNodeStore).getAll();

        assertDoesNotThrow(() -> processor.process("test", increment(1, 0, 5)));
        verify(state).setGlobalC(0);
    }
}
