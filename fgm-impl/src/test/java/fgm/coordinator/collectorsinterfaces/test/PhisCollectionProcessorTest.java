package fgm.coordinator.collectorsinterfaces.test;


import fgm.coordinator.collectorsinterfaces.implementation.processors.state.PhiMessageCollection;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.phiscollector.PhisCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmCoordinatorState;
import fgm.common.messages.PhiMessage;
import fgm.queryfunction.QueryFunction;
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
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.*;

/**
 * Unit tests for PhisCollectionProcessor
 * A PhiMessageCollection is injected
 * everything else is mocked.
 */
class PhisCollectionProcessorTest {

    private PhisCollectionProcessor processor;
    private NodeRMICollectionInterface remoteNodeStore;
    private FgmCoordinatorState<?> state;
    private PhiMessageCollection phis;

    private static void inject(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static PhiMessage phi(String site, int round, int subround, double value) {
        PhiMessage m = PhiMessage.getInstance();
        m.setSiteId(site);
        m.setCurrentRound(round);
        m.setCurrentSubround(subround);
        m.setPhi(value);
        return m;
    }

    @BeforeEach
    void setUp() throws Exception {
        IMessageSender sender = mock(IMessageSender.class);
        QueryFunction<?> qf = mock(QueryFunction.class);
        processor = new PhisCollectionProcessor("stateStore", "rmi://invalid:1/none", sender, qf);

        remoteNodeStore = mock(NodeRMICollectionInterface.class);
        state = mock(FgmCoordinatorState.class);
        phis = new PhiMessageCollection();               // real collection

        doReturn(phis).when(state).getPhis();

        inject(processor, "remoteNodeStore", remoteNodeStore);
        inject(processor, "fgmCoordinatorState", state);
    }

    @Test
    void phiForWrongRound_isNotCollected() {
        when(state.getCurrentRound()).thenReturn(5);
        when(state.getCurrentSubround()).thenReturn(2);
        when(state.getNumberOfNodes()).thenReturn(3);

        processor.process("test",phi("siteA", 4, 1, 0.5)); // round 4 != 5

        assertEquals(0, phis.size());
    }

    @Test
    void phiForWrongSubround_isNotCollected() {
        // Match condition is: phi.subround == state.subround - 1. Here state.subround=2 -> expects 1.
        when(state.getCurrentRound()).thenReturn(5);
        when(state.getCurrentSubround()).thenReturn(2);
        when(state.getNumberOfNodes()).thenReturn(3);

        processor.process("test", phi("siteA", 5, 99, 0.5));

        assertEquals(0, phis.size());
    }

    @Test
    void phiForMatchingRoundAndSubround_isCollected() {
        when(state.getCurrentRound()).thenReturn(5);
        when(state.getCurrentSubround()).thenReturn(2);  // matching phi subround = 1
        when(state.getNumberOfNodes()).thenReturn(3);    // not full yet

        processor.process("test", phi("siteA", 5, 1, 0.5));

        assertEquals(1, phis.size());
    }

    @Test
    void partialCollection_doesNotNotifyNodes() throws RemoteException {
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(2);
        when(state.getNumberOfNodes()).thenReturn(3);    // need 3, send 2

        processor.process("test", phi("siteA", 1, 1, 0.1));
        processor.process("test", phi("siteB", 1, 1, 0.2));

        assertEquals(2, phis.size());
        verify(remoteNodeStore, never()).getAll();       // no dispatch on partial
    }

    @Test
    void fullCollection_belowThreshold_startsNewSubround() throws Exception {
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(2);
        when(state.getNumberOfNodes()).thenReturn(2);
        when(state.getEPsi()).thenReturn(0.01);
        when(state.getTheta()).thenReturn(-0.25);
        // psi (post-add) small, RHS large => psi < threshold => NEW SUBROUND branch.
        when(state.getPsi()).thenReturn(0.0);
        when(state.computeQuery()).thenReturn(1.0);
        when(state.computePhiWithZeroDrift(anyDouble())).thenReturn(1.0);

        NodeRMIInterface<?> n1 = mock(NodeRMIInterface.class);
        NodeRMIInterface<?> n2 = mock(NodeRMIInterface.class);
        doReturn(new ArrayList<>(Arrays.asList(n1, n2))).when(remoteNodeStore).getAll();

        processor.process("test", phi("siteA", 1, 1, 0.1));
        processor.process("test", phi("siteB", 1, 1, 0.1));

        verify(state, atLeastOnce()).setTheta(anyDouble());   // recomputed for new subround
        verify(state, never()).setCurrentRound(anyInt());     // NOT a round end
        verify(n1, timeout(2000)).startNewSubround(anyDouble());
        verify(n2, timeout(2000)).startNewSubround(anyDouble());
    }

    @Test
    void fullCollection_aboveThreshold_endsRound() throws Exception {
        when(state.getCurrentRound()).thenReturn(4);
        when(state.getCurrentSubround()).thenReturn(2);
        when(state.getNumberOfNodes()).thenReturn(2);
        when(state.getEPsi()).thenReturn(0.01);
        // psi large, RHS small => psi >= threshold => ROUND END branch.
        when(state.getPsi()).thenReturn(1000.0);
        when(state.computeQuery()).thenReturn(1.0);
        when(state.computePhiWithZeroDrift(anyDouble())).thenReturn(1.0);

        NodeRMIInterface<?> n1 = mock(NodeRMIInterface.class);
        NodeRMIInterface<?> n2 = mock(NodeRMIInterface.class);
        doReturn(new ArrayList<>(Arrays.asList(n1, n2))).when(remoteNodeStore).getAll();

        processor.process("test", phi("siteA", 4, 1, 5.0));
        processor.process("test", phi("siteB", 4, 1, 5.0));

        verify(state).setCurrentRound(5);                     // 4 -> 5
        verify(state).setCurrentSubround(0);
        verify(n1, timeout(2000)).roundEnded();
        verify(n2, timeout(2000)).roundEnded();
    }
}
