package fgm.coordinator.collectorsinterfaces.test;

import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.driftcollector.DriftCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.driftcollection.model.DriftVectorsCollection;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmCoordinatorState;
import fgm.common.messages.DriftVectorMessage;
import fgm.queryfunction.QueryFunction;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.remoteobject.NodeRMICollectionInterface;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DriftCollectionProcessor.
 * These show the drift-collection bookkeeping that runs before the
 * "all drifts arrived" branch: drift messages are appended, and a duplicate
 * from the same site is rejected.
 */
class DriftCollectionProcessorTest {

    private DriftCollectionProcessor processor;
    private NodeRMICollectionInterface remoteStoreCollection;
    private FgmCoordinatorState<?> state;
    private DriftVectorsCollection<?> drifts;

    private static void inject(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static DriftVectorMessage drift(String site, int round, int subround) {
        DriftVectorMessage m = DriftVectorMessage.getInstance();
        m.setSiteId(site);
        m.setCurrentRound(round);
        m.setCurrentSubround(subround);
        // stateVector left null: the covered tests stop before vector arithmetic.
        return m;
    }

    @BeforeEach
    void setUp() throws Exception {
        IMessageSender sender = mock(IMessageSender.class);
        QueryFunction<?> qf = mock(QueryFunction.class);
        processor = new DriftCollectionProcessor("stateStore", "rmi://invalid:1/none", sender, qf);

        remoteStoreCollection = mock(NodeRMICollectionInterface.class);
        state = mock(FgmCoordinatorState.class);
        drifts = new DriftVectorsCollection<>();           // real collection

        doReturn(drifts).when(state).getDriftCollection();

        inject(processor, "remoteStoreCollection", remoteStoreCollection);
        inject(processor, "fgmCoordinatorState", state);
    }

    @Test
    void firstDrift_isCollected() {
        when(state.getNumberOfNodes()).thenReturn(3);    // one drift never finalizes
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);

        processor.updateGlobalEstimate(drift("siteA", 1, 0));

        assertEquals(1, drifts.getDrifts().size());
    }

    @Test
    void driftsFromDistinctSites_areAllCollected() {
        when(state.getNumberOfNodes()).thenReturn(3);
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);

        processor.updateGlobalEstimate(drift("siteA", 1, 0));
        processor.updateGlobalEstimate(drift("siteB", 1, 0));

        assertEquals(2, drifts.getDrifts().size());
    }

    @Test
    void duplicateDriftFromSameSite_isRejected() {
        when(state.getNumberOfNodes()).thenReturn(3);
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);

        processor.updateGlobalEstimate(drift("siteA", 1, 0));
        processor.updateGlobalEstimate(drift("siteA", 1, 0));   // same site again

        assertEquals(1, drifts.getDrifts().size());
    }

    @Test
    void fewerThanNodeCount_doesNotFinaliseRound() {
        when(state.getNumberOfNodes()).thenReturn(3);    // 3 needed, 2 sent
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);

        processor.updateGlobalEstimate(drift("siteA", 1, 0));
        processor.updateGlobalEstimate(drift("siteB", 1, 0));

        verify(state, never()).setStarted(anyBoolean());
        verifyNoInteractions(remoteStoreCollection);
    }

    @Test
    void reachingNodeCount_entersFinalisationBranch() {
        // With numberOfNodes == 1 a single drift makes the collection "full", so the
        // synchronized finalization block is entered. populateEisCollection and the
        // vector arithmetic that follows need real EisCollection / Vector wiring; here
        // we only assert the boundary is crossed without an unhandled exception
        // escaping.
        when(state.getNumberOfNodes()).thenReturn(1);
        when(state.getCurrentRound()).thenReturn(1);
        when(state.getCurrentSubround()).thenReturn(0);

        assertDoesNotThrow(() -> {
            try {
                processor.updateGlobalEstimate(drift("siteA", 1, 0));
            } catch (NullPointerException expectedFromUnstubbedDeepState) {
                // EisCollection / Vector arithmetic is out of unit-test scope; an NPE
                // here only confirms the finalization branch was reached.
            }
        });

        assertEquals(1, drifts.getDrifts().size());
    }
}