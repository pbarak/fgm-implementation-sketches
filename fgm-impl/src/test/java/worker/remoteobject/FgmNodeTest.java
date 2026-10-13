package worker.remoteobject;


import fgm.common.GlobalStatistic;
import fgm.common.Vector;
import fgm.common.cfg.BaseConfiguration;
import fgm.factory.specs.IFgmFactory;
import fgm.queryfunction.QueryFunction;
import fgm.streamdata.StreamRecord;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.worker.remoteobject.implementation.FgmNodeStatistics;
import fgm.worker.statistics.LocalStatistic;
import fgm.worker.windowoperator.implementation.CustomWindow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FgmNode.
 *
 * The RMI control handlers (startNewRound/subroundEnded/roundEnded) delegate to
 * command objects;
 * we assert FgmNode wires state through, not the commands' internals.
 */
class FgmNodeTest {

    @SuppressWarnings("unchecked")
    private FgmNodeStatistics<Vector> stats = mock(FgmNodeStatistics.class);
    private BaseConfiguration cfg = mock(BaseConfiguration.class);
    private FgmNode<Vector> node;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        IFgmFactory<Vector> factory = mock(IFgmFactory.class);
        when(factory.getBaseCfg()).thenReturn(cfg);
        // Constructor reads these for the (doomed, swallowed) registry lookup.
        when(cfg.getRmiHostName()).thenReturn("localhost");
        when(cfg.getRemoteStorePort()).thenReturn(1099);
        when(cfg.getRemoteStoreBindName()).thenReturn("none");
        // Logging path reads a topic name and node id.
        when(cfg.getWorkerLogTopic()).thenReturn("workerLog");
        when(stats.getNodeId()).thenReturn("fgmTest-01");

        node = new FgmNode<>(factory, stats);
    }

    // ---- data-plane update: window branch vs sketch branch ----

    @Test
    void update_withWindowConfigured_routesToWindowAndSetsMaxTimestamp() {
        CustomWindow<Vector> window = mock(CustomWindow.class);
        when(stats.getWindow()).thenReturn(window);
        StreamRecord rec = mock(StreamRecord.class);

        node.update(rec, 1.0, 5000L);

        verify(window).update(rec, 1.0, 5000L);
        verify(stats).setMaxTimeStamp(5000L);
        verify(stats, never()).getSi();          // sketch branch must NOT run
    }

    @Test
    @SuppressWarnings("unchecked")
    void update_withoutWindow_routesToSketchDirectly() {
        when(stats.getWindow()).thenReturn(null);
        LocalStatistic<Vector> si = mock(LocalStatistic.class);
        when(stats.getSi()).thenReturn(si);
        StreamRecord rec = mock(StreamRecord.class);

        node.update(rec, 2.0, 7000L);

        verify(si).update(rec, 2.0);             // cash-register path
        verify(stats, never()).setMaxTimeStamp(anyLong());
    }

    @Test
    void update_withNullRecord_isNoOp() {
        node.update(null, 1.0, 1L);
        verify(stats, never()).getWindow();
        verify(stats, never()).setMaxTimeStamp(anyLong());
    }

    // ---- warm-up staging + flush ----

    @Test
    void warmUpStateVector_withWindow_stagesAndAdvancesTimestamp() {
        CustomWindow<Vector> window = mock(CustomWindow.class);
        when(stats.getWindow()).thenReturn(window);
        StreamRecord rec = mock(StreamRecord.class);

        node.warmUpStateVector(rec, 1.0, 9000L);

        verify(window).update(rec, 1.0, 9000L);
        verify(stats).setMaxTimeStamp(9000L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void warmUpStateVector_withoutWindow_writesSketch_andNullTimestampSkipsMaxTs() {
        when(stats.getWindow()).thenReturn(null);
        LocalStatistic<Vector> si = mock(LocalStatistic.class);
        when(stats.getSi()).thenReturn(si);
        StreamRecord rec = mock(StreamRecord.class);

        node.warmUpStateVector(rec, 3.0, null);   // cash-register uses null ts

        verify(si).update(rec, 3.0);
        verify(stats, never()).setMaxTimeStamp(anyLong());
    }

    @Test
    void warmUpSlideFlush_withWindow_drivesSlidePunctuate() {
        CustomWindow<Vector> window = mock(CustomWindow.class);
        when(stats.getWindow()).thenReturn(window);

        node.warmUpSlideFlush(12345L);

        verify(window).slidePunctuate(12345L);
    }

    @Test
    void warmUpSlideFlush_withoutWindow_isNoOp() {
        when(stats.getWindow()).thenReturn(null);
        assertDoesNotThrow(() -> node.warmUpSlideFlush(12345L));
        // nothing to verify beyond "did not throw / did not touch a window"
    }

    // ---- pure predicate: checkIfLocalCounterIsIncreased ----

    @Test
    void checkIfLocalCounterIsIncreased_trueWhenFloorExceedsCi() {
        // floor((update - zi)/theta) = floor((10 - 0)/2) = 5; ci = 3 -> 3 < 5 -> true
        assertTrue(node.checkIfLocalCounterIsIncreased(3, 0.0, 2.0, 10.0));
    }

    @Test
    void checkIfLocalCounterIsIncreased_falseWhenCiAtFloor() {
        // floor((10 - 0)/2) = 5; ci = 5 -> 5 < 5 -> false
        assertFalse(node.checkIfLocalCounterIsIncreased(5, 0.0, 2.0, 10.0));
    }

    // ---- stream-time punctuation predicate ----

    @Test
    void streamTimePunctuation_fires_andAdvancesLastPunctuation_whenGapReachesSlide() {
        when(cfg.getSlideSizeMillis()).thenReturn(1000L);
        when(stats.getMaxTimeStamp()).thenReturn(5000L);
        when(stats.getLastTimeStampPunctuation()).thenReturn(3000L); // gap 2000 >= 1000

        boolean fired = node.streamTimePunctuation();

        assertTrue(fired);
        verify(stats).setLastTimeStampPunctuation(5000L); // advanced to maxTs
    }

    @Test
    void streamTimePunctuation_doesNotFire_whenGapBelowSlide() {
        when(cfg.getSlideSizeMillis()).thenReturn(1000L);
        when(stats.getMaxTimeStamp()).thenReturn(3500L);
        when(stats.getLastTimeStampPunctuation()).thenReturn(3000L); // gap 500 < 1000

        boolean fired = node.streamTimePunctuation();

        assertFalse(fired);
        verify(stats, never()).setLastTimeStampPunctuation(anyLong());
    }

    // ---- lock-protected accessor pass-through ----

    @Test
    void accessors_delegateToState() {
        when(stats.getCurrentRound()).thenReturn(7);
        when(stats.getCurrentSubRound()).thenReturn(2);
        when(stats.getTheta()).thenReturn(-0.5);
        when(stats.getCi()).thenReturn(4);
        when(stats.getZi()).thenReturn(1.25);
        when(stats.isThetaEqualsZero()).thenReturn(true);

        assertEquals(7, node.getCurrentRound());
        assertEquals(2, node.getCurrentSubRound());
        assertEquals(-0.5, node.getTheta());
        assertEquals(4, node.getCi());
        assertEquals(1.25, node.getZi());
        assertTrue(node.isThetaEqualsZero());

        node.setCi(9);
        node.setThetaEqualsZero(false);
        verify(stats).setCi(9);
        verify(stats).setThetaEqualsZero(false);
    }

    @Test
    void getT_computesQueryOverEstimate() {
        @SuppressWarnings("unchecked")
        QueryFunction<Vector> qf = mock(QueryFunction.class);
        @SuppressWarnings("unchecked")
        GlobalStatistic<Vector> e = mock(GlobalStatistic.class);
        when(stats.getQueryFunction()).thenReturn(qf);
        when(stats.getE()).thenReturn(e);
        when(qf.computeQuery(e)).thenReturn(42.0);

        assertEquals(42.0, node.getT());
        verify(qf).computeQuery(e);
    }

    // ---- first-drift once-only guard ----

    @Test
    void sendFirstDriftMessage_whenAlreadySent_doesNothing() {
        when(stats.tryMarkFirstDriftSent()).thenReturn(false); // already sent

        node.sendFirstDriftMessage();

        // Guard returns immediately: no drift vector read, no producer used.
        verify(stats, never()).getDrift();
        verify(stats, never()).getDriftMessagesProducer();
    }

    @Test
    void ping_returnsTrue() throws Exception {
        assertTrue(node.ping());
    }
}
