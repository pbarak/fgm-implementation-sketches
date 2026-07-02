//package fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.xiscollector;
//
//import fgm.common.GlobalStatistic;
//import fgm.common.messages.DriftVectorMessage;
//import fgm.common.vector.FastAGMS;
//import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.CoordinatorMetrics;
//import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmCoordinatorState;
//import fgm.coordinator.collectorsinterfaces.implementation.processors.statestore.CoordinatorStateStore;
//import fgm.factory.CoordinatorDependencyInjection;
//import fgm.factory.specs.IFgmFactory;
//import fgm.worker.messagesender.implementation.DiscreteMessageSender;
//import fgm.worker.remoteobject.NodeRMICollectionInterface;
//import fgm.worker.remoteobject.NodeRMIInterface;
//import org.apache.kafka.streams.processor.api.ProcessorContext;
//import org.apache.kafka.streams.processor.api.Record;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.*;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//import java.lang.reflect.Field;
//import java.util.ArrayList;
//import java.util.Arrays;
//
//import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
//import static org.mockito.ArgumentMatchers.*;
//import static org.mockito.Mockito.*;
//
//@ExtendWith(MockitoExtension.class)
//class XisCollectionProcessorTest {
//
//    private static final String STORE_NAME = "coordinator-state-store";
//
//    @Mock ProcessorContext<String, String> context;
//    @Mock CoordinatorStateStore<FastAGMS> coordinatorStateStore;
//
//    // Deep stubs so calls like getXisCollection().getXis() don't NPE
//    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
//    FgmCoordinatorState<FastAGMS> coordinatorState;
//
//    @Mock IFgmFactory<FastAGMS> factory;
//    @Mock GlobalStatistic<FastAGMS> globalStatistic;
//
//    @Mock NodeRMICollectionInterface remoteCollection;
//    @Mock NodeRMIInterface<FastAGMS> node1;
//    @Mock NodeRMIInterface<FastAGMS> node2;
//
//    @Captor ArgumentCaptor<Record<String, String>> recordCaptor;
//
//    XisCollectionProcessor processor;
//
//    @BeforeEach
//    void setUp() throws Exception {
//        // 1) Provide a real sender that delegates to context.forward(...)
//        when(factory.getMessageSender()).thenReturn(new DiscreteMessageSender());
//
//        when(context.getStateStore(STORE_NAME)).thenReturn(coordinatorStateStore);
//        when(coordinatorStateStore.getFgmCoordinatorState()).thenReturn(coordinatorState);
//
//        when(coordinatorState.getE()).thenReturn(globalStatistic);
//        // 2) Avoid Double -> double NPE
//        processor = Mockito.spy(new XisCollectionProcessor(CoordinatorDependencyInjection.init(), "rmi://noop", factory));
//        processor.init(context);
//
//        // Inject the RMI collection
//        setPrivate(processor, "remoteStoreCollection", remoteCollection);
//    }
//
//    private static void setPrivate(Object target, String field, Object value) throws Exception {
//        Field f = XisCollectionProcessor.class.getDeclaredField(field);
//        f.setAccessible(true);
//        f.set(target, value);
//    }
//
//    @Test
//    void init_wiresStateStore() {
//        verify(context).getStateStore(STORE_NAME);
//        assertDoesNotThrow(() -> processor.notifyNodesForNewRound());
//    }
//
//    @Test
//    void process_delegatesToUpdateGlobalEstimate() {
//        DriftVectorMessage msg = mock(DriftVectorMessage.class);
//        doNothing().when(processor).updateGlobalEstimate(msg);
//
//        processor.process(new Record<>("k", msg, System.currentTimeMillis()));
//
//        verify(processor).updateGlobalEstimate(msg);
//    }
//
//    @Test
//    void notifyNodesForNewRound_whenNotStarted_initializesAllNodes_andEmitsRound() throws Exception {
//        when(coordinatorState.getNumberOfNodes()).thenReturn(2);
//        when(coordinatorState.getIfStarted()).thenReturn(false);
//        when(remoteCollection.getAll()).thenReturn(Arrays.asList(node1, node2));
//
//        processor.notifyNodesForNewRound();
//
//        verify(node1).initializeFirstRound(any(), anyDouble());
//        verify(node2).initializeFirstRound(any(), anyDouble());
//
//        verify(context, atLeastOnce()).forward(recordCaptor.capture());
//        long round = recordCaptor.getAllValues().stream()
//                .map(Record::value)
//                .filter(v -> v.contains(CoordinatorMetrics.ROUND.toString()))
//                .count();
//        long sent = recordCaptor.getAllValues().stream()
//                .map(Record::value)
//                .filter(v -> v.contains(CoordinatorMetrics.SENT_ESTIMATE.toString()))
//                .count();
//        org.junit.jupiter.api.Assertions.assertEquals(1, round);
//        org.junit.jupiter.api.Assertions.assertEquals(0, sent);
//    }
//
//    @Test
//    void notifyNodesForNewRound_whenStarted_startsAllNodes_andEmitsSentEstimatePerNode_andRoundOnce() throws Exception {
//        when(coordinatorState.getIfStarted()).thenReturn(true);
//        when(coordinatorState.getTheta()).thenReturn(0.0);           // ensure not null
//        when(remoteCollection.getAll()).thenReturn(Arrays.asList(node1, node2));
//        when(coordinatorState.getNumberOfNodes()).thenReturn(2);      // optional, used in key
//
//        processor.notifyNodesForNewRound();
//
//        verify(node1).startNewRound(any(), anyDouble());
//        verify(node2).startNewRound(any(), anyDouble());
//
//        verify(context, atLeastOnce()).forward(recordCaptor.capture());
//        long sent = recordCaptor.getAllValues().stream().map(Record::value)
//                .filter(v -> v.contains(CoordinatorMetrics.SENT_ESTIMATE.toString())).count();
//        long round = recordCaptor.getAllValues().stream().map(Record::value)
//                .filter(v -> v.contains(CoordinatorMetrics.ROUND.toString())).count();
//        org.junit.jupiter.api.Assertions.assertEquals(2, sent);
//        org.junit.jupiter.api.Assertions.assertEquals(1, round);
//    }
//
//    @Test
//    void updateGlobalEstimate_whenPartialXi_doesNotThrow_orEmitDriftOrRound() {
//
//        // Make Xi collection empty AND ensure numberOfNodes != 0
//        when(coordinatorState.getXisCollection().getXis()).thenReturn(new ArrayList<>());
//        when(coordinatorState.getNumberOfNodes()).thenReturn(2); // <-- important
//
//        DriftVectorMessage msg = mock(DriftVectorMessage.class);
//        when(msg.getCurrentRound()).thenReturn(1);
//        when(msg.getCurrentSubround()).thenReturn(0);
//        when(msg.getSiteId()).thenReturn("site-1");
//
//        assertDoesNotThrow(() -> processor.updateGlobalEstimate(msg));
//
//        verify(context, never()).forward(argThat(r ->
//                String.valueOf(r.value()).contains(CoordinatorMetrics.RECEIVED_DRIFT.toString())));
//        verify(context, never()).forward(argThat(r ->
//                String.valueOf(r.value()).contains(CoordinatorMetrics.ROUND.toString())));
//    }
//
//}
