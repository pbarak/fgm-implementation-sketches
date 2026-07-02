package fgm.factory.implementation.fastagms;

import com.fasterxml.jackson.databind.JsonDeserializer;
import fgm.common.cfg.FastAGMSConfiguration;
import fgm.common.cfg.implementation.FastAGMSCfgImpl;
import fgm.common.serde.JsonPOJOSerializer;
import fgm.common.serde.records.StreamRecordInterfaceDeserializer;
import fgm.common.serde.statistics.StatisticsContentDeserializer;
import fgm.common.serde.statistics.StatisticsMessageSerializer;
import fgm.common.serde.vectors.DriftVectorMessageSerializer;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.increment.IncrementCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.phiscollector.PhisCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.driftcollector.DriftCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.statestore.CoordinatoreStateStoreBuilder;
import fgm.messageproducer.IMessageProducer;
import fgm.messageproducer.implementation.KafkaMessageProducer;
import fgm.timestampextractor.WorldCupExtractor;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.messagesender.implementation.DiscreteMessageSender;
import fgm.worker.processor.window.WorkerWindowProcessor;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.worker.windowoperator.Window;
import fgm.worker.windowoperator.implementation.CustomWindow;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.PhiMessage;
import fgm.common.messages.DriftVectorMessage;
import fgm.queryfunction.implementation.FastAGMSQueryFunction;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmGlobalStatistic;
import fgm.common.messages.StatisticsMessage;
import fgm.safefunction.implementation.SelfJoin;
import fgm.worker.remoteobject.implementation.FgmNodeStatistics;
import fgm.common.GlobalStatistic;
import fgm.common.statevector.FastAGMS;
import fgm.factory.specs.IFgmFactory;
import fgm.queryfunction.QueryFunction;
import fgm.safefunction.SafeFunction;
import fgm.statistics.WindowsStatisticsProcessor;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import fgm.worker.statistics.LocalStatistic;
import fgm.worker.statistics.implementation.state.FgmLocalStatistic;
import fgm.streamdata.StreamRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Factory implements IFgmFactory<FastAGMS> {

    private static final Logger logger = LoggerFactory.getLogger(Factory.class);

    private static volatile FgmNodeStatistics<FastAGMS> sharedState;
    private static volatile FgmNode<FastAGMS>   sharedNode;

    public Factory() {
    }

    public FgmNode<FastAGMS> getFgmNode() {
        try {
            FgmNode<FastAGMS> local = sharedNode;
            if (local == null) {
                synchronized (Factory.class) {
                    if (sharedNode == null) {
                        sharedState = new FgmNodeStatistics<>(this);
                        sharedNode = new FgmNode<>(this, sharedState);
                        logger.info("Shared node successfully initialized");
                    }
                    local = sharedNode;
                }
            }
            return local;
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    public Window<FastAGMS> getWindowOperator() {
        try {
            if (Boolean.TRUE.equals(getLocalBaseCfg().hasWindow())) {
                return new CustomWindow<>(this);
            } else {
                return null;
            }
        } catch (IllegalArgumentException ex) {
            logger.info("error in reading from property file");
            System.exit(0);
        }
        return null;
    }

    public LocalStatistic<FastAGMS> getLocalStatistic() {
        FastAGMS fastAGMS = new FastAGMS(this.getBaseCfg().getSketchDepth(), this.getBaseCfg().getSketchBuckets());
        return new FgmLocalStatistic<>(fastAGMS);
    }

    private static final class CfgHolder {
        static final FastAGMSConfiguration INSTANCE = new FastAGMSCfgImpl();
    }

    private static FastAGMSConfiguration cfg() {
        return CfgHolder.INSTANCE;
    }

    private static FastAGMSConfiguration getLocalBaseCfg() {
        return cfg();
    }

    @Override
    public FastAGMSConfiguration getBaseCfg() {
        return new FastAGMSCfgImpl();
    }

    public GlobalStatistic<FastAGMS> getGlobalStatistic() {
        return new FgmGlobalStatistic<>(new FastAGMS(getLocalBaseCfg().getSketchDepth(), getLocalBaseCfg().getSketchBuckets()));
    }

    @Override
    public QueryFunction<FastAGMS> getQueryFunction() {
        return new FastAGMSQueryFunction<>();
    }

    public SafeFunction<FastAGMS> getSafeFunction() {
        return new SelfJoin<>();
    }

    public IMessageProducer<String, DriftVectorMessage> getDriftMessageProducer() {
        return new KafkaMessageProducer<>(createDriftMessagesProducer());
    }

    public IMessageProducer<String, PhiMessage> getPhiMessageProducer() {
        return new KafkaMessageProducer<>(createPhiMessagesProducer());
    }

    public Producer<String, IncrementMessage> getIncrementMessageProducer() {
        return createIncrementMessagesProducer();
    }

    public IMessageProducer<String, String> getLogsProducer() {
        return new KafkaMessageProducer<>(createLogsProducer());
    }

    public JsonDeserializer<StreamRecord> getRecordDeserializer() {
        return new StreamRecordInterfaceDeserializer();
    }

    public JsonDeserializer<StatisticsMessage> getStatisticsMessageDeserializer() {
        return new StatisticsContentDeserializer();
    }

    public IMessageSender getMessageSender() {
        return new DiscreteMessageSender();
    }

    public Processor<StreamRecord, Double> getStreamRecordsProcessor(String id, int srcTopicPartitionsNum) {
        return new WorkerWindowProcessor(id, this.getBaseCfg(), this.getMessageSender(), getFgmNode(), srcTopicPartitionsNum);
    }

    public Processor<String, IncrementMessage> getIncrementProcessor(CoordinatoreStateStoreBuilder<?> customStoreBuilder,
                                                                     String remoteStoreUrl, IMessageSender messageSender){
        return new IncrementCollectionProcessor(customStoreBuilder.name(), remoteStoreUrl, messageSender);
    }

    public Processor<String, PhiMessage> getPhisCollectionProcessor(String stateStoreName, String rmiServerUrl, IMessageSender messageSender,
                                                                    QueryFunction<?> queryFunction){
        return new PhisCollectionProcessor(stateStoreName, rmiServerUrl, messageSender, queryFunction);
    }

    public Processor<String, DriftVectorMessage> getDriftCollectionProcessor(String stateStoreName, String rmiServerUrl, IMessageSender messageSender,
                                                                             QueryFunction<?> queryFunction){
        return new DriftCollectionProcessor(stateStoreName, rmiServerUrl, messageSender, queryFunction);
    }

    public CoordinatoreStateStoreBuilder<?> createGenericCoordinatorClient() {
        return new CoordinatoreStateStoreBuilder<>(this);
    }

    public Processor<String, StatisticsMessage> getStatisticsProcessor(IFgmFactory<?> factory){
        return new WindowsStatisticsProcessor(this);
    }

    public Class<?> getTimestampExtractor(){
        return WorldCupExtractor.class;
    }

    private Producer<String, StatisticsMessage> createStatisticsMessagesProducer() {
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<?> statisticsMessageSerializer = new StatisticsMessageSerializer();
        serdeProps.put("StatisticsMessageClass", StatisticsMessage.class);
        statisticsMessageSerializer.configure(serdeProps, false);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBaseCfg().getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StatisticsMessageSerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return new KafkaProducer<>(properties);
    }

    public IMessageProducer<String, StatisticsMessage> getStatisticsMessageProducer() {
        return new KafkaMessageProducer<>(createStatisticsMessagesProducer());
    }

    private Producer<String, DriftVectorMessage> createDriftMessagesProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBaseCfg().getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DriftVectorMessageSerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return new KafkaProducer<>(properties);
    }

    private Producer<String, PhiMessage> createPhiMessagesProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBaseCfg().getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return new KafkaProducer<>(properties);
    }

    private Producer<String, IncrementMessage> createIncrementMessagesProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBaseCfg().getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return new KafkaProducer<>(properties);
    }

    private Producer<String, String> createLogsProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBaseCfg().getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return new KafkaProducer<>(properties);
    }
}