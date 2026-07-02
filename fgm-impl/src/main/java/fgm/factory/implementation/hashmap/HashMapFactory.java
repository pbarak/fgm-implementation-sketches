package fgm.factory.implementation.hashmap;

import com.fasterxml.jackson.databind.JsonDeserializer;
import fgm.common.cfg.BaseConfiguration;
import fgm.common.cfg.implementation.BaseCfgImpl;
import fgm.common.messages.DriftVectorMessage;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.PhiMessage;
import fgm.common.serde.JsonPOJOSerializer;
import fgm.common.serde.records.StreamRecordInterfaceDeserializer;
import fgm.common.serde.statistics.StatisticsContentDeserializer;
import fgm.common.serde.statistics.StatisticsMessageSerializer;
import fgm.common.serde.vectors.DriftVectorMessageSerializer;
import fgm.common.statevector.HashMapState;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.driftcollector.DriftCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.increment.IncrementCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.collectors.phiscollector.PhisCollectionProcessor;
import fgm.coordinator.collectorsinterfaces.implementation.processors.statestore.CoordinatoreStateStoreBuilder;
import fgm.messageproducer.IMessageProducer;
import fgm.messageproducer.implementation.KafkaMessageProducer;
import fgm.queryfunction.implementation.NormHashMapStateQFunction;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmGlobalStatistic;
import fgm.common.messages.StatisticsMessage;
import fgm.common.GlobalStatistic;
import fgm.safefunction.implementation.NormSafeFunctionHashMapState;
import fgm.statistics.VectorStatisticsProcessor;
import fgm.timestampextractor.WorldCupExtractor;
import fgm.worker.messagesender.IMessageSender;
import fgm.worker.messagesender.implementation.DiscreteMessageSender;
import fgm.worker.processor.cashregister.WorkerProcessor;
import fgm.worker.processor.window.WorkerWindowProcessor;
import fgm.worker.remoteobject.implementation.FgmNode;
import fgm.worker.remoteobject.implementation.FgmNodeStatistics;
import fgm.factory.specs.IFgmFactory;
import fgm.queryfunction.QueryFunction;
import fgm.safefunction.SafeFunction;
import fgm.worker.windowoperator.Window;
import fgm.worker.statistics.LocalStatistic;
import fgm.worker.statistics.implementation.state.FgmLocalStatistic;
import fgm.streamdata.StreamRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.Properties;

public class HashMapFactory implements IFgmFactory<HashMapState> {

    private static final Logger logger = LoggerFactory.getLogger(HashMapFactory.class);


    private static volatile FgmNodeStatistics<HashMapState> sharedState;
    private static volatile FgmNode<HashMapState>   sharedNode;

    public LocalStatistic<HashMapState> getLocalStatistic() {
        try {
            HashMapState keyFrequencyList = new HashMapState();
            return new FgmLocalStatistic<>(keyFrequencyList);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public FgmNode<HashMapState> getFgmNode() {
        try {
            logger.info("Getting shared node");
            FgmNode<HashMapState> local = sharedNode;
            if (local == null) {
                synchronized (this) {
                    if (sharedNode == null) {
                        sharedState = new FgmNodeStatistics<>(this);          // one shared state object
                        sharedNode = new FgmNode<>(this, sharedState);  // one shared node wrapper
                    }
                    local = sharedNode;
                }
            }
            return local;
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    public String getId() {
        return null;
    }

    public Window<HashMapState> getWindowOperator() {
        return null;
    }

    public GlobalStatistic<HashMapState> getGlobalStatistic() {
        return new FgmGlobalStatistic<>(new HashMapState());
    }

    @Override
    public QueryFunction<HashMapState> getQueryFunction() {
        return new NormHashMapStateQFunction<>();
    }

    public SafeFunction<HashMapState> getSafeFunction() {
        return new NormSafeFunctionHashMapState<>();
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

    @Override
    public BaseConfiguration getBaseCfg() {
        return new BaseCfgImpl();
    }

    public JsonDeserializer<StreamRecord> getRecordDeserializer() {
        return  new StreamRecordInterfaceDeserializer();
    }

    public JsonDeserializer<StatisticsMessage> getStatisticsMessageDeserializer() {
        return new StatisticsContentDeserializer();
    }

    public IMessageProducer<String, String> getLogsProducer() {
        return new KafkaMessageProducer<>(createLogsProducer());
    }

    public Class<?> getTimestampExtractor(){
        return WorldCupExtractor.class;
    }

    public IMessageSender getMessageSender() {
        return new DiscreteMessageSender();
    }

    public Processor<StreamRecord, Double> getStreamRecordsProcessor(String id, int srcTopicPartitionsNum) {
        if(this.getBaseCfg().hasWindow()){
            return new WorkerWindowProcessor(id, this.getBaseCfg(), this.getMessageSender(), getFgmNode(), srcTopicPartitionsNum);
        } else {
            return new WorkerProcessor(id, this, srcTopicPartitionsNum);
        }
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

    @Override
    public Processor<String, StatisticsMessage> getStatisticsProcessor(IFgmFactory<?> factory) {
        return new VectorStatisticsProcessor(this);
    }

    private Producer<String, StatisticsMessage> createStatisticsMessagesProducer() {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", getBaseCfg().getBootstrapServers());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StatisticsMessageSerializer.class.getName());
        properties.put("enable.auto.commit", "true");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);

        return new KafkaProducer<>(properties);
    }

    public IMessageProducer<String, StatisticsMessage> getStatisticsMessageProducer() {
        return new KafkaMessageProducer<>(createStatisticsMessagesProducer());
    }

    private Producer<String, DriftVectorMessage> createDriftMessagesProducer() {

        Properties properties = new Properties();

        properties.put("bootstrap.servers", getBaseCfg().getBootstrapServers());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", DriftVectorMessageSerializer.class.getName());
        properties.put("enable.auto.commit", "true");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);

        return new KafkaProducer<>(properties);
    }

    private Producer<String, PhiMessage> createPhiMessagesProducer() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", getBaseCfg().getBootstrapServers());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", JsonPOJOSerializer.class.getName());
        properties.put("enable.auto.commit", "true");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);

        return new KafkaProducer<>(properties);

    }

    public Producer<String, IncrementMessage> createIncrementMessagesProducer() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", getBaseCfg().getBootstrapServers());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", JsonPOJOSerializer.class.getName());
        properties.put("enable.auto.commit", "true");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);

        return new KafkaProducer<>(properties);
    }

    public Producer<String, String> createLogsProducer() {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", getBaseCfg().getBootstrapServers());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("enable.auto.commit", "true");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);

        return new KafkaProducer<>(properties);
    }
}


