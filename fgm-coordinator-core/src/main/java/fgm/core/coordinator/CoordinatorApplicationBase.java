package fgm.core.coordinator;

import fgm.common.messages.DriftVectorMessage;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.PhiMessage;
import fgm.coordinator.collectorsinterfaces.implementation.processors.statestore.CoordinatoreStateStoreBuilder;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;
import fgm.common.cfg.BaseConfiguration;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;

public class CoordinatorApplicationBase {
    static final String DRIFT_MESSAGES_SOURCE_NAME = "driftMessages-source";
    static final String PHI_MESSAGES_SOURCE_NAME = "phiMessages-source";
    static final String INCREMENTS_SOURCE_NAME = "incrementMessages-source";
    static final String DRIFT_MESSAGES_PROCESSOR_NAME = "driftMessages-processor";
    static final String PHI_MESSAGES_PROCESSOR_NAME = "phiMessages-processor";
    static final String INCREMENT_PROCESSOR_NAME = "incrementMessages-processor";
    static final String SINK_NAME = "log-sink";
    static final String SHUT_DOWN_HOOK = "streams-shutdown-hook";
    static final String JSON_SERDES_CLASS_NAME = "JsonPOJOClass";

    /** Holds everything the app needs after bootstrapping. */
    static final class BootContext {
        public final CoordinatoreStateStoreBuilder<?> customStoreBuilder;
        public final IFgmFactory<?> factory;
        public final BaseConfiguration baseConfiguration;
        public final Topology topology;
        public final String remoteStoreUrl;

        BootContext(CoordinatoreStateStoreBuilder<?> customStoreBuilder,
                    IFgmFactory<?> factory,
                    BaseConfiguration baseConfiguration,
                    Topology topology,
                    String remoteStoreUrl) {
            this.customStoreBuilder = customStoreBuilder;
            this.factory = factory;
            this.baseConfiguration = baseConfiguration;
            this.topology = topology;
            this.remoteStoreUrl = remoteStoreUrl;
        }
    }

    /** Centralized bootstrap: reads factory, builds store builder, base cfg, topology, and RMI URL. */
    static BootContext boot() {
        IFgmFactory<?> factory = FactoryInjection.createFactory();
        CoordinatoreStateStoreBuilder<?> customStoreBuilder = factory.createGenericCoordinatorClient();

        BaseConfiguration baseConfiguration = factory.getBaseCfg();
        Topology topology = new Topology();

        String remoteStoreUrl = "rmi://" + baseConfiguration.getRmiHostName()
                + ":" + baseConfiguration.getRemoteStorePort()
                + "/" + baseConfiguration.getRemoteStoreBindName();

        return new BootContext(
                customStoreBuilder,
                factory,
                baseConfiguration,
                topology,
                remoteStoreUrl
        );
    }

    static Processor<String, IncrementMessage> getIncrementProcessor(BootContext ctx) {
        return ctx.factory.getIncrementProcessor(ctx.customStoreBuilder, ctx.remoteStoreUrl, ctx.factory.getMessageSender());
    }

    static Processor<String, DriftVectorMessage> getDriftCollectionProcessor(BootContext ctx) {
        return ctx.factory.getDriftCollectionProcessor(ctx.customStoreBuilder.name(), ctx.remoteStoreUrl,
                ctx.factory.getMessageSender(), ctx.factory.getQueryFunction());
    }

    static Processor<String, PhiMessage> getPhisCollectionProcessor(BootContext ctx) {
        return ctx.factory.getPhisCollectionProcessor(ctx.customStoreBuilder.name(), ctx.remoteStoreUrl,
                ctx.factory.getMessageSender(), ctx.factory.getQueryFunction());
    }
}
