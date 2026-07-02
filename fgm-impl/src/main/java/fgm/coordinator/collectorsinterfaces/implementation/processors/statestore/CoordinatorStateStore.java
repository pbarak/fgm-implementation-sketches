package fgm.coordinator.collectorsinterfaces.implementation.processors.statestore;


import fgm.common.Vector;
import fgm.factory.specs.IFgmFactory;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmCoordinatorState;

public class CoordinatorStateStore<T extends Vector> implements StateStore {
    private boolean open = false;

    //the internal state of coordinator is stored in this state store.
    //check CoordinatorApplication how the state store of the coordinator is instantiated
    private final FgmCoordinatorState<T> fgmCoordinatorState;
    private final String name;
    boolean loggingEnabled;
    int id;

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        context.register(root, new StateRestoreCallback() {
            @Override
            public void restore(byte[] bytes, byte[] bytes1) {
            }
        });
        this.open = true;

    }

    public FgmCoordinatorState<T> getFgmCoordinatorState() {
        return this.fgmCoordinatorState;
    }

    public CoordinatorStateStore(String name, boolean loggingEnabled, int id, IFgmFactory<T> factory) {
        this.id = id;
        this.name = name;
        this.loggingEnabled = loggingEnabled;
        this.fgmCoordinatorState = new FgmCoordinatorState<>(factory);

    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        this.open = false;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public boolean persistent() {
        return false;
    }
}
