package fgm.coordinator.collectorsinterfaces;

import fgm.common.messages.IncrementMessage;

public interface IncrementsCollectorInterface {
    void notifyNodesForSubroundEnded();

    void collectIncrements(IncrementMessage increment);
}
