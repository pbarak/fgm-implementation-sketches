package fgm.coordinator.collectorsinterfaces;

import fgm.common.messages.DriftVectorMessage;

public interface DriftCollectorInterface {
    void updateGlobalEstimate(DriftVectorMessage stateMessage);

    void notifyNodesForNewRound();
}
