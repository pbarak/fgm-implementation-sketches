package fgm.coordinator.collectorsinterfaces;

import fgm.common.messages.PhiMessage;

public interface PhisCollectorInterface {
    void collectPhis(PhiMessage phiMessage);

    void notifyNodesForNewSubround(double theta);

    void notifyNodesForRoundEnded();
}
