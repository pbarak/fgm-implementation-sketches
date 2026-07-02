package fgm.configuration;

import fgm.common.cfg.BaseConfiguration;
import fgm.configuration.FilePropertiesLocalStore;
import org.apache.kafka.streams.processor.PunctuationType;

public class CfgRetrieval {
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String WINDOW_MS_WIDTH = "window.ms.width";
    private static final String WINDOW_ADVANCE_MS = "window.advance.ms";

    FilePropertiesLocalStore filePropertiesLocalStore;

    public CfgRetrieval(FilePropertiesLocalStore filePropertiesLocalStore) {
        if (filePropertiesLocalStore == null) {
            this.filePropertiesLocalStore = FilePropertiesLocalStore.getInstance();
        }
    }

    public FilePropertiesLocalStore getFilePropertiesLocalStore() {
        return filePropertiesLocalStore;
    }

    public CfgRetrieval() {
        this.filePropertiesLocalStore = FilePropertiesLocalStore.getInstance();
    }

    public String getBootstrapServers() {
        return filePropertiesLocalStore.retrieveString(BOOTSTRAP_SERVERS);
    }

    public Long getWindowMillisWidth() {
        return filePropertiesLocalStore.retrieveLong(WINDOW_MS_WIDTH);
    }

    public Long getWindowAdvanceMillis() {
        return filePropertiesLocalStore.retrieveLong(WINDOW_ADVANCE_MS);
    }

}
