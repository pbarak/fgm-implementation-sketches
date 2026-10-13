package cfg;


public class CfgRetrieval {
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String WINDOW_CONSUMER_INPUT_TOPIC = "window.consumer.input.topic";
    private static final String WINDOW_CONSUMER_OUTPUT_FILE = "window.consumer.output.file";
    private static final String WINDOW_CONSUMER_WIDTH_MILLIS = "window.consumer.width.millis";

    private static final String HOPPING_WINDOW_SIZE_MINUTES = "hopping.window.size.minutes";
    private static final String HOPPING_WINDOW_ADVANCE_SECONDS = "hopping.window.advance.seconds";
    private static final String HOPING_WINDOW_SIZE_MILLIS = "hopping.window.size.millis";
    private static final String HOPPING_WINDOW_INPUT_TOPIC = "hopping.window.input.topic";
    private static final String HOPPING_WINDOW_OUTPUT_TOPIC =  "hopping.window.output.topic";

    cfg.FilePropertiesLocalStore filePropertiesLocalStore;

    public CfgRetrieval() {
            this.filePropertiesLocalStore = cfg.FilePropertiesLocalStore.getInstance();
    }

    public FilePropertiesLocalStore getFilePropertiesLocalStore() {
        return filePropertiesLocalStore;
    }

    public String getBootstrapServers() {
        return filePropertiesLocalStore.retrieveString(BOOTSTRAP_SERVERS);
    }

    public String getWindowConsumerInputTopic() {return filePropertiesLocalStore.retrieveString(WINDOW_CONSUMER_INPUT_TOPIC); }

    public String getWindowConsumerOutputFile() {
        return filePropertiesLocalStore.retrieveString(WINDOW_CONSUMER_OUTPUT_FILE);
    }

    public long getWindowConsumerWidthMillis() {
        return filePropertiesLocalStore.retrieveLong(WINDOW_CONSUMER_WIDTH_MILLIS);
    }

    public int getHoppingWindowSizeMinutes() {
        return filePropertiesLocalStore.retrieveInt(HOPPING_WINDOW_SIZE_MINUTES);
    }

    public int getHoppingWindowAdvanceSeconds() {
        return filePropertiesLocalStore.retrieveInt(HOPPING_WINDOW_ADVANCE_SECONDS);
    }

    public long getHoppingWindowSizeMillis() {
        return filePropertiesLocalStore.retrieveLong(HOPING_WINDOW_SIZE_MILLIS);
    }

    public String getHoppingWindowInputTopic() {
        return filePropertiesLocalStore.retrieveString(HOPPING_WINDOW_INPUT_TOPIC);
    }

    public String getHoppingWindowOutputTopic() {
        return filePropertiesLocalStore.retrieveString(HOPPING_WINDOW_OUTPUT_TOPIC);
    }
}
