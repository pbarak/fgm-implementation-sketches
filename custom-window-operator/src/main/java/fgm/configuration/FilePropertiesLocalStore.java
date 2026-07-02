package fgm.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class FilePropertiesLocalStore {

    private static final String RESOURCE_FILE = "custom-window.properties";

    Logger logger = Logger.getLogger(FilePropertiesLocalStore.class.getName());
    private Properties props;

    private static final FilePropertiesLocalStore INSTANCE = new FilePropertiesLocalStore();

    public FilePropertiesLocalStore() {
        loadProperties();
    }

    private void loadProperties() {
        try {
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream input = classloader.getResourceAsStream(RESOURCE_FILE);
            props = new Properties();
            // load a properties file
            props.load(input);
            System.out.println("props size is " + props.size());
        } catch (IOException ex) {
            logger.info(ex.toString());
        }
    }

    public static FilePropertiesLocalStore getInstance() {
        return INSTANCE;
    }

    public void store(String key, Object value) {
        props.put(key, value);
    }

    public Object retrieveProperty(String key) {
        return props.get(key);
    }

    public String retrieveString(String key) {
        return props.get(key).toString();
    }

    public Integer retrieveInt(String key) {
        return Integer.parseInt(props.get(key).toString());
    }

    public Double retrieveDouble(String key) {
        return Double.parseDouble(props.get(key).toString());
    }

    public Long retrieveLong(String key) {
        return Long.parseLong(props.get(key).toString());
    }

}
