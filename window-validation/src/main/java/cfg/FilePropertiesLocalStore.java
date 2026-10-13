package cfg;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class FilePropertiesLocalStore {

    Logger logger = Logger.getLogger(FilePropertiesLocalStore.class.getName());
    private Properties props;
    private static final String envPropFileName = "window-validation-env.properties";

    private static final FilePropertiesLocalStore INSTANCE = new FilePropertiesLocalStore();

    public FilePropertiesLocalStore() {
        loadProperties();
    }

    private void loadProperties() {
        try {
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream input = classloader.getResourceAsStream(envPropFileName);
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
        String raw = props.getProperty(key);
        if (raw == null) {
            throw new IllegalStateException("Missing required config property: " + key);
        }
        return raw.trim();
    }

    public Integer retrieveInt(String key) {
        String raw = props.getProperty(key);
        if (raw == null) {
            throw new IllegalStateException("Missing required config property: " + key);
        }
        try {
            return Integer.valueOf(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Config property '" + key + "' is not a valid int: \"" + raw + "\"", e);
        }
    }

    public double retrieveDouble(String key) {
        String raw = props.getProperty(key);
        if (raw == null) {
            throw new IllegalStateException("Missing required config property: " + key);
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "Config property '" + key + "' is not a valid double: \"" + raw + "\"", e);
        }
    }

    public Long retrieveLong(String key) {
        return Long.parseLong(props.get(key).toString());
    }
}
