package fgm.utils;


import java.io.*;

/**
 * Clone Utilities Class
 */
public class SerializationUtil {

    /**
     * Deep copies an object
     */
    public static <T extends Serializable> T deepCopy(T source) {
        return deserialize(serialize(source));
    }

    public static <T extends Serializable> byte[] serialize(T source) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(bos);
            oos.writeObject(source);
            oos.flush();
            oos.close();
            bos.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot Serialize Object", e);
        }
        return bos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T deserialize(byte[] byteData) {
        ByteArrayInputStream bais = new ByteArrayInputStream(byteData);
        Object o = null;
        try {
            o = new ObjectInputStream(bais).readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Cannot Deserialize Object", e);
        }
        return (T) o;
    }


    /**
     * Returns the serialized payload size in bytes. Uses standard Java
     * serialization, which is also what RMI uses on the wire, so the
     * returned figure corresponds to the actual protocol payload that
     * the JVM was about to put on the network.
     *
     * Returns 0 for null and for objects that fail to serialize
     */
    public static long bytesOf(Object payload) {
        if (payload == null) return 0L;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(payload);
            oos.flush();
            return bos.size();
        } catch (Exception e) {
            return 0L;
        }
    }

}
