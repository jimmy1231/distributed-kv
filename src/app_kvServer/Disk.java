package app_kvServer;

import java.io.File;

public class Disk {
    private static final String KV_FILE = "";

    public Disk() {
        // do nothing for now
    }

    /**
     * Given a key:
     *  - Returns value of KV pair if key exists in file
     *  - Returns null if key DNE
     *
     * @param key
     * @return value of KV pair
     */
    public static String getKV(String key) { return null; }

    /**
     * Given a key, value pair:
     *  - Inserts new pair if key does not exist
     *  - Updates existing pair if key exists
     *
     * @param key
     * @param value
     */
    public static void putKV(String key, String value) { return; }

    /**
     * Given a key:
     *  - returns true if key is in persistent storage
     *  - false otherwise
     *
     * @param key
     * @return
     */
    public static boolean inStorage(String key) { return false; }

    /**
     * Clears all contents of persistent storage
     */
    public static void clearStorage() { }

}
