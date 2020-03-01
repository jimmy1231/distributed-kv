package app_kvServer;

import shared.Pair;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Disk {
    private static final String KV_STORE_FILE = String.format("kvstore_%s.txt", UUID.randomUUID().toString());
    private static ReadWriteLock RW_LOCK = new ReentrantReadWriteLock();
    private static Logger logger = Logger.getLogger(Disk.class);

    public Disk() {
        // do nothing for now
    }

    /**
     * Given key and value, formats the pair into the following
     * format to be stored in the kv_store.txt file:
     *      <key> <value>
     *  Key + value separated by a space
     *  NOTE: if there are any design changes to the persistent
     *  storage format, only this function should change
     *
     * @param key
     * @param value
     * @return
     */
    private static String formatKVPair(String key, String value) {
        return key + " " + value;
    }

    /**
     * Creates kv_store.txt file if DNE
     */
    private static void createKVStoreFile() {
        Lock writeLock = RW_LOCK.writeLock();
        writeLock.lock();
        try {
            File file = new File(KV_STORE_FILE);
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (Exception ex) {
            logger.error("Cannot create persistent storage file!");
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Updates KV-pair with key = /key/.
     * kvPairs list will include all KV pairs to be written to persistent
     * storage including the updated pair.
     *
     * Returns true if /key/ exists in storage.
     *
     * @param key
     * @param kvPairs
     * @return
     */
    private static boolean updateKV(String key, String value, List<String> kvPairs) {
        boolean exists = false;
        try {
            FileReader fr = new FileReader(KV_STORE_FILE);
            BufferedReader reader = new BufferedReader(fr);
            String line;
            String []kvPair;
            while ((line = reader.readLine()) != null) {
                // split line into [ key, value ]
                kvPair = line.split(" ", 2);

                // found a match
                if (kvPair[0].equals(key)) {
                    exists = true;
                    line = formatKVPair(key, value);
                }
                kvPairs.add(line);
            }
            reader.close();
            fr.close();
        } catch (Exception ex) {
            logger.error("Error reading kv_store.txt: " + ex.getMessage());
        }
        return exists;
    }

    /**
     * Deletes /key/ pair from persistent storage.
     * kvPairs will include all KV pairs to be written to
     * persistent storage (excluding /key/).
     *
     * Returns true if /key/ exists in storage.
     *
     * @param key
     * @param kvPairs
     * @return
     */
    private static boolean deleteKV(String key, List<String> kvPairs) {
        boolean exists = false;
        try {
            FileReader fr = new FileReader(KV_STORE_FILE);
            BufferedReader reader = new BufferedReader(fr);
            String line;
            String []kvPair;
            while ((line = reader.readLine()) != null) {
                // split line into [ key, value ]
                kvPair = line.split(" ", 2);

                // only add kv-pair if it doesn't match key param
                if (!kvPair[0].equals(key)) {
                    kvPairs.add(line);
                } else {
                    exists = true;
                }

            }
            reader.close();
            fr.close();
        } catch (Exception ex) {
            logger.error("Error reading kv_store.txt: " + ex.getMessage());
        }
        return exists;
    }

    /**
     * Returns all key-value pairs that are stored on disk
     * in a List of Pairs.
     *
     * @return
     */
    public static List<Pair<String, String>> getAll() {
        // Create KV Store File if DNE
        createKVStoreFile();

        Lock read_lock = RW_LOCK.readLock();
        read_lock.lock();

        List<Pair<String, String>> entries = new ArrayList<>();
        String value = null;
        try {
            FileReader fr = new FileReader(KV_STORE_FILE);
            BufferedReader reader = new BufferedReader(fr);
            String line;
            String []kvPair;
            while ((line = reader.readLine()) != null) {
                // split line into [ key, value ]
                kvPair = line.split(" ", 2);
                entries.add(new Pair<>(kvPair[0], kvPair[1]));
            }

        } catch (Exception ex) {
            logger.error("Error reading kv_store.txt: " + ex.getMessage());
        } finally {
            read_lock.unlock();
        }

        return entries;
    }


    /**
     * Given a key:
     *  - Returns value of KV pair if key exists in file
     *  - Returns null if key DNE
     *
     *  Searches through all of the lines in the KV_STORE_FILE
     *  until it finds the requested key or reaches EOF.
     *
     * @param key
     * @return value of KV pair
     */
    public static String getKV(String key) {
        // Create KV Store File if DNE
        createKVStoreFile();

        Lock read_lock = RW_LOCK.readLock();
        read_lock.lock();

        String value = null;
        try {
            FileReader fr = new FileReader(KV_STORE_FILE);
            BufferedReader reader = new BufferedReader(fr);
            String line;
            String []kvPair;
            while ((line = reader.readLine()) != null) {
                // split line into [ key, value ]
                kvPair = line.split(" ", 2);

                // found a match
                if (kvPair[0].equals(key)) {
                    value = kvPair[1];
                    break;
                }
            }

        } catch (Exception ex) {
            logger.error("Error reading kv_store.txt: " + ex.getMessage());
        } finally {
            read_lock.unlock();
        }
        return value;
    }

    /**
     * Given a key, value pair:
     *  - Inserts new pair if key does not exist
     *  - Updates existing pair if key exists
     *  - If value param is null, delete the KV pair if exists
     *
     * @param key
     * @param value
     */
    public static boolean putKV(String key, String value) {
        logger.info("PUTKV REQUEST FOR: { " + key + ", " + value + " }");
        // Create KV Store File if DNE
        createKVStoreFile();

        Lock writeLock = RW_LOCK.writeLock();
        writeLock.lock();
        List<String> kvPairs = new ArrayList<String>();
        boolean exists = false;
        try {
            /*
             * Determine whether a kv-pair exists.
             * kvPairs stores a list of all of the kvPairs to
             * be written to the persistent storage file.
             *  (1) If pair is to be deleted, the pair will not be
             *      included in the kvPairs list.
             *  (2) If pair is to be updated, updated value will be
             *      reflected in the kvPairs list.
             */
            if (value == null) {
                exists = deleteKV(key, kvPairs);
                if (exists)
                    logger.debug("DELETING: { " + key + " }");
            } else {
                exists = updateKV(key, value, kvPairs);
                if (exists)
                    logger.debug("UPDATING: { " + key + " } -> " + "{ " + key + ", " + value + " }");
            }

            /*
             * Write all lines to file if exists
             * If does not exist, just append to the file
             */
            FileWriter fw = null;
            BufferedWriter  writer = null;
            if (exists) { // case 1: key already exists - need to overwrite all lines
                fw = new FileWriter(KV_STORE_FILE);
                writer = new BufferedWriter(fw);
                for (String pair : kvPairs) {
                    writer.write(pair);
                    writer.newLine();
                }
            } else if (value != null) { // case 2: key DNE, just append to end of file
                logger.debug("INSERTING: { " + key + ", " + value + " }");

                fw = new FileWriter(KV_STORE_FILE, true);
                writer = new BufferedWriter(fw);
                writer.write(formatKVPair(key, value));
                writer.newLine();
            }

            /* Cleanup - close writers */
            if (writer != null)
                writer.close();
            if (fw != null)
                fw.close();
        } catch (Exception ex) {
            logger.error("Error writing to kv_store.txt: " + ex.getMessage());
        } finally {
            writeLock.unlock();
        }
        return exists;
    }

    /**
     * Given a key:
     *  - returns true if key is in persistent storage
     *  - false otherwise
     *
     * @param key
     * @return
     */
    public static boolean inStorage(String key) {
        String val = getKV(key);
        return val != null;
    }

    /**
     * Clears all contents of persistent storage
     * Simply writes an empty string to the file.
     */
    public static void clearStorage() {
        // Create KV Store File if DNE
        createKVStoreFile();

        Lock writeLock = RW_LOCK.writeLock();
        writeLock.lock();
        try {
            logger.info("Clearing kv_store.txt");

            FileWriter fw = new FileWriter(KV_STORE_FILE);
            BufferedWriter writer = new BufferedWriter(fw);
            writer.write("");

            writer.close();
            fw.close();
        } catch (Exception ex) {
            logger.error("Error clearing storage: " + ex.getMessage());
        } finally {
            writeLock.unlock();
        }
    }

    /* For debugging */
    public static void main(String[] args) {
        putKV("key", "value");
        System.out.println(getKV("key"));
        putKV("key", "value_2");
        System.out.println(getKV("key"));
        putKV("hello", "world how are you 38108301");
        System.out.println(getKV("hello"));
        putKV("hello", null);
        System.out.println(getKV("hello"));
        putKV("jello", null);
        System.out.println(getKV("jello"));
        clearStorage();
    }
}
