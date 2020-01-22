package app_kvServer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Disk {
    private static final String KV_STORE_FILE = "kv_store.txt";
    private static ReadWriteLock RW_LOCK = new ReentrantReadWriteLock();

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

        } finally {
            writeLock.unlock();
        }
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
            // do nothing for now - write to ERROR log
        } finally {
            read_lock.unlock();
        }
        return value;
    }

    /**
     * Given a key, value pair:
     *  - Inserts new pair if key does not exist
     *  - Updates existing pair if key exists
     *
     * @param key
     * @param value
     */
    public static void putKV(String key, String value) {
        // Create KV Store File if DNE
        createKVStoreFile();

        Lock writeLock = RW_LOCK.writeLock();
        writeLock.lock();
        List<String> kvPairs = new ArrayList<String>();
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

            // Write all lines to file if exists
            // If does not exist, just append to the file
            FileWriter fw;
            BufferedWriter  writer;
            if (exists) { // case 1: key already exists - need to overwrite all lines
                fw = new FileWriter(KV_STORE_FILE);
                writer = new BufferedWriter(fw);
                for (String pair : kvPairs) {
                    writer.write(pair);
                    writer.newLine();
                }
            } else { // case 2: key DNE, just append to end of file
                fw = new FileWriter(KV_STORE_FILE, true);
                writer = new BufferedWriter(fw);
                writer.write(formatKVPair(key, value));
                writer.newLine();
            }

            writer.close();
            fw.close();
        } catch (Exception ex) {
            // do nothing for now
            System.out.println("AAH ERROR");
        } finally {
            writeLock.unlock();
        }

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
            FileWriter fw = new FileWriter(KV_STORE_FILE);
            BufferedWriter writer = new BufferedWriter(fw);
            writer.write("");

            writer.close();
            fw.close();
        } catch (Exception ex) {
            // do nothing for now
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
        System.out.println(getKV("goodbye"));
        clearStorage();
    }
}
