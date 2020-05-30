package shared.messages;

import ecs.HashRing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import shared.Pair;

import java.lang.reflect.Type;
import java.util.*;

public class KVDataSet implements Iterable<Pair<String, String>> {
    private static Gson KVDATA_GSON = new GsonBuilder()
        .enableComplexMapKeySerialization()
        .excludeFieldsWithoutExposeAnnotation()
        .create();

    @Expose
    private List<Pair<String, String>> entries;

    public KVDataSet() {
        /* Default constructor */
        entries = new ArrayList<>();
    }

    public KVDataSet(List<Pair<String, String>> entries) {
        this.entries = entries;
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<Pair<String, String>> iterator() {
        return entries.iterator();
    }

    public List<Pair<String, String>> getEntries() {
        return entries;
    }

    public KVDataSet setEntries(List<Pair<String, String>> entries) {
        this.entries = entries;
        return this;
    }

    public void addEntry(Pair<String, String> entry) {
        this.entries.add(entry);
    }

    public void merge(KVDataSet other) {
        entries.addAll(other.getEntries());
    }

    public void sortByKeys(boolean ascending) {
        if (ascending) {
            entries.sort(Comparator.comparing(Pair::getKey));
        } else {
            entries.sort((c1, c2) -> {
                int compare = c1.getKey().compareTo(c2.getKey());
                if (compare > 0) {
                    return -1;
                } else if (compare < 0) {
                    return 1;
                }

                return 0;
            });
        }
    }

    public void sort(Comparator<Pair<String, String>> comparator) {
        entries.sort(comparator);
    }

    public String combineValues() {
        StringBuilder sb = new StringBuilder();
        Pair<String, String> entry;
        int i;
        for (i=0; i<this.entries.size(); i++) {
            entry = this.entries.get(i);
            sb.append(entry.getValue());

            if (i < this.entries.size()-1) {
                sb.append(" ");
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return KVDATA_GSON.toJson(this);
    }

    public int size() {
        return entries.size();
    }

    public String serialize() {
        String str = KVDATA_GSON.toJson(entries);
        return Base64.getEncoder().encodeToString(str.getBytes());
    }

    public KVDataSet deserialize(String b64str) {
        String json = new String(Base64.getDecoder().decode(b64str));
        Type type = new TypeToken<List<Pair<String, String>>>() {}.getType();
        this.entries = KVDATA_GSON.fromJson(json, type);

        return this;
    }

    public String print(String header) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("\n============%s: DATA===========\n", header));
        int keyFormatLen = 0;
        for (Pair<String, String> pair : entries) {
            if (keyFormatLen < pair.getKey().length()) {
                keyFormatLen = pair.getKey().length();
            }
        }
        keyFormatLen+=2;

        HashRing.Hash hash;
        String key;
        String value;
        for (Pair<String, String> pair : entries) {
            key = pair.getKey();
            value = pair.getValue();
            hash = new HashRing.Hash(key);
            sb.append(String.format(
                "\tKey: %" + keyFormatLen + "s -> %s | Data: %s\t\t\n",
                key, hash.toHexString(), value));
        }

        return sb.toString();
    }
}
