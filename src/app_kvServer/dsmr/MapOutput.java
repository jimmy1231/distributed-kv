package app_kvServer.dsmr;

import shared.Pair;
import shared.messages.KVDataSet;

import java.util.List;
import java.util.Objects;

public class MapOutput {
    private static final String SPLIT_DELIMITER = " ";
    private static final String KV_DELIMITER = ",";
    private KVDataSet dataSet;

    public MapOutput(KVDataSet dataSet) {
        this.dataSet = dataSet;
    }

    public MapOutput(String str) {
        dataSet = new KVDataSet();

        String[] list = str.split(SPLIT_DELIMITER);
        Pair<String, String> entry;
        for (String kv : list) {
            String[] kvSplit = kv.split(KV_DELIMITER);
            entry = new Pair<>(kvSplit[0], kvSplit[1]);
            dataSet.addEntry(entry);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String strEntry;
        List<Pair<String,String>> entries = dataSet.getEntries();
        for (Pair<String,String> entry : entries) {
            strEntry = entry.getKey()+KV_DELIMITER+entry.getValue();
            sb.append(strEntry).append(SPLIT_DELIMITER);
        }

        return sb.toString().trim();
    }
}
