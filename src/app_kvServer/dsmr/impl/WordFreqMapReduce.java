package app_kvServer.dsmr.impl;

import app_kvServer.dsmr.MapInput;
import app_kvServer.dsmr.MapReduce;
import app_kvServer.dsmr.ReduceInput;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;

public class WordFreqMapReduce extends MapReduce {
    private static final Set<String> EXCLUDE_WORD_LIST = new HashSet<>(Arrays.asList(
        "the", "of", "for", "in", "you", "a"
    ));
    public WordFreqMapReduce(BiConsumer<String, String> Emit) {
        // Define emitter
        super(Emit);
    }

    @Override
    public void Map(MapInput input) {
        final String text = input.getValue();
        final String[] split = text.split(" ");

        String token;
        int i;
        for (i=0; i<split.length; i++) {
            token = split[i];
            if (!EXCLUDE_WORD_LIST.contains(token)) {
                Emit.accept(split[i], "1");
            }
        }
    }

    @Override
    public void Reduce(ReduceInput input) {
        int value = 0;
        Iterator<String> it = input.iterator();
        while (it.hasNext()) {
            try {
                value += Integer.parseInt(it.next());
            } catch (NumberFormatException e) {
                /* Swallow */
            }
        }

        if (value > 50) {
            Emit.accept(input.getKey(), Integer.toString(value));
        }
    }
}
