package server.dsmr.impl;

import server.dsmr.MapInput;
import server.dsmr.MapReduce;
import server.dsmr.ReduceInput;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

public class WordFreq extends MapReduce {
    private static final Set<String> EXCLUDE_WORD_LIST = new HashSet<>(Arrays.asList(
        "the", "of", "for", "in", "you", "a", "with",
        "on", "was", "is", "to", "as", "at", "his", "her",
        "him", "he", "hers", "their", "they", "she", "and",
        "were", "where", "from", "by", "be", "this", "that",
        "an"
    ));
    private static final Pattern P = Pattern.compile("[\\x00-\\x7F]");

    public WordFreq(BiConsumer<String, String> Emit) {
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
            token = split[i].toLowerCase();
            if (!EXCLUDE_WORD_LIST.contains(token)
                    && Pattern.matches("^[a-zA-Z]*$", token)) {
                Emit.accept(token, "1");
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

        if (value > 3) {
            Emit.accept(input.getKey(), Integer.toString(value));
        }
    }
}
