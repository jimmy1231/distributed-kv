package app_kvServer.dsmr.impl;

import app_kvServer.dsmr.MapInput;
import app_kvServer.dsmr.MapReduce;
import app_kvServer.dsmr.ReduceInput;

import java.util.function.BiConsumer;

public class WordFreqMapReduce extends MapReduce {
    public WordFreqMapReduce(BiConsumer<String, String> Emit) {
        // Define emitter
        super(Emit);
    }

    @Override
    public void Map(MapInput input) {
        final String text = input.getValue();
        final String[] split = text.split(" ");

        int i;
        for (i=0; i<split.length; i++) {
            Emit.accept(split[i], "1");
        }
    }

    @Override
    public void Reduce(ReduceInput input) {

    }
}
