package server.dsmr;

import java.util.function.BiConsumer;

public abstract class MapReduce {
    public enum Type {
        K_MEANS_CLUSTERING,
        SORT,
        WORD_FREQ
    }

    protected BiConsumer<String, String> Emit;

    protected MapReduce(BiConsumer<String, String> Emit) {
        this.Emit = Emit;
    }

    public abstract void Map(MapInput input);
    public abstract void Reduce(ReduceInput input);
}
