package server.dsmr.impl;

import server.dsmr.MapInput;
import server.dsmr.MapReduce;
import server.dsmr.ReduceInput;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;

public class Sort extends MapReduce {
    public static final String DELIMITER = "%_SORT_%";
    private static final List<Integer> BIN_RANGES = Arrays.asList(
        0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
        110, 120, 130, 140, 150, 160, 170, 180,
        190, 200, 210, 220, 230, 240, 255
    );

    public Sort(BiConsumer<String, String> Emit) {
        // Define emitter
        super(Emit);
    }

    @Override
    public void Map(MapInput input) {
        final String text = input.getValue();
        final String[] split = text.split(" ");

        byte[] tokenBytes;
        Integer byteVal;
        String token;
        int i;
        for (i=0; i<split.length; i++) {
            token = split[i];
            tokenBytes = token.getBytes(StandardCharsets.US_ASCII);
            if (tokenBytes.length > 0) {
                byteVal = ((int) tokenBytes[0]) & 0xFF;

                /*
                 * Choose appropriate bin range for each byteVal:
                 * lower <= byteVal < upper
                 */
                Integer binId;
                int lower, upper;
                for (binId=0; binId<BIN_RANGES.size()-1; binId++) {
                    lower = BIN_RANGES.get(binId);
                    upper = BIN_RANGES.get(binId+1);

                    if (byteVal >= lower && byteVal < upper) {
                        Emit.accept(binId.toString(), token);
                    }
                }
            }
        }
    }

    @Override
    public void Reduce(ReduceInput input) {
        input.getValues().sort(Comparator.naturalOrder());
        Emit.accept(input.getKey(), String.join(DELIMITER, input.getValues()));
    }
}
