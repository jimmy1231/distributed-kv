package app_kvServer.dsmr;

import java.util.*;
import java.util.stream.Collectors;

public class ReduceInput implements Iterable<String> {
    private String key;
    private List<String> values;
    private ReduceDTO transport;

    public static class ReduceDTO {
        private static final String DELIMITER = "%__B33F__%";
        List<ReduceInput> inputs;

        public ReduceDTO() {
            inputs = new ArrayList<>();
        }

        public ReduceDTO(String str) {
            inputs = new ArrayList<>();
            String[] split = str.split(DELIMITER);
            for (String inputStr : split) {
                inputs.add(new ReduceInput(inputStr));
            }
        }

        public void addInput(ReduceInput input) {
            inputs.add(input);
        }

        public List<ReduceInput> getInputs() {
            return inputs;
        }

        public ReduceDTO setInputs(List<ReduceInput> inputs) {
            this.inputs = inputs;
            return this;
        }

        @Override
        public String toString() {
            return inputs.stream()
                .map(ReduceInput::toString)
                .collect(Collectors.joining(DELIMITER));
        }
    }

    /**
     * Input: "'word' 1,1,1,1,1,1,1,1,1,1,1"
     * @param str
     */
    public ReduceInput(String str) {
        String[] split = str.split(" ");
        key = split[0];
        values = Arrays.asList(split[1].split(","));
    }

    public ReduceInput(String key, List<String> values) {
        this.key = key;
        this.values = values;
    }

    public ReduceInput(String key, boolean construction) {
        /* construction parameter used to avoid overloading */
        this.key = key;
        this.values = new ArrayList<>();
    }

    public static class ReduceInputIterator implements Iterator<String> {
        Iterator<String> it;

        protected ReduceInputIterator(List<String> values) {
            it = values.iterator();
        }
        /**
         * Returns {@code true} if the iteration has more elements.
         * (In other words, returns {@code true} if {@link #next} would
         * return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         */
        @Override
        public String next() {
            return it.next();
        }
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<String> iterator() {
        return new ReduceInputIterator(values);
    }

    public void addValue(String value) {
        values.add(value);
    }

    @Override
    public String toString() {
        return String.format("%s %s", key, String.join(",", values));
    }

    //////////////////////////////////////////////////////////////
    public String getKey() {
        return key;
    }

    public List<String> getValues() {
        return values;
    }

    public ReduceInput setKey(String key) {
        this.key = key;
        return this;
    }

    public ReduceInput setValues(List<String> values) {
        this.values = values;
        return this;
    }
    //////////////////////////////////////////////////////////////
}
