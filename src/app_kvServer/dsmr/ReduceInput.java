package app_kvServer.dsmr;

import java.util.*;
import java.util.function.Consumer;

public class ReduceInput implements Iterable<String> {
    private String key;
    private List<String> values;

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
