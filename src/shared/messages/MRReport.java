package shared.messages;

import com.google.gson.annotations.Expose;

public class MRReport {
    @Expose
    private int numMappers = 0;
    @Expose
    private int numReducers = 0;
    @Expose
    private long timeMap = 0L;
    @Expose
    private long timeReduce = 0L;
    @Expose
    private int numAvailNodes = 0;
    @Expose
    private long inputSize = 0L;
    @Expose
    private long outputSize = 0L;

    public int getNumMappers() {
        return numMappers;
    }

    public MRReport setNumMappers(int numMappers) {
        this.numMappers = numMappers;
        return this;
    }

    public int getNumReducers() {
        return numReducers;
    }

    public MRReport setNumReducers(int numReducers) {
        this.numReducers = numReducers;
        return this;
    }

    public long getTimeMap() {
        return timeMap;
    }

    public MRReport setTimeMap(long timeMap) {
        this.timeMap = timeMap;
        return this;
    }

    public long getTimeReduce() {
        return timeReduce;
    }

    public MRReport setTimeReduce(long timeReduce) {
        this.timeReduce = timeReduce;
        return this;
    }

    public int getNumAvailNodes() {
        return numAvailNodes;
    }

    public MRReport setNumAvailNodes(int numAvailNodes) {
        this.numAvailNodes = numAvailNodes;
        return this;
    }

    public long getInputSize() {
        return inputSize;
    }

    public MRReport setInputSize(long inputSize) {
        this.inputSize = inputSize;
        return this;
    }

    public long getOutputSize() {
        return outputSize;
    }

    public MRReport setOutputSize(long outputSize) {
        this.outputSize = outputSize;
        return this;
    }
}
