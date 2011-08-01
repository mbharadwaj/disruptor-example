package com.ashiia.disruptor.example.wordcount;

import com.lmax.disruptor.BatchHandler;

import java.util.HashMap;

public class MapReduceHandler implements BatchHandler<MapReduceEntry> {
    private Step step;
    private int ordinal;
    private int numberOfMapConsumers;
    private Reducer reducer = new Reducer();
    private Mapper mapper = new Mapper();

    public enum Step {MAP, REDUCE}

    public MapReduceHandler(final Step step, final int ordinal, final int numberOfMapConsumers) {
        this.step = step;
        this.ordinal = ordinal;
        this.numberOfMapConsumers = numberOfMapConsumers;
    }

    public MapReduceHandler(final Step step) {
        this.step = step;
    }

    public void onAvailable(final MapReduceEntry entry) throws Exception {
        switch (step) {
            case MAP:
                if (entry.getSequence() % numberOfMapConsumers == ordinal) {
                    entry.setReduceValue(mapper.map(entry.getMapValue()));
                }
                break;
            case REDUCE:
                //reducer.reduce(entry.getReduceValue());
                entry.getReduceValue();
                break;

        }
    }

    public void onEndOfBatch() throws Exception {
    }

    public HashMap<String, Integer> getResults() {
        return reducer.getResults();
    }

    public void reset() {
        reducer.reset();
    }

}
