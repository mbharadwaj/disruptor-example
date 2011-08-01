package com.ashiia.disruptor.example.wordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public final class Reducer implements Runnable {
    private BlockingQueue<HashMap<String, Integer>> reducerQueue;
    private HashMap<String, Integer> accumulator = new HashMap<String, Integer>();

    private volatile boolean running;
    private volatile long sequence;

    public Reducer() {
    }

    public Reducer(final BlockingQueue<HashMap<String, Integer>> reducerQueue) {
        this.reducerQueue = reducerQueue;
    }

    public void halt() {
        running = false;
    }

    public void reset() {
        sequence = 0L;
        accumulator = new HashMap<String, Integer>();
    }

    public long getSequence() {
        return sequence;
    }


    public HashMap<String, Integer> reduce(HashMap<String, Integer> values) {
        //System.out.println("reducing: " + values);
        Set<Map.Entry<String, Integer>> entries = values.entrySet();
        for (Map.Entry<String, Integer> entry : entries) {
            String key = entry.getKey();
            Integer val = entry.getValue();
            if (accumulator.containsKey(key)) {
                accumulator.put(key, val + accumulator.get(key));
            } else {
                accumulator.put(key, val);
            }
        }
        return accumulator;
    }

    public void run() {
        running = true;
        while (running) {
            try {
                //reduce(reducerQueue.take());
                reducerQueue.take();
                sequence++;
            } catch (InterruptedException ex) {
                break;
            }
        }
    }

    public HashMap<String, Integer> getResults() {
        return accumulator;
    }
}
