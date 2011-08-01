package com.ashiia.disruptor.example.wordcount;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public final class Mapper implements Runnable {
    private BlockingQueue<String> mapperQueue;
    private BlockingQueue<HashMap<String, Integer>> reducerQueue;

    private volatile boolean running;

    public Mapper() {
    }

    public Mapper(final BlockingQueue<String> mapperQueue,
                  final BlockingQueue<HashMap<String, Integer>> reducerQueue) {
        this.mapperQueue = mapperQueue;
        this.reducerQueue = reducerQueue;
    }

    public void halt() {
        running = false;
    }

    private void accumulate(String s, HashMap<String, Integer> acc) {
        String key = s.toLowerCase();
        if (acc.containsKey(key)) {
            Integer i = acc.get(key);
            acc.put(key, i + 1);
        } else {
            acc.put(key, 1);
        }
    }

    public HashMap<String, Integer> map(String data) {
        //System.out.println("mapping: " + data);
        String[] tokens = data.trim().split("\\s+");
        HashMap<String, Integer> results = new HashMap<String, Integer>();
        for (String token : tokens) {
            accumulate(token, results);
        }
        return results;
    }

    public void run() {
        running = true;
        while (running) {
            try {
                HashMap<String, Integer> results = map(mapperQueue.take());
                reducerQueue.put(results);
            } catch (InterruptedException ex) {
                break;
            }
        }
    }
}
