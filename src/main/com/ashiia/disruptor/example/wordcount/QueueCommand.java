package com.ashiia.disruptor.example.wordcount;

import java.util.HashMap;
import java.util.concurrent.*;

public class QueueCommand {
    BlockingQueue[] mapperQueues;
    BlockingQueue<HashMap<String, Integer>> reducerQueue;
    ExecutorService executorService;
    Mapper[] mappers;
    Reducer reducer;
    Future[] futures;
    private int numberOfMappers;
    private int numberOfConsumers;

    //String doc = WordCount.buildString();

    public QueueCommand(int numberOfMappers) {
        this.numberOfMappers = numberOfMappers;
        this.numberOfConsumers = numberOfMappers + 1;
        executorService = Executors.newFixedThreadPool(numberOfConsumers);

        mapperQueues = new ArrayBlockingQueue[numberOfMappers];
        reducerQueue = new ArrayBlockingQueue<HashMap<String, Integer>>(WordCount.SIZE);
        reducer = new Reducer(reducerQueue);

        mappers = new Mapper[numberOfMappers];
        for (int i = 0; i < numberOfMappers; i++) {
            mapperQueues[i] = new ArrayBlockingQueue<String>(WordCount.SIZE);
            mappers[i] = new Mapper(mapperQueues[i], reducerQueue);
        }

    }

    public long execute(String[] fileContents) throws InterruptedException {
        System.out.println("queues...");

        futures = new Future[numberOfConsumers];
        for (int i = 0; i < numberOfMappers; i++) {
            futures[i] = executorService.submit(mappers[i]);
        }
        futures[numberOfMappers] = executorService.submit(reducer);

        long start = System.currentTimeMillis();

        long numberOfMessages = 0;
        for (int i = 0; i < fileContents.length; i++) {
            String[] tokens = fileContents[i].trim().split("\n");
            numberOfMessages += tokens.length;
            //System.out.format("number_messages: %d, seq = %d\n", numberOfMessages, reducer.getSequence());
            for (String token : tokens) {
                mapperQueues[(i % numberOfMappers)].put(token);
            }
        }

        final long expectedSequence = numberOfMessages;
        while (reducer.getSequence() < expectedSequence) {
            //System.out.format("seq = %d\n", reducer.getSequence());
            // busy spin
        }

        long timeTaken = System.currentTimeMillis() - start;

        for (Mapper mapper : mappers) {
            mapper.halt();
        }
        reducer.halt();

        for (Future future : futures) {
            future.cancel(true);
        }

        System.out.format("num_messages: %d, time_taken = %d, ops/sec = %d\n", numberOfMessages, timeTaken, numberOfMessages / timeTaken);
        //System.out.println(reducer.getResults());
        return timeTaken;
    }

    public void reset() {
        reducer.reset();
    }

    public void halt() {
        executorService.shutdown();
    }
}
