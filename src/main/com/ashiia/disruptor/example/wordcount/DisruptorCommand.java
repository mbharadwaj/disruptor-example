package com.ashiia.disruptor.example.wordcount;

import com.lmax.disruptor.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DisruptorCommand {
    private final RingBuffer<MapReduceEntry> ringBuffer = new RingBuffer<MapReduceEntry>(MapReduceEntry.ENTRY_FACTORY, WordCount.SIZE,
            ClaimStrategy.Option.MULTI_THREADED,
            WaitStrategy.Option.YIELDING);

    ConsumerBarrier<MapReduceEntry> consumerBarrierMap;
    MapReduceHandler[] mapReduceHandlers;
    BatchConsumer[] batchConsumers;
    BatchConsumer<MapReduceEntry> batchConsumerReduce;
    ProducerBarrier<MapReduceEntry> producerBarrier;
    MapReduceHandler reduceHandler;
    ExecutorService executorService;
    private int numberOfMappers;
    private int numberOfConsumers;


    public DisruptorCommand(int numberOfMappers) {
        this.numberOfMappers = numberOfMappers;
        this.numberOfConsumers = numberOfMappers + 1;
        executorService = Executors.newFixedThreadPool(numberOfConsumers);

        consumerBarrierMap = ringBuffer.createConsumerBarrier();

        mapReduceHandlers = new MapReduceHandler[numberOfMappers];
        batchConsumers = new BatchConsumer[numberOfMappers];
        for (int i = 0; i < numberOfMappers; i++) {
            mapReduceHandlers[i] = new MapReduceHandler(MapReduceHandler.Step.MAP, i, numberOfMappers);
            batchConsumers[i] = new BatchConsumer<MapReduceEntry>(consumerBarrierMap, mapReduceHandlers[i]);
        }

        ConsumerBarrier<MapReduceEntry> consumerBarrierReduce;
        consumerBarrierReduce = ringBuffer.createConsumerBarrier(batchConsumers);
        reduceHandler = new MapReduceHandler(MapReduceHandler.Step.REDUCE);
        batchConsumerReduce = new BatchConsumer<MapReduceEntry>(consumerBarrierReduce, reduceHandler);

        producerBarrier = ringBuffer.createProducerBarrier(batchConsumerReduce);

    }

    public long execute(String[] fileContents) {

        for (BatchConsumer batchConsumer : batchConsumers) {
            executorService.submit(batchConsumer);
        }
        executorService.submit(batchConsumerReduce);

        System.out.println("disruptor...");
        long start = System.currentTimeMillis();

        long numberOfMessages = 0;
        for (String fileContent : fileContents) {
            String[] tokens = fileContent.trim().split("\n");
            numberOfMessages += tokens.length;
            for (String token : tokens) {
                MapReduceEntry entry = producerBarrier.nextEntry();
                entry.setMapValue(token);
                producerBarrier.commit(entry);
            }
        }

        final long expectedSequence = ringBuffer.getCursor();
        while (batchConsumerReduce.getSequence() < expectedSequence) {
            // busy spin
        }

        long timeTaken = System.currentTimeMillis() - start;

        for (BatchConsumer batchConsumer : batchConsumers) {
            batchConsumer.halt();
        }
        batchConsumerReduce.halt();

        System.out.format("num_messages: %d, time_taken = %d, ops/sec = %d\n", numberOfMessages, timeTaken, numberOfMessages / timeTaken);
        //System.out.println(reduceHandler.getResults());

        return timeTaken;
    }

    public void reset() {
        reduceHandler.reset();
    }

    public void halt() {
        executorService.shutdown();
    }
}
