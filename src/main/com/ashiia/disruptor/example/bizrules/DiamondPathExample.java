package com.ashiia.disruptor.example.bizrules;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: mbharadwaj
 * Date: 7/25/11
 */
public class DiamondPathExample {

    public static final int NUM_CONSUMERS = 3;
    public static final int SIZE = 1024 * 32;
    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(NUM_CONSUMERS);
    private static final long ITERATIONS = 1000 * 1000 * 100;
    public static final long MOD_VALUE = 100;

    public static void main(String[] args) throws InterruptedException {
        DiamondPathExample diamondPath = new DiamondPathExample();
        for (long iter = 1000; iter <= ITERATIONS; iter*=10) {
            diamondPath.runs(iter);
        }
        EXECUTOR.shutdown();
    }

    private void runs(long iter) throws InterruptedException {
        final int RUNS = 3;
        long disruptorOps;
        long queueOps;

        for (int run = 0; run < RUNS; run++) {
            System.gc();

            disruptorOps = runDisruptorPass(run, iter);
            queueOps = runQueuePass(run, iter);

            printResults(getClass().getSimpleName(), disruptorOps, queueOps, run, iter);
        }

    }

    private long runQueuePass(int run, long iter) throws InterruptedException {
        QueueStrategy queueStrategy = new QueueStrategy(run, iter);
        return queueStrategy.execute();
    }

    private long runDisruptorPass(int run, long iter) {
        DisruptorStrategy disruptor = new DisruptorStrategy(run, iter);
        return disruptor.execute();
    }

    public static void printResults(final String className, final long disruptorOps, final long queueOps, final int run, final long iter) {
        System.out.format("%s OpsPerSecond iter %d, run %d: BlockingQueues=%d, Disruptor=%d\n", className, iter, run, queueOps, disruptorOps);
    }

}
