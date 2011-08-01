package com.ashiia.disruptor.example.bizrules;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

/**
 * User: mbharadwaj
 * Date: 7/31/11
 */
public class QueueStrategy {
    private final BlockingQueue<BizRuleData> validateInputQueue = new ArrayBlockingQueue<BizRuleData>(DiamondPathExample.SIZE);
    private final BlockingQueue<BizRuleData> saveInputQueue = new ArrayBlockingQueue<BizRuleData>(DiamondPathExample.SIZE);
    private final BlockingQueue<BizRuleData> validateOutputQueue = new ArrayBlockingQueue<BizRuleData>(DiamondPathExample.SIZE);
    private final BlockingQueue<BizRuleData> saveOutputQueue = new ArrayBlockingQueue<BizRuleData>(DiamondPathExample.SIZE);

    private final QueueHandler validateQueueConsumer =
            new QueueHandler(BizRuleStep.VALIDATE, validateInputQueue, saveInputQueue, validateOutputQueue, saveOutputQueue);

    private final QueueHandler saveQueueConsumer =
            new QueueHandler(BizRuleStep.SAVE, validateInputQueue, saveInputQueue, validateOutputQueue, saveOutputQueue);

    private final QueueHandler notifyQueueConsumer =
            new QueueHandler(BizRuleStep.NOFITY, validateInputQueue, saveInputQueue, validateOutputQueue, saveOutputQueue);

    private int run;
    private long iter;

    public QueueStrategy(int run, long iter) {
        this.run = run;
        this.iter = iter;
    }

    public long execute() throws InterruptedException {
        notifyQueueConsumer.reset();

        Future[] futures = new Future[DiamondPathExample.NUM_CONSUMERS];
        futures[0] = DiamondPathExample.EXECUTOR.submit(validateQueueConsumer);
        futures[1] = DiamondPathExample.EXECUTOR.submit(saveQueueConsumer);
        futures[2] = DiamondPathExample.EXECUTOR.submit(notifyQueueConsumer);

        long start = System.currentTimeMillis();

        for (long i = 0; i < iter; i++) {
            BizRuleData bizRuleData = BizRuleData.ENTRY_FACTORY.create();
            bizRuleData.setValue(i);
            validateInputQueue.put(bizRuleData);
            saveInputQueue.put(bizRuleData);
        }

        final long expectedSequence = iter - 1;
        while (notifyQueueConsumer.getSequence() < expectedSequence) {
            // busy spin
        }

        long opsPerSecond = (iter * 1000L) / (System.currentTimeMillis() - start);

        validateQueueConsumer.halt();
        saveQueueConsumer.halt();
        notifyQueueConsumer.halt();

        //System.out.format("queue:: bizRuleCounter: %d, sequence: %d\n", notifyQueueConsumer.getBizRuleCounter(), notifyQueueConsumer.getSequence());
        for (Future future : futures) {
            future.cancel(true);
        }

        return opsPerSecond;

    }
}
