package com.ashiia.disruptor.example.bizrules;

import java.util.concurrent.BlockingQueue;

/**
 * User: mbharadwaj
 * Date: 7/31/11
 */

public final class QueueHandler implements Runnable {
    private final BizRuleStep bizRuleStep;
    private final BlockingQueue<BizRuleData> validateInputQueue;
    private final BlockingQueue<BizRuleData> saveInputQueue;
    private final BlockingQueue<BizRuleData> validateOutputQueue;
    private final BlockingQueue<BizRuleData> saveOutputQueue;

    private volatile boolean running;

    private volatile long sequence = 0;
    private long bizRuleCounter = 0;

    public QueueHandler(final BizRuleStep bizRuleStep,
                        final BlockingQueue<BizRuleData> validateInputQueue,
                        final BlockingQueue<BizRuleData> saveInputQueue,
                        final BlockingQueue<BizRuleData> validateOutputQueue,
                        final BlockingQueue<BizRuleData> saveOutputQueue) {
        this.bizRuleStep = bizRuleStep;
        this.validateInputQueue = validateInputQueue;
        this.saveInputQueue = saveInputQueue;
        this.validateOutputQueue = validateOutputQueue;
        this.saveOutputQueue = saveOutputQueue;
    }

    public long getBizRuleCounter() {
        return bizRuleCounter;
    }

    public void reset() {
        bizRuleCounter = 0;
        sequence = 0;
    }

    public long getSequence() {
        return sequence;
    }

    public void halt() {
        running = false;
    }

    public void run() {
        running = true;
        while (running) {
            try {
                switch (bizRuleStep) {
                    case VALIDATE: {
                        BizRuleData data = validateInputQueue.take();
                        validateOutputQueue.put(data.validate(DiamondPathExample.MOD_VALUE));
                        break;
                    }

                    case SAVE: {
                        BizRuleData value = saveInputQueue.take();
                        saveOutputQueue.put(value.save());
                        break;
                    }

                    case NOFITY: {
                        final BizRuleData validate = validateOutputQueue.take();
                        final BizRuleData save = saveOutputQueue.take();
                        if (validate.isValid() && save.isSaved()) {
                            //sendNotification
                            bizRuleCounter++;
                        }
                        break;
                    }
                }

                sequence++;
            } catch (InterruptedException ex) {
                break;
            }
        }
    }
}
