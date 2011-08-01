package com.ashiia.disruptor.example.bizrules;

import com.lmax.disruptor.*;

/**
 * User: mbharadwaj
 * Date: 7/25/11
 */
public class DisruptorStrategy {

    private final RingBuffer<BizRuleData> ringBuffer =
            new RingBuffer<BizRuleData>(BizRuleData.ENTRY_FACTORY, DiamondPathExample.SIZE,
                    ClaimStrategy.Option.SINGLE_THREADED,
                    WaitStrategy.Option.YIELDING);

    private final ConsumerBarrier<BizRuleData> consumerBarrier = ringBuffer.createConsumerBarrier();

    private final DisruptorHandler validateHandler = new DisruptorHandler(BizRuleStep.VALIDATE);
    private final BatchConsumer<BizRuleData> batchConsumerValidate =
            new BatchConsumer<BizRuleData>(consumerBarrier, validateHandler);

    private final DisruptorHandler saveHandler = new DisruptorHandler(BizRuleStep.SAVE);
    private final BatchConsumer<BizRuleData> batchConsumerSave =
            new BatchConsumer<BizRuleData>(consumerBarrier, saveHandler);

    private final ConsumerBarrier<BizRuleData> consumerBarrierValidateSave =
            ringBuffer.createConsumerBarrier(batchConsumerValidate, batchConsumerSave);

    private final DisruptorHandler notifyHandler = new DisruptorHandler(BizRuleStep.NOFITY);
    private final BatchConsumer<BizRuleData> batchConsumerNotify =
            new BatchConsumer<BizRuleData>(consumerBarrierValidateSave, notifyHandler);

    private final ProducerBarrier<BizRuleData> producerBarrier = ringBuffer.createProducerBarrier(batchConsumerNotify);

    private int run;
    private long iter;

    public DisruptorStrategy(int run, long iter) {
        this.run = run;
        this.iter = iter;
    }

    public long execute() {
        notifyHandler.reset();

        DiamondPathExample.EXECUTOR.submit(batchConsumerValidate);
        DiamondPathExample.EXECUTOR.submit(batchConsumerSave);
        DiamondPathExample.EXECUTOR.submit(batchConsumerNotify);

        long start = System.currentTimeMillis();

        for (long i = 0; i < iter; i++) {
            BizRuleData bizRuleData = producerBarrier.nextEntry();
            bizRuleData.setValue(i);
            producerBarrier.commit(bizRuleData);
        }

        final long expectedSequence = ringBuffer.getCursor();
        while (batchConsumerNotify.getSequence() < expectedSequence) {
            // busy spin
        }

        long opsPerSecond = (iter * 1000L) / (System.currentTimeMillis() - start);

        batchConsumerValidate.halt();
        batchConsumerSave.halt();
        batchConsumerNotify.halt();
        //System.out.format("disruptor:: bizRuleCounter: %d, sequence: %d\n", notifyHandler.getBizRuleCounter(), notifyHandler.getSequence());

        return opsPerSecond;
    }
}
