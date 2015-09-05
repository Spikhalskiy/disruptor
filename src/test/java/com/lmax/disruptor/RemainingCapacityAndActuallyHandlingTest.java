package com.lmax.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Dmitry Spikhalskiy <dspikhalskiy@pulsepoint.com>
 */
public class RemainingCapacityAndActuallyHandlingTest {
    @SuppressWarnings("unchecked")
    @Test
    public void remainingAfterOneEventProceed() throws Exception
    {
        Executor executor = Executors.newFixedThreadPool(2);
        Disruptor<AtomicLong> disruptor = new Disruptor<AtomicLong>(new AtomicLongEventFactory(), 8, executor, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleExceptionsWith(new FatalExceptionHandler());
        disruptor
                .handleEventsWith(new AtomicLongWorkHandler(0), new AtomicLongWorkHandler(1)).then(new AtomicLongWorkHandler(2));
        disruptor.start();

        RingBuffer<AtomicLong> ringBuffer = disruptor.getRingBuffer();
        long initialRemainCapacity = ringBuffer.remainingCapacity();

        disruptor.publishEvent(new EventTranslator<AtomicLong>() {
            @Override
            public void translateTo(AtomicLong event, long sequence) {
                event.set(0);
            }
        });

        Thread.sleep(1000);

        assertThat(ringBuffer.remainingCapacity(), is(initialRemainCapacity));
    }

    private static class AtomicLongWorkHandler implements EventHandler<AtomicLong>
    {
        private int number;

        private AtomicLongWorkHandler(int number) {
            this.number = number;
        }

        @Override
        public void onEvent(AtomicLong event, long sequence, boolean endOfBatch) throws Exception {
            System.out.println(number);
        }
    }


    private static class AtomicLongEventFactory implements EventFactory<AtomicLong>
    {
        @Override
        public AtomicLong newInstance()
        {
            return new AtomicLong(0);
        }
    }
}
