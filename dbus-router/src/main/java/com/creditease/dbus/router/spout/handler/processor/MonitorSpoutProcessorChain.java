package com.creditease.dbus.router.spout.handler.processor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import com.creditease.dbus.router.spout.aware.ContextAware;
import com.creditease.dbus.router.spout.aware.KafkaConsumerAware;
import com.creditease.dbus.router.spout.context.ProcessorContext;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MonitorSpoutProcessorChain implements Processor, KafkaConsumerAware {

    private ProcessorContext context = null;
    private KafkaConsumer kafkaConsumer = null;
    private List<AbstractProcessor> processors = null;

    public MonitorSpoutProcessorChain(ProcessorContext context, List<AbstractProcessor> processors) {
        this.context = context;
        this.processors = processors;

    }

    private void propagationKafkaConsumerAware(AbstractProcessor processor) {
        if (KafkaConsumerAware.class.isAssignableFrom(processor.getClass()))
            ((KafkaConsumerAware) processor).setKafkaConsumer(kafkaConsumer);
    }

    private void propagationFromContextAware(AbstractProcessor processor) {
        if (ContextAware.class.isAssignableFrom(processor.getClass()))
            ((ContextAware) processor).setContext(context);
    }

    @Override
    public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        Optional.ofNullable(processors).ifPresent(processors -> processors.stream().forEach(processor -> propagationKafkaConsumerAware(processor)));
    }

    @Override
    public Object process(Object obj, Supplier... suppliers) {
        Objects.requireNonNull(processors, "processors is not null");
        boolean isBreak = false;
        for (AbstractProcessor processor : processors) {
            propagationFromContextAware(processor);
            processor.process(obj, suppliers);
            if(isBreak = processor.isBreak()) break;
        }
        return isBreak;
    }

}
