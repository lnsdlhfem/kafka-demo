package com.lnsdlhfem.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;
import java.util.stream.Stream;


/**
 * 计数程序示例
 * 基于Kafka Stream的流式应用的业务逻辑全部通过一个被称为Processor Topology的地方执行。
 * 它与Storm的Topology和Spark的DAG类似，都定义了数据在各个处理单元（在Kafka Stream中被称作Processor）间的流动方式，或者说定义了数据的处理逻辑。
 *
 * @ClassName: WordCount
 * @author: lnsdlhfem
 * @date: 2017/11/26 0:04
 */
public class WordCount implements Processor<String, String>{

    private ProcessorContext context;

    private KeyValueStore<String, Integer> kvStore;

    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        // context.scheduler定义了punctuate被执行的周期，从而提供了实现窗口操作的能力
        this.context.schedule(1000);
        // context.getStateStore提供的状态存储为有状态计算（如窗口，聚合）提供了可能
        this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
    }

    public void process(String key, String value) {
        Stream.of(value.toLowerCase().split(" ")).forEach((String word) -> {
            Optional<Integer> counts = Optional.ofNullable(kvStore.get(word));
            int count = counts.map(wordcount -> wordcount + 1).orElse(1);
            kvStore.put(word, count);
        });
    }

    public void punctuate(long l) {
        KeyValueIterator<String, Integer> iterator = this.kvStore.all();
        iterator.forEachRemaining(entry -> {
            context.forward(entry.key, entry.value);
            this.kvStore.delete(entry.key);
        });
        context.commit();
    }

    public void close() {
        this.kvStore.close();
    }
}
