package model;

import service.consumer.ConsumerWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer {
    private final int id;
    private final int POOL_SIZE = 100;
    private final Map<Integer, ConsumerWorker> topicExecutors;
    private final ExecutorService[] executorServices = new ExecutorService[POOL_SIZE];
    private final List<Integer> topics;

    public Consumer(int id) {
        this.id = id;
        this.topics = new ArrayList<>();
        this.topicExecutors = new ConcurrentHashMap<>();
        for(int i = 0 ; i < POOL_SIZE; i++) {
            executorServices[i] = Executors.newSingleThreadExecutor();
        }
    }

    public Integer get() {
        return this.id;
    }

    public ConsumerWorker getTopicExecutor(final int topicId) {
        return this.topicExecutors.get(topicId);
    }


    public boolean add(final Topic topic) {
        if(!topics.contains(topic.get()))  {
            topics.add(topic.get());
            final ConsumerWorker consumerWorker = new ConsumerWorker(topic);
            topicExecutors.put(topic.get(), consumerWorker);
            CompletableFuture.runAsync(() ->consumerWorker.consume(), executorServices[topic.get() % POOL_SIZE]);
            return true;
        }
        return false;
    }

    public synchronized void resetOffset(final int topicId, final int modifiedOffset) {
        if(topics.contains(topicId)) {
            final ConsumerWorker consumerWorker = topicExecutors.get(topicId);
            if (consumerWorker != null) {
                int expected = consumerWorker.getOffset().get();
                boolean value = consumerWorker.getOffset().compareAndSet(expected, modifiedOffset);
                System.out.println("Modified offset status: " + value + "by thread " + Thread.currentThread());
            }
        }
    }

    public void wakeUp(final int topicId) {
        if(topics.contains(topicId))  {
            final ConsumerWorker consumerWorker = topicExecutors.get(topicId);
            consumerWorker.wakeUp();
        }
    }
}
