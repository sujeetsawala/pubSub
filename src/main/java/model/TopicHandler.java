package model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TopicHandler {
    private final int POOl_SIZE = 100;
    private final Topic topic;
    private final List<Producer> publishers;
    private final List<Consumer> consumers;
    private final ExecutorService[] threadPool;

    public TopicHandler(final Topic topic) {
        this.topic = topic;
        this.consumers = new ArrayList<>();
        this.publishers = new ArrayList<>();
        this.threadPool = new ExecutorService[POOl_SIZE];
        for(int i = 0; i < POOl_SIZE; i++) {
            threadPool[i] = Executors.newSingleThreadExecutor();
        }
    }

    public synchronized void addPublisher(final Producer producer) {
        publishers.add(producer);
    }

    public synchronized void addConsumer(final Consumer consumer) {
        consumers.add(consumer);
    }

    public void addMessage(final int producerId, final Message message) {
        if(this.publishers.stream().filter(e -> e.get().equals(producerId)).count() > 0) {
            this.topic.addMessage(message);
            for (Consumer consumer : consumers) {
                CompletableFuture.runAsync(() -> consumer.wakeUp(topic.get()), threadPool[consumer.get() % POOl_SIZE]);
            }
        }
    }

    public Topic getTopic() {
        return this.topic;
    }
}
