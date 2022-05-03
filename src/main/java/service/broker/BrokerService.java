package service.broker;

import com.google.inject.Singleton;
import model.*;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Singleton
public class BrokerService {
    private final static int POOL_SIZE = 100;
    private final Map<Integer, TopicHandler> topicHandlerMap;
    private final ExecutorService[] topicExecutor = new ExecutorService[POOL_SIZE];

    public BrokerService() {
        this.topicHandlerMap = new ConcurrentHashMap<>();
        for(int i = 0; i < POOL_SIZE; i++) {
            topicExecutor[i] = Executors.newSingleThreadExecutor();
        }
    }

    public int createTopic() {
        final int topicId = Math.abs(UUID.randomUUID().hashCode());
        final Topic topic = new Topic(topicId);
        final TopicHandler topicHandler = new TopicHandler(topic);
        topicHandlerMap.put(topicId, topicHandler);
        return topicId;
    }

    public Topic getTopic(final int topicId) {
        return topicHandlerMap.get(topicId).getTopic();
    }

    public void subscribeProducer(final Producer producer, final int topicId) {
       topicHandlerMap.get(topicId).addPublisher(producer);
    }

    public void subscribeConsumer(final Consumer consumer, final int topicId) {
       topicHandlerMap.get(topicId).addConsumer(consumer);
    }

    public void addMessage(final int topicId, final int producerId, final Message message) {
        topicExecutor[topicId % POOL_SIZE].execute(() -> {
            if (topicHandlerMap.get(topicId) != null) {
                topicHandlerMap.get(topicId).addMessage(producerId, message);
            }
        });
    }
}
