package service.consumer;

import model.Consumer;
import model.Topic;
import service.broker.BrokerService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
public class ConsumerService {
    private final Map<Integer, Consumer> consumerMap;
    private final BrokerService brokerService;

    @Inject
    public ConsumerService(final BrokerService brokerService) {
        this.consumerMap = new ConcurrentHashMap<>();
        this.brokerService = brokerService;

    }

    public int createConsumer() {
        int consumerId = Math.abs(UUID.randomUUID().hashCode());
        final Consumer consumer = new Consumer(consumerId);
        consumerMap.put(consumerId, consumer);
        return consumerId;
    }

    public void subscribeConsumer(int consumerId, int topicId) {
        if (consumerMap.containsKey(consumerId)) {
            final Consumer consumer = this.consumerMap.get(consumerId);
            brokerService.subscribeConsumer(consumer, topicId);
            final Topic topic = brokerService.getTopic(topicId);
            consumer.add(topic);
        }
    }

    public synchronized void resetOffset(final int consumerId, final int topicId, final int modifiedOffset) {
        if (consumerMap.containsKey(consumerId)) {
            final Consumer consumer = this.consumerMap.get(consumerId);
            consumer.resetOffset(topicId, modifiedOffset);
            this.wakeUp(consumerId, topicId);
        }
    }

    public void wakeUp(final int consumerId, final int topicId) {
        if (consumerMap.containsKey(consumerId)) {
            final Consumer consumer = this.consumerMap.get(consumerId);
            final ConsumerWorker consumerWorker = consumer.getTopicExecutor(topicId);
            if(consumerWorker != null) {
                consumer.wakeUp(topicId);
            }
        }
    }
}
