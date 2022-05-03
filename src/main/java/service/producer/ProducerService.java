package service.producer;

import model.Message;
import model.Producer;
import service.broker.BrokerService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Singleton
public class ProducerService {
    private static final int POOL_SIZE = 100;
    private final Map<Integer, Producer> producerMap;
    private final Map<Integer, ExecutorService[]> producerExecutors;
    private final BrokerService brokerService;

    @Inject
    public ProducerService(final BrokerService brokerService) {
        this.producerMap = new ConcurrentHashMap<>();
        this.producerExecutors = new ConcurrentHashMap<>();
        this.brokerService = brokerService;

    }

    public int createProducer() {
        int producerId = Math.abs(UUID.randomUUID().hashCode());
        Producer producer = new Producer(producerId);
        producerMap.put(producerId, producer);
        ExecutorService[] executors = new ExecutorService[POOL_SIZE];
        for(int i = 0; i < POOL_SIZE; i++) {
            executors[i] = Executors.newSingleThreadExecutor();
        }
        producerExecutors.put(producerId, executors);
        return producerId;
    }

    public void subscribeProducer(int producerId, int topicId) {
        if (producerMap.containsKey(producerId)) {
            final Producer producer = this.producerMap.get(producerId);
            brokerService.subscribeProducer(producer, topicId);
            producer.add(topicId);
        }
    }

    public void publishMessage(final int producerId, final int topicId, final Message message) {
        if (producerMap.containsKey(producerId)) {
            final Producer producer = this.producerMap.get(producerId);
            if(producer.isPresent(topicId)) {
                CompletableFuture.runAsync(() -> brokerService.addMessage(topicId, producerId, message), producerExecutors.get(producerId)[topicId % POOL_SIZE]);
            }
        }
    }

    public void publishMessage(int producerId, List<Integer> topicIds, final Message message) {
        if (producerMap.containsKey(producerId)) {
            final Producer producer = this.producerMap.get(producerId);
            for(Integer topicId: topicIds) {
                if(producer.isPresent(topicId)) {
                    CompletableFuture.runAsync(() -> brokerService.addMessage(topicId, producerId, message), producerExecutors.get(producerId)[topicId % POOL_SIZE]);
                }
            }
        }
    }
}
