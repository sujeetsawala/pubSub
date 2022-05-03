import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import model.Message;
import module.PubsubModule;
import service.broker.BrokerService;
import service.consumer.ConsumerService;
import service.producer.ProducerService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        Injector injector = Guice.createInjector(new PubsubModule());
        BrokerService brokerService = injector.getInstance(BrokerService.class);
        ConsumerService consumerService = injector.getInstance(ConsumerService.class);
        ProducerService producerService = injector.getInstance(ProducerService.class);

        List<Integer> producerIds = new ArrayList<>();
        for(int i = 0; i < 1; i++) {
            int producerId = producerService.createProducer();
            producerIds.add(producerId);
        }

        List<Integer> topicIds = new ArrayList<>();
        for(int i = 0; i < 1; i++) {
            int topicId = brokerService.createTopic();
            topicIds.add(topicId);
        }

        List<Integer> consumerIds = new ArrayList<>();
        for(int i = 0; i < 1; i++) {
            int consumerId = consumerService.createConsumer();
            consumerIds.add(consumerId);
        }

        for(Integer producerId: producerIds) {
            for(Integer topicId: topicIds) {
                producerService.subscribeProducer(producerId, topicId);
            }
        }

        for(Integer consumerId: consumerIds) {
            for(Integer topicId: topicIds) {
                consumerService.subscribeConsumer(consumerId, topicId);
            }
        }

        Map<String, String> data1 = ImmutableMap.of("key", "hello1");
        Map<String, String> data2 = ImmutableMap.of("key", "hello2");
        Map<String, String> data3 = ImmutableMap.of("key", "hello3");
        Map<String, String> data4 = ImmutableMap.of("key", "hello4");

        Message message1 = Message.builder().data(data1).build();
        Message message2 = Message.builder().data(data2).build();
        Message message3 = Message.builder().data(data3).build();



        producerService.publishMessage(producerIds.get(0), topicIds, message1);
        producerService.publishMessage(producerIds.get(0), topicIds, message2);
        producerService.publishMessage(producerIds.get(0), topicIds, message3);

        Thread.sleep(5000);

        consumerService.resetOffset(consumerIds.get(0), topicIds.get(0), 0);




    }
}
