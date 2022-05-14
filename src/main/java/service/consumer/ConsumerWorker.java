package service.consumer;

import lombok.Data;
import model.Message;
import model.Topic;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class ConsumerWorker {
    private final int id;
    private final AtomicInteger offset;
    private final Topic topic;

    public ConsumerWorker(final Topic topic) {
        this.id = Math.abs(UUID.randomUUID().hashCode());
        this.offset = new AtomicInteger(0);
        this.topic = topic;
    }

    public void consume() {
        synchronized (this) {
            System.out.println("Worker started by thread: " + Thread.currentThread());
            do {
                try {
                    final int offset = this.offset.get();
                    final int queueSize = topic.getQueueSize().get();
                    while(offset == queueSize) {
                        this.wait();
                    }
                    final Message message = topic.getMessage().get(offset);
                    System.out.println("Message" + message + " printed by thread: " + Thread.currentThread());
                    this.offset.compareAndSet(offset, offset + 1);
                } catch (Exception e) {

                }
            } while (true);
        }
    }

    public synchronized void wakeUp() {
        synchronized (this) {
            this.notify();
            System.out.println("Notified by thread: " + Thread.currentThread());
        }
    }

}
