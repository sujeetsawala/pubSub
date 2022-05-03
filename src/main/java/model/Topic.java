package model;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Topic {
    private final int topicId;
    private final List<Message> queue;
    private final AtomicInteger queueSize;


    public Topic(int topicId) {
        this.topicId = topicId;
        this.queue = new ArrayList<>();
        this.queueSize = new AtomicInteger(0);

    }

    public Integer get() {
        return this.topicId;
    }


    public synchronized boolean addMessage(final Message message) {
        try {
            int currSize = queue.size();
            queue.add(message);
            queueSize.compareAndSet(currSize, currSize + 1);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public ImmutableList<Message> getMessage() {
       return ImmutableList.copyOf(this.queue);
    }

    public AtomicInteger getQueueSize() {
        return this.queueSize;
    }
}
