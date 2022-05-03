package model;

import java.util.ArrayList;
import java.util.List;


public class Producer {
    private int id;
    private final List<Integer> topics;

    public Producer(int id) {
        this.id = id;
        this.topics = new ArrayList<>();
    }

    public synchronized boolean add(final int topicId) {
        if(!topics.contains(topicId)) {
            topics.add(topicId);
            return true;
        }
        return false;
    }

    public Integer get() {
       return this.id;
    }

    public boolean isPresent(final int topicId) {
        if(topics.contains(topicId)) {
            return true;
        }
        return false;
    }
}
