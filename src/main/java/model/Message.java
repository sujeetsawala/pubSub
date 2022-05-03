package model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class Message {
    private Map<String, String> data;
}
