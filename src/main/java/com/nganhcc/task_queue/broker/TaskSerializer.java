package com.nganhcc.task_queue.broker;

import org.springframework.stereotype.Component;

import com.nganhcc.task_queue.model.Task;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;


@Component
public class TaskSerializer {
    //convert Task to/from JSON for Redis
    private final JsonMapper jsonMapper;

    public TaskSerializer(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public String serialize(Task task) {
        try {
            return jsonMapper.writeValueAsString(task);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize task",e);
        }
    }

    public Task deserialize(String json){
        try {
            return jsonMapper.readValue(json,Task.class);
        }catch(JacksonException e){
            throw new IllegalStateException("Failed to deserialize task", e);
        }
    }

}
