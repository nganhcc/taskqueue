package com.nganhcc.task_queue.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "taskqueue")
public class QueueProperties {
    private Map<String, QueueConfig> queues = new HashMap<>();
    private SchedulerConfig scheduler = new SchedulerConfig();
    private ReaperConfig reaper = new ReaperConfig();
    private WorkerConfig worker = new WorkerConfig();

    public Map<String,QueueConfig> getQueues(){
        return this.queues;
    }
    public void setQueues(Map<String, QueueConfig> queues){
        this.queues=queues;
    }

    public WorkerConfig getWorker(){
        return this.worker;
    }
    public SchedulerConfig getScheduler(){
        return this.scheduler;
    }
    public void setScheduler(SchedulerConfig scheduler){
        this.scheduler = scheduler;
    }
    public ReaperConfig getReaper(){
        return this.reaper;
    }
    public void setReaper(ReaperConfig reaper){
        this.reaper= reaper;
    }

    //Dinh nghia QueueConfig 
    public static class QueueConfig{
        private int concurrency = 4; // so luong worker cho 1 queue
        private int maxRetries = 3; //
        private long baseDelayMs = 1000;

        public int getConcurrency(){
            return this.concurrency;
        }
        public void setConcurrency(int concurrency){
            this.concurrency= concurrency;
        }
        public int getMaxRetries(){
            return this.maxRetries;
        }
        public void setMaxRetries(int maxRetries){
            this.maxRetries= maxRetries;
        }
        public long getBaseDelayMs(){
            return this.baseDelayMs;
        }
        public void setBaseDelayMs(long baseDelayMs){
            this.baseDelayMs= baseDelayMs;
        }
    }

    //WorkerConfig
    public static class WorkerConfig{
        private long handlerTimeoutMs= 30000;
        public long getHandlerTimeoutMs(){
            return this.handlerTimeoutMs;
        }
        public void setHandlerTimeoutMs(long handlerTimeoutMs){
            this.handlerTimeoutMs=handlerTimeoutMs;
        }
    }
    //Dinh nghia SchedulerConfig
    public static class SchedulerConfig{
        private long pollIntervalMs = 1000;
        private long lockTtlMs = 10000;
        public long getPollIntervalMs(){
            return this.pollIntervalMs;
        }
        public void setPollIntervalMs(long pollIntervalMs){
            this.pollIntervalMs= pollIntervalMs;
        }
        public long getLockTtlMs(){
            return this.lockTtlMs;
        }
        public void setLockTtlMs(long lockTtlMs){
            this.lockTtlMs=lockTtlMs;
        }
    }
    //Dinh nghia ReaperConfig
    public static class ReaperConfig{
        private int stuckThresholdMinutes = 5;
        private long pollIntervalMs = 3000;

        public int getStuckThresholdMinutes(){
            return this.stuckThresholdMinutes;
        }
        public void setStuckThresholdMinutes(int stuckThresholdMinutes){
            this.stuckThresholdMinutes= stuckThresholdMinutes;
        }

        public long getPollIntervalMs(){
            return this.pollIntervalMs;
        }
        public void setPollIntervalMs(long pollIntervalMs){
            this.pollIntervalMs= pollIntervalMs;
        }

    }
}
