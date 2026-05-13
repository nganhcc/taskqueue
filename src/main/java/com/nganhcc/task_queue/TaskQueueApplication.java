package com.nganhcc.task_queue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TaskQueueApplication {

	public static void main(String[] args) {
		SpringApplication.run(TaskQueueApplication.class, args);
	}

}
