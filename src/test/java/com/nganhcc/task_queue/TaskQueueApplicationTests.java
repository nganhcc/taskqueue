package com.nganhcc.task_queue;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;

class TaskQueueApplicationTests {

	@Test
	void applicationClassIsSpringBootApplication() {
		assertThat(TaskQueueApplication.class).hasAnnotation(SpringBootApplication.class);
	}

}
