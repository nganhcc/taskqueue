package com.nganhcc.task_queue.store;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;

public interface TaskRepository extends JpaRepository<Task, UUID> {

    List<Task> findByStatus(TaskStatus status);

    List<Task> findByQueueAndStatus(String queue, TaskStatus status);

    List<Task> findByStatusAndRunAtLessThanEqual(TaskStatus status, Instant runAt);

    List<Task> findByStatusAndStartedAtBefore(TaskStatus status, Instant startedAt);
}
