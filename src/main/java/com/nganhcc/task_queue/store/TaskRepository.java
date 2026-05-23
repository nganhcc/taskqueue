package com.nganhcc.task_queue.store;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import com.nganhcc.task_queue.model.Task;
import com.nganhcc.task_queue.model.TaskStatus;

public interface TaskRepository extends JpaRepository<Task, UUID> {

    List<Task> findByStatus(TaskStatus status);

    Page<Task> findByStatus(TaskStatus status, Pageable pageable);

    List<Task> findByQueueAndStatus(String queue, TaskStatus status);

    Page<Task> findByQueueAndStatus(String queue, TaskStatus status, Pageable pageable);

    List<Task> findByStatusAndRunAtLessThanEqual(TaskStatus status, Instant runAt);

    List<Task> findByStatusAndStartedAtBefore(TaskStatus status, Instant startedAt);

    List<Task> findByQueue(String queue);

    Page<Task> findByQueue(String queue, Pageable pageable);

    long countByStatus(TaskStatus status);

    List<Task> findByQueueAndStatusIn(String queue, List<TaskStatus> statuses);

    List<Task>  findByStatusAndRunAtIsNotNull(TaskStatus status);
    
    long deleteByStatusAndCreatedAtBefore(TaskStatus status, Instant cutoff);

    Optional<Task> findByQueueAndIdempotencyKey(String queue, String idempotencyKey);

    List<Task> findByStatusAndHeartbeatAtBefore(TaskStatus status, Instant heartbeatAt);
}
