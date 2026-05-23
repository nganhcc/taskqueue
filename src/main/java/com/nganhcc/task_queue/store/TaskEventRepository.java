package com.nganhcc.task_queue.store;

import java.util.List;
import java.util.UUID;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import com.nganhcc.task_queue.model.TaskEvent;

public interface TaskEventRepository extends JpaRepository<TaskEvent, UUID> {

    List<TaskEvent> findByTaskIdOrderByCreatedAtAsc(UUID taskId);

    Page<TaskEvent> findByTaskId(UUID taskId, Pageable pageable);
}
