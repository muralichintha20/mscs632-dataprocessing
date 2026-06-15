package dps;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded-by-nothing, thread-safe FIFO queue guarding a plain ArrayDeque
 * with an explicit ReentrantLock and a Condition.
 *
 * The queue distinguishes "empty for now" from "empty forever". A producer
 * calls {@link #close()} once all tasks have been enqueued. After that point,
 * a consumer that drains the queue receives an empty Optional instead of
 * blocking forever, which is what lets every worker terminate cleanly without
 * poison pills or a deadlock.
 */
public final class TaskQueue {

    private final Deque<Task> items = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private boolean closed = false;

    /** Enqueue a task. Rejects work once the queue has been closed. */
    public void addTask(Task task) {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Cannot add to a closed queue");
            }
            items.addLast(task);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieve the next task, blocking while the queue is empty but still open.
     * Returns an empty Optional once the queue is both empty and closed, which
     * signals the calling worker that no further work will ever arrive.
     *
     * @throws InterruptedException if the worker thread is interrupted while waiting
     */
    public Optional<Task> getTask() throws InterruptedException {
        lock.lock();
        try {
            while (items.isEmpty() && !closed) {
                notEmpty.await();
            }
            if (items.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(items.removeFirst());
        } finally {
            lock.unlock();
        }
    }

    /** Mark the queue as closed and wake every waiting worker. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int remaining() {
        lock.lock();
        try {
            return items.size();
        } finally {
            lock.unlock();
        }
    }
}
