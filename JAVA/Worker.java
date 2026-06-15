package dps;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A worker pulls tasks from the shared queue, processes each one with a
 * simulated computational delay, and writes a Result to the shared sink.
 *
 * The worker exits cleanly when the queue is drained and closed. All failure
 * modes (interruption, malformed records, write errors) are caught and logged
 * so a single bad task never brings down the whole pool.
 */
public final class Worker implements Runnable {

    private final TaskQueue queue;
    private final ResultsSink sink;
    private final Logger log;
    private final AtomicInteger processedCounter;
    private final AtomicInteger errorCounter;

    public Worker(TaskQueue queue, ResultsSink sink, Logger log,
                  AtomicInteger processedCounter, AtomicInteger errorCounter) {
        this.queue = queue;
        this.sink = sink;
        this.log = log;
        this.processedCounter = processedCounter;
        this.errorCounter = errorCounter;
    }

    @Override
    public void run() {
        log.info("started");
        try {
            while (true) {
                Optional<Task> next = queue.getTask();
                if (next.isEmpty()) {
                    break; // queue drained and closed: no more work will arrive
                }
                handle(next.get());
            }
        } catch (InterruptedException ie) {
            // Restore the interrupt status and stop quietly.
            Thread.currentThread().interrupt();
            log.error("interrupted while waiting for work; shutting down");
        }
        log.info("finished");
    }

    private void handle(Task task) {
        try {
            Result result = process(task);
            sink.write(result);
            processedCounter.incrementAndGet();
            log.info("completed " + task + " -> value=" + result.getValue());
        } catch (NumberFormatException nfe) {
            errorCounter.incrementAndGet();
            log.error("bad record in " + task + ": " + nfe.getMessage());
        } catch (IOException ioe) {
            errorCounter.incrementAndGet();
            log.error("I/O failure writing " + task + ": " + ioe.getMessage());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("interrupted during " + task);
        }
    }

    /**
     * Simulates real work: a short randomized delay followed by parsing the
     * record "label,n" and computing the sum of squares from 1..n. A record
     * whose numeric field is not an integer triggers a NumberFormatException
     * that the caller logs and skips.
     */
    private Result process(Task task) throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextInt(40, 160));

        String[] parts = task.getRecord().split(",", 2);
        String label = parts[0].trim();
        int n = Integer.parseInt(parts[1].trim()); // may throw NumberFormatException

        long sumOfSquares = 0;
        for (int i = 1; i <= n; i++) {
            sumOfSquares += (long) i * i;
        }
        String summary = label + "[1.." + n + "]";
        return new Result(task.getId(), Thread.currentThread().getName(),
                sumOfSquares, summary);
    }
}
