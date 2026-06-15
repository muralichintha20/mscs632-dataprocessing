package dps;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entry point for the Java Data Processing System.
 *
 * Builds a shared queue, seeds it with data records (including one deliberately
 * malformed record), then runs a fixed pool of worker threads through an
 * ExecutorService. The producer closes the queue once all tasks are enqueued,
 * which lets every worker terminate without poison pills or deadlock.
 */
public final class DataProcessingSystem {

    private static final int WORKER_COUNT = 4;

    public static void main(String[] args) {
        try (FileWriter logFile = new FileWriter("log_java.txt");
             FileWriter resultFile = new FileWriter("results_java.csv");
             Logger log = new Logger(logFile);
             ResultsSink sink = new ResultsSink(resultFile)) {

            TaskQueue queue = new TaskQueue();
            AtomicInteger processed = new AtomicInteger();
            AtomicInteger errors = new AtomicInteger();

            log.info("=== Java Data Processing System starting with "
                    + WORKER_COUNT + " workers ===");

            ExecutorService pool = Executors.newFixedThreadPool(WORKER_COUNT,
                    runnable -> {
                        Thread t = new Thread(runnable);
                        t.setName("worker-" + t.threadId());
                        return t;
                    });

            for (int i = 0; i < WORKER_COUNT; i++) {
                pool.submit(new Worker(queue, sink, log, processed, errors));
            }

            // Produce work. One record (id 5) is intentionally malformed.
            List<String> records = List.of(
                    "alpha,10", "bravo,25", "charlie,7", "delta,40",
                    "echo,not_a_number", "foxtrot,15", "golf,33", "hotel,5",
                    "india,28", "juliet,12");
            int id = 1;
            for (String record : records) {
                queue.addTask(new Task(id++, record));
            }
            log.info("enqueued " + records.size() + " tasks; closing queue");
            queue.close();

            // Orderly shutdown: stop accepting work, then wait for completion.
            pool.shutdown();
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                log.error("timeout waiting for workers; forcing shutdown");
                pool.shutdownNow();
            }

            log.info("=== Done. processed=" + processed.get()
                    + " errors=" + errors.get()
                    + " written=" + sink.count() + " ===");

        } catch (IOException ioe) {
            System.err.println("Fatal I/O error: " + ioe.getMessage());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted: " + ie.getMessage());
        }
    }
}
