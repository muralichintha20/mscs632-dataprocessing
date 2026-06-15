package dps;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared destination for processed results. Guards both an in-memory list and
 * the output file with a single intrinsic lock so concurrent writers never
 * corrupt the file or lose entries.
 */
public final class ResultsSink implements AutoCloseable {

    private final List<Result> results = new ArrayList<>();
    private final PrintWriter fileOut;
    private final Object lock = new Object();

    public ResultsSink(Writer fileWriter) {
        this.fileOut = new PrintWriter(fileWriter, true);
        this.fileOut.println("taskId,worker,value,summary");
    }

    /**
     * Record one result. The synchronized block keeps the list mutation and
     * the file append atomic with respect to other worker threads.
     *
     * @throws IOException if the underlying stream reports a write failure
     */
    public void write(Result result) throws IOException {
        synchronized (lock) {
            results.add(result);
            fileOut.printf("%d,%s,%d,%s%n",
                    result.getTaskId(), result.getWorker(),
                    result.getValue(), result.getSummary());
            if (fileOut.checkError()) {
                throw new IOException("Failed writing result for task " + result.getTaskId());
            }
        }
    }

    public int count() {
        synchronized (lock) {
            return results.size();
        }
    }

    public List<Result> snapshot() {
        synchronized (lock) {
            return new ArrayList<>(results);
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            fileOut.flush();
            fileOut.close();
        }
    }
}
