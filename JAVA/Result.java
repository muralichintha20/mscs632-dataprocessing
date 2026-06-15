package dps;

/**
 * The outcome of processing a single Task.
 *
 * A Result records which worker handled the Task, a derived numeric value,
 * and a human-readable summary that is written to the shared output file.
 */
public final class Result {

    private final int taskId;
    private final String worker;
    private final long value;
    private final String summary;

    public Result(int taskId, String worker, long value, String summary) {
        this.taskId = taskId;
        this.worker = worker;
        this.value = value;
        this.summary = summary;
    }

    public int getTaskId() {
        return taskId;
    }

    public String getWorker() {
        return worker;
    }

    public long getValue() {
        return value;
    }

    public String getSummary() {
        return summary;
    }

    @Override
    public String toString() {
        return String.format("Task#%-2d  worker=%-9s  value=%-6d  %s",
                taskId, worker, value, summary);
    }
}
