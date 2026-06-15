package dps;

/**
 * Immutable unit of work placed on the shared queue.
 *
 * Each Task carries an identifier and a raw data record (a comma-separated
 * string). Worker threads retrieve a Task, parse and transform its payload,
 * and emit a Result. Records that cannot be parsed deliberately raise an
 * exception so the error-handling and logging paths can be exercised.
 */
public final class Task {

    private final int id;
    private final String record;

    public Task(int id, String record) {
        this.id = id;
        this.record = record;
    }

    public int getId() {
        return id;
    }

    public String getRecord() {
        return record;
    }

    @Override
    public String toString() {
        return "Task#" + id + "{" + record + "}";
    }
}
