package dps;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Minimal thread-safe logger. Every record is timestamped and tagged with the
 * calling thread name, then mirrored to the console and to a log file.
 *
 * All public methods are synchronized so that messages emitted from many
 * worker threads never interleave mid-line on the shared streams.
 */
public final class Logger implements AutoCloseable {

    private static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final PrintWriter fileOut;

    public Logger(Writer fileWriter) {
        this.fileOut = new PrintWriter(fileWriter, true);
    }

    public synchronized void info(String message) {
        write("INFO ", message);
    }

    public synchronized void error(String message) {
        write("ERROR", message);
    }

    private void write(String level, String message) {
        String line = String.format("%s [%s] %-5s %s",
                LocalTime.now().format(TS),
                Thread.currentThread().getName(),
                level,
                message);
        System.out.println(line);
        fileOut.println(line);
    }

    @Override
    public synchronized void close() throws IOException {
        fileOut.flush();
        fileOut.close();
    }
}
