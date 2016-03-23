import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.slf4j.LoggerFactory.getLogger;

public class Scheduler {
    private static final Logger LOG = getLogger(Scheduler.class);
    private final int id;

    private int count = 0; // state

    private ExecutorService executor;

    public Scheduler(int id) {
        this.id = id;
    }

    public void standby() {
        LOG.info("shutting down scheduler {}", id);
        executor.shutdownNow();
        executor = null;
    }

    private void runInternal() {
        while (true) {
            LOG.info("scheduler {} is running scheduled task {} in {}", id, ++count, this);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                LOG.warn("interrupted in {}, while performing task {}; we are being asked to stop; complying gracefully", this, count);
                Thread.interrupted();
                return;
            }
        }
    }

    public void start() {
        LOG.info("starting scheduler {}", id);
        executor = Executors.newSingleThreadExecutor();
        executor.submit(this::runInternal);
    }
}
