import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.slf4j.LoggerFactory.getLogger;

public class InterProcessLockingMain {
    private static final Logger LOG = getLogger(InterProcessLockingMain.class);
    private static final String LOCK_PATH = "/locks/shared";
    private static final ExecutorService pool = Executors.newFixedThreadPool(10);

    public static void main(String[] args) throws Exception {
        CuratorFramework client = createCuratorClient();

        LOG.info("first client starts long work, second tries on a lock, but times out");
        pool.submit(new Worker(1, client, 3, 1));
        pool.submit(new Worker(2, client, 3, 1));
        LOG.info(Strings.repeat("-", 100));

        Thread.sleep (10000);

        LOG.info("first client starts long work on the same lock again, second tries on a lock, but times out");
        pool.submit(new Worker(3, client, 3, 1));
        pool.submit(new Worker(4, client, 3, 1));
        LOG.info(Strings.repeat("-", 100));

        Thread.sleep (10000);

        LOG.info("first client finishes before the second client's lock times out, so second client can proceed");
        pool.submit(new Worker(5, client, 1, 1));
        pool.submit(new Worker(6, client, 2, 5));
        LOG.info(Strings.repeat("-", 100));
    }

    private static CuratorFramework createCuratorClient() throws Exception {
        TestingServer server = new TestingServer();
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        client.start();

        return client;
    }

    private static final class Worker implements Runnable {
        private final int id;
        private final CuratorFramework client;
        private final int workDurationSec;
        private final int lockTimeoutSec;

        private Worker(int id, CuratorFramework client, int workDurationSec, int lockTimeoutSec) {
            this.id = id;
            this.client = client;
            this.workDurationSec = workDurationSec;
            this.lockTimeoutSec = lockTimeoutSec;
        }

        @Override
        public void run() {
            LOG.info("starting worker {} with lock timeout {} sec and work duration {} sec", id, lockTimeoutSec, workDurationSec);
            InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, LOCK_PATH);
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                if (lock.acquire(lockTimeoutSec, TimeUnit.SECONDS)) {
                    LOG.info("lock acquired for instance {} in {} msec, doing some work for {} sec",
                            id, stopwatch.elapsed(TimeUnit.MILLISECONDS), workDurationSec);
                    Thread.sleep(1000 * workDurationSec);
                    LOG.info("work done in instance {}, releasing lock", id);
                } else {
                    LOG.warn("timed out in instance {} after {} msec while waiting for lock",
                            id, stopwatch.elapsed(TimeUnit.MILLISECONDS));
                }
            } catch (Exception e) {
                LOG.error("failed to acquire lock in {}", id, e);
            } finally {
                if (lock.isAcquiredInThisProcess()) {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        LOG.warn("exception on releasing the lock; ignoring", e);
                    }
                }
            }
            LOG.info("all done in instance {}", id);
        }
    }
}
