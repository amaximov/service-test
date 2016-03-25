import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class ServiceMain {
    private static final Logger LOG = getLogger(ServiceMain.class);
    private static final String PATH = "/service/leader";

    private static final int CLIENT_COUNT = 2;


    public static void main(String[] args) throws Exception {
        LOG.info("ServiceMain.main");

        List<CuratorFramework> clients = Lists.newLinkedList();

        TestingServer server = new TestingServer();
        for (int i = 0; i < CLIENT_COUNT; i++) {
            clients.add(startNewClient(server, i));
        }

        LOG.info("{} clients started: {}", clients.size(), clients);

        // imitating late join to verify singleton service stickiness
        Thread.sleep(15000);
        LOG.info("starting another client (service):");
        clients.add(startNewClient(server, Integer.MIN_VALUE));
    }

    private static CuratorFramework startNewClient(TestingServer server, int i) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        client.start();

        LeaderSelector selector = new LeaderSelector(client, PATH, new SchedulerService(i));
        selector.autoRequeue(); // not in real code, since takeLeadership() will never return in usual circumstances
        selector.start();

        return client;
    }

    private static final class SchedulerService extends LeaderSelectorListenerAdapter {
        private final int id;
        private final Scheduler scheduler;

        private SchedulerService(int id) {
            this.id = id;
            this.scheduler = new Scheduler(id);
        }

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            LOG.info("i am a leader now: {} with id {}", this, id);
            scheduler.start();

            // in real world, leadership will never be taken away, only jvm death will do it
            System.out.println("\nPress Enter to make me lose leadership: \n");
            System.in.read(); // blocking wait; probably entered at startup and remain here until jvm shutdown
            // in real world, Thread.currentThread().join() might work as well

            scheduler.standby(); // can wait for scheduled tasks to complete before relinquishing
            // should also implement bean pre-destroy to cleanly shut down and relinquish leadership
        }
    }
}
