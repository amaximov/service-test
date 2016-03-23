import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
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
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            clients.add(client);
            client.start();

            final int finalI = i;
            LeaderSelectorListener listener = new LeaderSelectorListenerAdapter() {
                private final Scheduler scheduler = new Scheduler(finalI);

                @Override
                public void takeLeadership(CuratorFramework client) throws Exception {
                    LOG.info("i am a leader now: {} with id {}", this, finalI);
                    scheduler.start();

                    // in real world, leadership will never be taken away, only jvm death will do it
                    System.out.println("\nPress Enter to make me lose leadership: \n");
                    System.in.read(); // blocking wait; probably entered at startup and remain here until jvm shutdown
                    // in real world, Thread.currentThread().join() miht work as well

                    scheduler.standby(); // can wait for scheduled tasks to complete before relinquishing
                    // should also implement bean pre-destroy to cleanly shut down and relinquish leadership
                }
            };

            LeaderSelector selector = new LeaderSelector(client, PATH, listener);
            selector.autoRequeue(); // not in real code, since takeLeadership() will never return in usual circumstances
            selector.start();
        }

        LOG.info("{} clients started: {}", clients.size(), clients);
    }
}
