package pl.ftang.storm.trident;


import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.testing.TestJob;
import backtype.storm.testing.TrackedTopology;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

/**
 * @author mcl
 */
public class WordCountTopologyTest {

    static Logger log = LoggerFactory.getLogger(WordCountTopologyTest.class);

    @Test
    public void shouldTestAckingInTopology() {
        Testing.withTrackedCluster(new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws IOException {
                FeederBatchSpout feederSpout = createFeederSpout("sentence");

                /*FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                        new Values("the cow jumped over the moon"),
                        new Values("the man went to the store and bought some candy"),
                        new Values("four score and seven years ago"),
                        new Values("how many apples can you eat"));
                spout.setCycle(true);*/

                TridentTopology topology = new TridentTopology();
                TridentState wordCounts =
                        topology.newStream("spout1", feederSpout)
                                .each(new Fields("sentence"), new Split(), new Fields("word"))
                                .groupBy(new Fields("word"))
                                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                                .parallelismHint(6);

                TrackedTopology tracked = Testing.mkTrackedTopology(cluster, topology.build());

                try {
                    cluster.submitTopology("experiments-acking2", createConfig(), tracked.getTopology());
                } catch (AlreadyAliveException e) {
                    log.error("Alive exception", e);
                } catch (InvalidTopologyException e) {
                    log.error("Invalid topology exception", e);
                }

                feederSpout.feed(new Values(Lists.newArrayList("a b c a b c")));
                Testing.trackedWait(tracked, 5);
                feederSpout.feed(new Values(Lists.newArrayList("a b c a b c")));
                Testing.trackedWait(tracked, 1);
            }
        });
    }

    private Map createConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    private FeederBatchSpout createFeederSpout(String... fields) {
        return new FeederBatchSpout(Arrays.asList(fields));
    }

    public static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
