package com.yahoo.ycsb.db;

import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.RiakFutureListener;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.operations.FetchOperation;
import com.basho.riak.client.core.operations.ListKeysOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public final class RiakFunctions implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(RiakFunctions.class);
    private RiakCluster cluster;
    private final int NUMBER_OF_PARALLEL_REQUESTS;

    private RiakFunctions() { NUMBER_OF_PARALLEL_REQUESTS = -1;}

    private RiakFunctions(RiakCluster cluster) {
        this.cluster = cluster;
        NUMBER_OF_PARALLEL_REQUESTS = cluster.getNodes().get(0).getMinConnections();
    }

    public void resetAndEmptyBucket(Namespace namespace) throws InterruptedException, ExecutionException
    {
        logger.debug("\n----------\n[Bucket RESET]  {}", namespace);

        ListKeysOperation.Response response =  cluster.execute(
                new ListKeysOperation.Builder(namespace).build()
            ).get();

        final List<BinaryValue> keyList = response.getKeys();

        final Semaphore semaphore = new Semaphore(NUMBER_OF_PARALLEL_REQUESTS);
        final CountDownLatch latch = new CountDownLatch(keyList.size());

        final RiakFutureListener<Void, Location> listener = new RiakFutureListener<Void, Location>() {
            @Override
            public void handle(RiakFuture<Void, Location> f)
            {
                try
                {
                    f.get();
                }
                catch (Exception ex)
                {
                    if (ex instanceof RuntimeException)
                    {
                        throw (RuntimeException)ex;
                    }
                    throw new RuntimeException(ex);
                }
                semaphore.release();
                latch.countDown();
            }

        };

        for (BinaryValue k : keyList)
        {
            final DeleteOperation delOp = new DeleteOperation.Builder(
                    new Location(namespace, k)
                ).build();

            delOp.addListener(listener);
            semaphore.acquire();
            cluster.execute(delOp);
        }

        // -- wait until completion of all started operations
        logger.trace("All operations were initiated, waiting for completion");
        latch.await();

        // -- waiting until the bucket become really empty
        int attempts = 10;
        boolean bucketIsClean;
        do {
            Thread.sleep(500);
            response = cluster.execute(new ListKeysOperation.Builder(namespace).build()).get();
            attempts -= 1;
            bucketIsClean = !response.getKeys().iterator().hasNext();
        } while(attempts>0 && !bucketIsClean);

        if (!bucketIsClean) {
            throw new IllegalStateException(
                    String.format("Bucket '%s' is not empty after truncation", namespace)
                );
        }

        logger.debug("\n----------\nBucket '{}' has been reset. All existed values were removed", namespace);
    }

    public void awaitWhileAvailable( Location location) throws ExecutionException, InterruptedException {
        for (;;) {
            FetchOperation op = new FetchOperation.Builder(location)
                    .withR(Quorum.allQuorum().getIntValue())
                    .build();

            final FetchOperation.Response r = cluster.execute(op).get();
            if (!r.isNotFound()) {
                break;
            }
            Thread.sleep(500);
        }
    }

    @Override
    public void close() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    public static RiakFunctions create(RiakNode.Builder builder, int defaultPort, String... hosts) throws UnknownHostException {
        final RiakCluster cluster = new RiakCluster.Builder(builder, defaultPort, hosts).build();
        cluster.start();

        return create(cluster);
    }

    public static RiakFunctions create(RiakCluster cluster) throws UnknownHostException {
        return new RiakFunctions(cluster);
    }
}
