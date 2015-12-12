/*
 * Copyright 2015 Basho Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.ycsb.db;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.timeseries.Row;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public abstract class AbstractRiakClient extends DB {

    protected static class Config {
        private static final String HOST_PROPERTY = "riak.hosts";
        private static final String PORT_PROPERTY = "riak.default_port";
        private static final String BUCKET_TYPE_PROPERTY = "riak.bucket_type";
        private static final String R_VALUE_PROPERTY = "riak.r_val";
        private static final String W_VALUE_PROPERTY = "riak.w_val";
        private static final String READ_RETRY_COUNT_PROPERTY = "riak.read_retry_count";
        private static final String DEBUG_PROPERTY = "riak.debug";

        private String bucketType;
        private int defaultPort;
        private String hosts;
        private int r_value;
        private int w_value;
        private int readRetryCount;
        private boolean debug;
        private int threadCount;

        private Config() {}

        public int readRetryCount() {
            return readRetryCount;
        }

        public static Config create(Properties props) {
            final Config cfg = new Config();

            cfg.defaultPort = Integer.parseInt(
                    props.getProperty(PORT_PROPERTY, Integer.toString(RiakNode.Builder.DEFAULT_REMOTE_PORT))
                );

            cfg.hosts = props.getProperty(HOST_PROPERTY, RiakNode.Builder.DEFAULT_REMOTE_ADDRESS);

            cfg.bucketType = props.getProperty(BUCKET_TYPE_PROPERTY, "ycsb");

            cfg.r_value = Integer.parseInt(
                    props.getProperty(R_VALUE_PROPERTY, "2")
                );

            cfg.w_value = Integer.parseInt(
                    props.getProperty(W_VALUE_PROPERTY, "2")
            );

            cfg.readRetryCount = Integer.parseInt(
                    props.getProperty(READ_RETRY_COUNT_PROPERTY, "5")
            );

            cfg.debug = Boolean.parseBoolean(
                    props.getProperty(DEBUG_PROPERTY, "false")
            );

            cfg.threadCount = Integer.parseInt(
                            props.getProperty(Client.THREAD_COUNT_PROPERTY, "1")
                    );
            return cfg;
        }

        public Namespace mkNamespaceFor(String table) {
            return new Namespace(bucketType, table);
        }

        public Location mkLocationFor(String table, String key) {
            return new Location(mkNamespaceFor(table), key);
        }

        public Quorum readQuorum() {
            return new Quorum(r_value);
        }

        public Quorum writeQuorum() {
            return new Quorum(w_value);
        }

        public RiakCluster createRiakCluster() throws UnknownHostException {
            return new RiakCluster.Builder(
                            new RiakNode.Builder()
                                    // Update number of connections based on threads
                                .withMinConnections(threadCount)
                                .withMaxConnections(threadCount)
                                .withRemotePort(this.defaultPort),
                            this.hosts
                        ).build();
        }

        public String getBucketType() {
            return bucketType;
        }

        public boolean isDebug() {
            return debug;
        }
    }

    /**
     * Count the number of times initialized to teardown on the last
     * {@link #cleanup()}.
     */
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

    private static Config config;
    protected static RiakClient riakClient;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());


    protected Config config() {
        assert config != null;
        return config;
    }

    @Override
    public void init() throws DBException {
        super.init();

        // Keep track of number of calls to init (for later cleanup)
        INIT_COUNT.incrementAndGet();

        // Synchronized so that we only have a single
        // cluster/session instance for all the threads.
        synchronized (INIT_COUNT) {

            if (config != null) {
                assert riakClient != null;
                return;
            }

            config = Config.create(getProperties());

            try {
                final RiakCluster riakCluster = config.createRiakCluster();
                riakCluster.start();

                riakClient = new RiakClient(riakCluster);
            } catch (UnknownHostException e) {
                throw new DBException("Can't create Riak Cluster", e);
            }

            debugPrint("\n\n%s client has been initialized with the following configuration:\n" +
                            "\tHosts:        %s\n" +
                            "\tDefault Port: %d\n" +
                            "\tBucket type:  %s\n" +
                            "\tThread cnt:   %d\n" +
                            "\tR Val:        %d\n" +
                            "\tW Val:        %d\n" +
                            "\tRead Retry Count: %d\n\n",
                    this.getClass().getSimpleName(),
                    config.hosts,
                    config.defaultPort,
                    config.bucketType,
                    config.threadCount,
                    config.r_value,
                    config.w_value,
                    config.readRetryCount
            );
        }
    }

    @Override
    public void cleanup() throws DBException
    {
        synchronized (INIT_COUNT) {
            final int curInitCount = INIT_COUNT.decrementAndGet();
            if (curInitCount <= 0) {
                if (riakClient != null) {
                    riakClient.shutdown();
                }

                if (config != null) {
                    config = null;
                }
            }

            if (curInitCount < 0) {
                // This should never happen.
                throw new DBException(
                        String.format("initCount is negative: %d", curInitCount));
            }
        }
    }

    protected void debugPrint(String str, Object... params) {
        if (config.debug) {
            if (params.length == 0) {
                System.out.println(str);
            } else {
                System.out.println("[tid:" + Thread.currentThread().getId() + "] " + String.format(str, params));
            }
        }
    }

    protected void dumpOperation(Row row, String operationTemplate, Object... params) {
        if (config.debug) {
            final String str = String.format(operationTemplate, params);
            System.out.println("[tid:" + Thread.currentThread().getId() +", " + str + "] " + (row == null ? "" : row.getCellsCopy()));
        }
    }
}
