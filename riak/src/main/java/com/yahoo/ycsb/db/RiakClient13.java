package com.yahoo.ycsb.db;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterClientFactory;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.basho.riak.pbc.RiakClient;
import com.yahoo.ycsb.*;
import java.io.IOException;
import java.util.*;

import static com.yahoo.ycsb.db.Constants.*;

public class RiakClient13 extends DB {
    private RawClient rawClient;
    private boolean use2i = false;
    private static int connectionNumber = 0;

    public static final int OK = 0;
    public static final int ERROR = -1;

    Map<String, Long> bucketIndexes = new HashMap<String, Long>();

    private int getIntProperty(Properties props, String propname, int defaultValue) {
        String stringProp = props.getProperty(propname, "" + defaultValue);
        return Integer.parseInt(stringProp);
    }

    public void init() throws DBException {
        try {
            Properties props = getProperties();
            use2i = Boolean.parseBoolean(props.getProperty(RIAK_USE_2I, RIAK_USE_2I_DEFAULT));
            String serializerName = props.getProperty(RIAK_SERIALIZER, RIAK_DEFAULT_SERIALIZER);
            String cluster_hosts = props.getProperty(RIAK_CLUSTER_HOSTS, RIAK_CLUSTER_HOST_DEFAULT);
            String[] servers = cluster_hosts.split(",");
            boolean useConnectionPool = false;
            if(props.containsKey(RIAK_POOL_ENABLED)
                    && props.getProperty(RIAK_POOL_ENABLED).equalsIgnoreCase("true")) {
                String usePool = props.getProperty(RIAK_POOL_ENABLED);
                useConnectionPool = Boolean.parseBoolean(usePool);
            }
            if(useConnectionPool) {
                setupConnectionPool(props, servers);
            }  else {
                setupConnections(props, servers);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException("Error connecting to Riak: " + e.getMessage());
        }
    }

    private void setupConnections(Properties props, String[] servers) throws IOException {
        String server = servers[connectionNumber++ % servers.length];
        String[] ipAndPort = server.split(":");
        String ip = ipAndPort[0].trim();
        int port = Integer.parseInt(ipAndPort[1].trim());
        //System.out.println("Riak connection to " + ip + ":" + port);
        RiakClient pbcClient = new RiakClient(ip, port);
        rawClient = new PBClientAdapter(pbcClient);
    }

    private void setupConnectionPool(Properties props, String[] servers) throws IOException {

        int poolMaxConnections =
                getIntProperty(props,
                        RIAK_POOL_TOTAL_MAX_CONNECTION,
                        RIAK_POOL_TOTAL_MAX_CONNECTIONS_DEFAULT);
        int poolIdleConnectionTtlMillis =
                getIntProperty(props,
                        RIAK_POOL_IDLE_CONNECTION_TTL_MILLIS,
                        RIAK_POOL_IDLE_CONNETION_TTL_MILLIS_DEFAULT);
        int poolInitialPoolSize =
                getIntProperty(props,
                        RIAK_POOL_INITIAL_POOL_SIZE,
                        RIAK_POOL_INITIAL_POOL_SIZE_DEFAULT);
        int poolRequestTimeoutMillis =
                getIntProperty(props,
                        RIAK_POOL_REQUEST_TIMEOUT_MILLIS,
                        RIAK_POOL_REQUEST_TIMEOUT_MILLIS_DEFAULT);
        int poolConnectionTimeoutMillis =
                getIntProperty(props,
                        RIAK_POOL_CONNECTION_TIMEOUT_MILLIS,
                        RIAK_POOL_CONNECTION_TIMEOUT_MILLIS_DEFAULT);

        PBClusterConfig clusterConf = new PBClusterConfig(poolMaxConnections);
        for(String server:servers) {
            String[] ipAndPort = server.split(":");
            String ip = ipAndPort[0].trim();
            int port = Integer.parseInt(ipAndPort[1].trim());
            //System.out.println("Riak connection to " + ip + ":" + port);
            PBClientConfig node = PBClientConfig.Builder
                    .from(PBClientConfig.defaults())
                    .withHost(ip)
                    .withPort(port)
                    .withIdleConnectionTTLMillis(poolIdleConnectionTtlMillis)
                    .withInitialPoolSize(poolInitialPoolSize)
                    .withRequestTimeoutMillis(poolRequestTimeoutMillis)
                    .withConnectionTimeoutMillis(poolConnectionTimeoutMillis)
                    .build();
            clusterConf.addClient(node);
        }
        rawClient = PBClusterClientFactory.getInstance().newClient(clusterConf);
    }

    public void cleanup() throws DBException {
        rawClient.shutdown();
    }

    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        try {
            for(String k : fields) {
                RiakResponse response = rawClient.fetch(table + ":" + key, k);
                if(response.hasValue()) {
                    IRiakObject obj = response.getRiakObjects()[0];
                    result.put(k, new ByteArrayByteIterator(obj.getValue()));
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return ERROR;
    }

    public int update(String table, String key,
                      HashMap<String, ByteIterator> values) {
        insert(table, key, values);
        return OK;
    }

    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        try {
            for(String k: values.keySet()) {
                RiakObjectBuilder builder =
                        RiakObjectBuilder.newBuilder(table + ":" + key, k)
                                .withContentType("application/octet-stream")
                        .withValue(values.get(k).toString());
                rawClient.store(builder.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }

    public int delete(String bucket, String key) {
        try {
            rawClient.delete(bucket, key);
        } catch (IOException e) {
            e.printStackTrace();
            return ERROR;
        }
        return OK;
    }



    public static void main(String[] args)
    {
        RiakClient13 cli = new RiakClient13();

        Properties props = new Properties();
        props.setProperty(RIAK_CLUSTER_HOSTS, "localhost:10017, localhost:10027, localhost:10037");

        cli.setProperties(props);

        try {
            cli.init();
        } catch(Exception e) {
            e.printStackTrace();
        }

        String bucket = "people";
        String key = "person1";

        {
            HashMap<String, String> values = new HashMap<String, String>();
            values.put("first_name", "Dave");
            values.put("last_name", "Parfitt");
            values.put("city", "Buffalo, NY");
            cli.insert(bucket, key, StringByteIterator.getByteIteratorMap(values));
            System.out.println("Added person");
        }

        {
            Set<String> fields = new HashSet<String>();
            fields.add("first_name");
            fields.add("last_name");
            HashMap<String, ByteIterator> results = new HashMap<String, ByteIterator>();
            cli.read(bucket, key,fields, results);
            System.out.println(results.toString());
            System.out.println("Read person");
        }

        {
            HashMap<String, String> updateValues = new HashMap<String, String>();
            updateValues.put("twitter", "@metadave");
            cli.update("people", "person1", StringByteIterator.getByteIteratorMap(updateValues));
            System.out.println("Updated person");
        }

        {
            HashMap<String, ByteIterator> finalResults = new HashMap<String, ByteIterator>();
            cli.read(bucket, key, null, finalResults);
            System.out.println(finalResults.toString());
            System.out.println("Read person");
        }
    }
}
