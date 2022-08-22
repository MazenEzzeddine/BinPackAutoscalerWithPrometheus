import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


public class PrometheusHttpClient {

    private static final Logger log = LogManager.getLogger(PrometheusHttpClient.class);

    static Instant lastUpScaleDecision;
    static Instant lastDownScaleDecision;
    static Long sleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    static int size;

    static ArrayList<Partition> topicpartitions = new ArrayList<>();


    static double dynamicTotalMaxConsumptionRate = 0.0;
    static double dynamicAverageMaxConsumptionRate = 0.0;

    static double wsla = 5.0;
    static List<Consumer> assignment;
    static Instant lastScaleUpDecision;
    static Instant lastScaleDownDecision;
    static Instant lastCGQuery;
    static Integer cooldown;



    public static void main(String[] args) throws InterruptedException, ExecutionException, URISyntaxException {
        readEnvAndCrateAdminClient();
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();
        lastScaleUpDecision=  Instant.now();
        lastScaleDownDecision = Instant.now();

        HttpClient client = HttpClient.newHttpClient();
        String all3 = "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
        String p0 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D%5B1m%5D))";
        String p1 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D%5B1m%5D))";
        String p2 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D%5B1m%5D))";
        String p3 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D%5B1m%5D))";
        String p4 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D%5B1m%5D))";
       /* String p5 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%225%22,namespace=%22default%22%7D%5B1m%5D))";
        String p6 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%226%22,namespace=%22default%22%7D%5B1m%5D))";
        String p7 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%227%22,namespace=%22default%22%7D%5B1m%5D))";
        String p8 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%228%22,namespace=%22default%22%7D%5B1m%5D))";
        String p9 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%229%22,namespace=%22default%22%7D%5B1m%5D))";
        String p10 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%2210%22,namespace=%22default%22%7D%5B1m%5D))";
        String p11 =   "http://prometheus-operated:9090/api/v1/query?" +
                "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%2211%22,namespace=%22default%22%7D%5B1m%5D))";*/


        //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
        //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

        String all4 = "http://prometheus-operated:9090/api/v1/query?query=" +
                "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
        String p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D";
        String p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D";
        String p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D";
        String p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D";
        String p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D";
       /* String p5lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%225%22,namespace=%22default%22%7D";
        String p6lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%226%22,namespace=%22default%22%7D";
        String p7lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%227%22,namespace=%22default%22%7D";
        String p8lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%228%22,namespace=%22default%22%7D";
        String p9lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%229%22,namespace=%22default%22%7D";
        String p10lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%2210%22,namespace=%22default%22%7D";
        String p11lag = "http://prometheus-operated:9090/api/v1/query?query=" +
                "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic2%22,partition=%2211%22,namespace=%22default%22%7D";*/

        List<URI> targets = Arrays.asList(
                new URI(all3),
                new URI(all4)
        );

        List<URI> partitions = Arrays.asList(
                new URI(p0),
                new URI(p1),
                new URI(p2),
                new URI(p3),
                new URI(p4)
               /* new URI(p5),
                new URI(p6),
                new URI(p7),
                new URI(p8),
                new URI(p9),
                new URI(p10),
                new URI(p11)*/
                );
        List<URI> partitionslag = Arrays.asList(
                new URI(p0lag),
                new URI(p1lag),
                new URI(p2lag),
                new URI(p3lag),
                new URI(p4lag)
               /* new URI(p5lag),
                new URI(p6lag),
                new URI(p7lag),
                new URI(p8lag),
                new URI(p9lag),
                new URI(p10lag),
                new URI(p11lag)*/
        );


        for (int i = 0; i<=4; i++) {
            topicpartitions.add(new Partition(i, 0, 0));
        }
        log.info("created the 5 partitions");

        while (true) {
            Instant start = Instant.now();
                List<CompletableFuture<String>> futures = targets.stream()
                        .map(target -> client
                                .sendAsync(
                                        HttpRequest.newBuilder(target).GET().build(),
                                        HttpResponse.BodyHandlers.ofString())
                                .thenApply(HttpResponse::body))
                        .collect(Collectors.toList());


            List<CompletableFuture<String>> partitionsfutures = partitions.stream()
                    .map(target -> client
                            .sendAsync(
                                    HttpRequest.newBuilder(target).GET().build(),
                                    HttpResponse.BodyHandlers.ofString())
                            .thenApply(HttpResponse::body))
                    .collect(Collectors.toList());


            List<CompletableFuture<String>> partitionslagfuture = partitionslag.stream()
                    .map(target -> client
                            .sendAsync(
                                    HttpRequest.newBuilder(target).GET().build(),
                                    HttpResponse.BodyHandlers.ofString())
                            .thenApply(HttpResponse::body))
                    .collect(Collectors.toList());


            boolean arrival = true;
            for (CompletableFuture cf : futures) {
                if(arrival) {
                    parseJson((String) cf.get());
                } else {
                    parseJsonLag((String) cf.get());
                }
            arrival = !arrival;
            }

            int partitionn = 0;
            Double totalarrivals=0.0;
            for (CompletableFuture cf : partitionsfutures) {
                topicpartitions.get(partitionn).setArrivalRate(parseJsonArrivalRate((String) cf.get(), partitionn), false);
               totalarrivals += parseJsonArrivalRate((String) cf.get(), partitionn);
                 partitionn++;


            }
            log.info("totalArrivalRate {}", totalarrivals);
          partitionn = 0;
          Double totallag=0.0;
            for (CompletableFuture cf : partitionslagfuture) {
                topicpartitions.get(partitionn).setLag(parseJsonArrivalLag((String) cf.get(), partitionn).longValue(), false);

                totallag += parseJsonArrivalLag((String) cf.get(), partitionn);
                partitionn++;
            }
            log.info("totallag {}", totallag);
            Instant end = Instant.now();
            log.info("Duration in seconds to query prometheus for " +
                            "arrival rate and lag and parse result {}",
                    Duration.between(start,end).toMillis());


            for (int i = 0; i<=4; i++) {
               log.info("partition {} has the following arrival rate {} and lag {}",  i, topicpartitions.get(i).getArrivalRate(),
                       topicpartitions.get(i).getLag()) ;
            }


            log.info("calling the scaler");


            queryConsumerGroup();
            //youMightWanttoScale(totalarrivals);
            youMightWanttoScaleUsingBinPack();

            log.info("sleeping for 5000ms");
            log.info("==================================================");

            try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
            }
        }
    }


    private static  void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(PrometheusHttpClient.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();
        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();
         size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("number of consumers {}", size );
    }

    private static void readEnvAndCrateAdminClient() {
        log.info("inside read env");
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
    }








    //////////////////////////////////////////////////////////////////////////////////////////////////////

    private static void youMightWanttoScaleUsingBinPack() {
        log.info("Calling the bin pack scaler");
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        if(Duration.between(lastScaleUpDecision, Instant.now()).toSeconds() >= 30) {
            scaleAsPerBinPack(size);
        } else {
            log.info("Scale  cooldown period has not elapsed yet not taking decisions");
        }
    }

    public static void scaleAsPerBinPack(int currentsize) {
        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);

        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
          /*  if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*/
        } else if (replicasForscale > 0) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale by {}", replicasForscale);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                log.info("I have Upscaled you should have {}", neededsize);
            }
            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();
            lastCGQuery = Instant.now();
        } else {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                log.info("I have Downscaled you should have {}", neededsize);
                lastScaleUpDecision = Instant.now();
                lastScaleDownDecision = Instant.now();
                lastCGQuery = Instant.now();
            }
        }
    }


    private static int binPackAndScale() {
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;
        List<Partition> parts = new ArrayList<>(topicpartitions);
        dynamicAverageMaxConsumptionRate = 95.0;

        long maxLagCapacity;
        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * wsla);
        consumers.add(new Consumer(consumerCount, maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        // atention to the window
        for (Partition partition : parts) {
            if (partition.getLag() > maxLagCapacity) {
                log.info("Since partition {} has lag {} higher than consumer capacity times wsla {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity, false);
            }
        }
        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f",  partition.getArrivalRate()),
                        String.format("%.2f", dynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate, false);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        Consumer consumer = null;
        for (Partition partition : parts) {
            for (Consumer cons : consumers) {
                //TODO externalize these choices on the inout to the FFD bin pack
                // TODO  hey stupid use instatenous lag instead of average lag.
                // TODO average lag is a decision on past values especially for long DI.
                if (cons.getRemainingLagCapacity() >=  partition.getLag() /*partition.getAverageLag()*/ &&
                        cons.getRemainingArrivalCapacity() >= partition.getArrivalRate()) {
                    cons.assignPartition(partition);
                    // we are done with this partition, go to next
                    break;
                }
                //we have iterated over all the consumers hoping to fit that partition, but nope
                //we shall create a new consumer i.e., scale up
                if (cons == consumers.get(consumers.size() - 1)) {
                    consumerCount++;
                    consumer = new Consumer(consumerCount, (long) (dynamicAverageMaxConsumptionRate * wsla),
                            dynamicAverageMaxConsumptionRate);
                    consumer.assignPartition(partition);
                }
            }
            if (consumer != null) {
                consumers.add(consumer);
                consumer = null;
            }
        }
        log.info(" The BP scaler recommended {}", consumers.size());
        // copy consumers and partitions for fair assignment
        List<Consumer> fairconsumers = new ArrayList<>(consumers.size());
        List<Partition> fairpartitions= new ArrayList<>();

        for (Consumer cons : consumers) {
            fairconsumers.add(new Consumer(cons.getId(), maxLagCapacity, dynamicAverageMaxConsumptionRate));
            for(Partition p : cons.getAssignedPartitions()){
                fairpartitions.add(p);
            }
        }

        //sort partitions in descending order for debugging purposes
        fairpartitions.sort(new Comparator<Partition>() {
            @Override
            public int compare(Partition o1, Partition o2) {
                return Double.compare(o2.getArrivalRate(), o1.getArrivalRate());
            }
        });

        //1. list of consumers that will contain the fair assignment
        //2. list of consumers out of the bin pack.
        //3. the partition sorted in their decreasing arrival rate.
        assignPartitionsFairly(fairconsumers,consumers,fairpartitions);
        for (Consumer cons : fairconsumers) {
            log.info("fair consumer {} is assigned the following partitions", cons.getId() );
            for(Partition p : cons.getAssignedPartitions()) {
                log.info("fair Partition {}", p.getId());
            }
        }
        assignment = fairconsumers;
        return consumers.size();
    }



    public static void assignPartitionsFairly(
            final List<Consumer> assignment,
            final List<Consumer> consumers,
            final List<Partition> partitionsArrivalRate) {
        if (consumers.isEmpty()) {
            return;
        }// Track total lag assigned to each consumer (for the current topic)
        final Map<Integer, Double> consumerTotalArrivalRate = new HashMap<>(consumers.size());
        final Map<Integer, Integer> consumerTotalPartitions = new HashMap<>(consumers.size());
        final Map<Integer, Double> consumerAllowableArrivalRate = new HashMap<>(consumers.size());
        for (Consumer cons : consumers) {
            consumerTotalArrivalRate.put(cons.getId(), 0.0);
            consumerAllowableArrivalRate.put(cons.getId(), 95.0);
        }
        // Track total number of partitions assigned to each consumer (for the current topic)
        for (Consumer cons : consumers) {
            consumerTotalPartitions.put(cons.getId(), 0);
        }
        // might want to remove, the partitions are sorted anyway.
        //First fit decreasing
        partitionsArrivalRate.sort((p1, p2) -> {
            // If lag is equal, lowest partition id first
            if (p1.getArrivalRate() == p2.getArrivalRate()) {
                return Integer.compare(p1.getId(), p2.getId());
            }
            // Highest arrival rate first
            return Double.compare(p2.getArrivalRate(), p1.getArrivalRate());
        });
        for (Partition partition : partitionsArrivalRate) {
            // Assign to the consumer with least number of partitions, then smallest total lag, then smallest id arrival rate
            // returns the consumer with lowest assigned partitions, if all assigned partitions equal returns the min total arrival rate
            final Integer memberId = Collections
                    .min(consumerTotalArrivalRate.entrySet(), (c1, c2) -> {

                        //TODO is that necessary partition count first... not really......
                        //lowest number of partitions first.
                        int comparePartitionCount = Integer.compare(consumerTotalPartitions.get(c1.getKey()),
                                consumerTotalPartitions.get(c2.getKey()));

                        // If partition count is equal, lowest total lag first, get the consumer with the lowest arrival rate
                        int compareTotalLags = Double.compare(c1.getValue(), c2.getValue());
                        if (compareTotalLags != 0) {
                            return compareTotalLags;
                        }
                        // If total arrival rate  is equal, lowest consumer id first
                        return c1.getKey().compareTo(c2.getKey());
                    }).getKey();

            assignment.get(memberId).assignPartition(partition);
            consumerTotalArrivalRate.put(memberId, consumerTotalArrivalRate.getOrDefault(memberId, 0.0) + partition.getArrivalRate());
            consumerTotalPartitions.put(memberId, consumerTotalPartitions.getOrDefault(memberId, 0) + 1);
            log.info(
                    "Assigned partition {} to consumer {}.  partition_arrival_rate={}, consumer_current_total_arrival_rate{} ",
                    partition.getId(),
                    memberId ,
                    String.format("%.2f", partition.getArrivalRate()) ,
                    consumerTotalArrivalRate.get(memberId));
        }
    }






    /////////////////////////////////////////////////////////////////////////////////////////////////////////













    private static void youMightWanttoScale(double totalArrivalRate) throws ExecutionException, InterruptedException {
        int size = consumerGroupDescriptionMap.get(PrometheusHttpClient.CONSUMER_GROUP).members().size();
        log.info("curent group size is {}", size);

        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 30 ) {
            log.info("Upscale logic, Up scale cool down has ended");

            upScaleLogic(totalArrivalRate, size);
        } else {
            log.info("Not checking  upscale logic, Up scale cool down has not ended yet");
        }
        if (Duration.between(lastDownScaleDecision, Instant.now()).toSeconds() >= 30 ) {
            log.info("DownScaling logic, Down scale cool down has ended");
            downScaleLogic(totalArrivalRate, size);
        }else {
            log.info("Not checking  down scale logic, down scale cool down has not ended yet");
        }
    }



    private static Double parseJsonArrivalRate(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //log.info(json);
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject)jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        ///String partition = jobjpartition.getString("partition");
        log.info("the partition is {}", p);
        log.info("partition arrival rate: {}", Double.parseDouble( jreq.getString(1)));
        return Double.parseDouble( jreq.getString(1));
    }


    private static Double parseJsonArrivalLag(String json, int p) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        //log.info(json);
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject)jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");
        log.info("the partition is {}", p);
        log.info("partition lag  {}",  Double.parseDouble( jreq.getString(1)));
        return Double.parseDouble( jreq.getString(1));
    }


    private static Double parseJson(String json) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject)jsonObject.get("data");

        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);

        JSONArray jreq = jobj.getJSONArray("value");
        //log.info("arrival rate: " + Double.parseDouble( jreq.getString(1)));
        return Double.parseDouble( jreq.getString(1));
    }


    private static Double parseJsonLag(String json) {
        //json string from prometheus
        //{"status":"success","data":{"resultType":"vector","result":[{"metric":{"topic":"testtopic1"},"value":[1659006264.066,"144.05454545454546"]}]}}
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject j2 = (JSONObject)jsonObject.get("data");
        JSONArray inter = j2.getJSONArray("result");
        JSONObject jobj = (JSONObject) inter.get(0);
        JSONArray jreq = jobj.getJSONArray("value");

        //System.out.println("lag: " + Double.parseDouble( jreq.getString(1)));
        /*String ts = jreq.getString(0);
        ts = ts.replace(".", "");
        //TODO attention to the case where after the . there are less less than 3 digits
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        Date d = new Date(Long.parseLong(ts));
       *//* log.info(" timestamp {} corresponding date {} :", ts, sdf.format(d));*/
        return Double.parseDouble( jreq.getString(1));
    }



    private static void upScaleLogic(double totalArrivalRate, int size) {
        log.info("current totalArrivalRate {}, group size {}", totalArrivalRate, size);
        if (totalArrivalRate > size *poll) {
            log.info("Consumers are less than nb partition we can scale");
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                log.info("Since  arrival rate {} is greater than  maximum consumption rate " +
                        "{} ,  I up scaled  by one ", totalArrivalRate , size * poll);
            }
            lastUpScaleDecision = Instant.now();
            lastDownScaleDecision = Instant.now();
        }
    }

    private static void downScaleLogic(double totalArrivalRate, int size) {
        if ((totalArrivalRate ) < (size - 1) * poll) {
            log.info("since  arrival rate {} is lower than maximum consumption rate " +
                            " with size - 1  I down scaled  by one {}",
                    totalArrivalRate, size * poll);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    lastDownScaleDecision = Instant.now();
                    lastUpScaleDecision = Instant.now();

                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }
    }
}
