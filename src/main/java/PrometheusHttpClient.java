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


    public static void main(String[] args) throws InterruptedException, ExecutionException, URISyntaxException {
        readEnvAndCrateAdminClient();
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();

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
            youMightWanttoScale(totalarrivals);

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
