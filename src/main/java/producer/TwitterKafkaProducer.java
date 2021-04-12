package producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterKafkaProducer {
  private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  private List<String> topics;

  public TwitterKafkaProducer() {
    this.topics = new ArrayList<>();
  }

  public void addTopic(String topic) {
    topics.add(topic);
  }

  public void clearAllTopics() {
    topics.clear();
  }

  public static void main(String[] args) {
    TwitterKafkaProducer twitterKafkaProducer = new TwitterKafkaProducer();
    twitterKafkaProducer.addTopic("angular");
    twitterKafkaProducer.run();
  }

  public void run() {
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>();
    Client client = createTwitterClient(msgQueue);
    KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
    client.connect();

    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        kafkaProducer.send(new ProducerRecord<>("tweets", null, msg));
        System.out.println(msg);
      }
    }
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    Properties properties =  new Properties();
    // Basic producer properties set-up
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Make producer idempotent (i.e. able to remove duplicates due to retries
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    // Require partition leader and replica's acknowledgement to minimize data loss
    // It adds a bit of latency to the producer due to waiting for acknowledgement
    // This needs correspond min.insync.replica argument on topic / broker leve
    // min.insync.replicas needed is 2 (leader + 1 replica)
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

    // Increase throughput of the producer via batching and compression
    // Trade off CPU against disk and network usage
    // Analysis of different compression algorithm: https://blog.cloudflare.com/squeezing-the-firehose/
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "50");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    return producer;
  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue) {
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    List<String> terms = Lists.newArrayList(topics);
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(Utils.API_KEY, Utils.API_SECRET_KEY,
            Utils.ACCESS_TOKEN, Utils.ACCESS_TOKEN_SECRET);

    ClientBuilder builder = new ClientBuilder()
            .name("Hosebird-Client-01")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }

}
