import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

  public static void main(String[] args) throws IOException {
    ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer();
    elasticSearchConsumer.run();
  }

  private KafkaConsumer<String, String> createKafkaConsumer(String groupId, String offsetOption, List<String> topics) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Utils.BOOTSTRAP_SERVER);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetOption);
    // Due to async nature of pur operation, if auto commit is true, then we would be under at most once behavior
    // which is not ideal for safety
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    // Batch request read from kafka
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(50));

    // Create consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
    kafkaConsumer.subscribe(topics);
    return kafkaConsumer;
  }

  private RestHighLevelClient createElasticSearchClient() {
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(Utils.USERNAME, Utils.PASSWORD));

    RestClientBuilder builder = RestClient.builder(
            new HttpHost(Utils.HOSTNAME, 443, "https"))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            });
    return new RestHighLevelClient(builder);
  }

  public void run() {
    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    RestHighLevelClient elasticSearchClient = createElasticSearchClient();
    KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer("twitter-consumer", "earliest",
            Arrays.asList("tweets"));

    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(200));
      int recordCount = records.count();

      BulkRequest bulkRequest = new BulkRequest();

      for (ConsumerRecord<String, String> record : records) {
        String id = record.topic() + "_" + record.partition() + "_" + record.offset();
        try {
          IndexRequest indexRequest = new IndexRequest("tweets")
                  .source(record.value(), XContentType.JSON)
                  .id(id);
          bulkRequest.add(indexRequest);
        } catch(Exception e) {
          logger.warn(e.getMessage());
        }
      }
      if (recordCount > 0) {
        try {
          BulkResponse bulkItemResponses = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
          kafkaConsumer.commitSync();

        } catch (IOException e) {
          logger.error(e.getMessage());
        }
      }
      kafkaConsumer.close();
      try {
        elasticSearchClient.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
