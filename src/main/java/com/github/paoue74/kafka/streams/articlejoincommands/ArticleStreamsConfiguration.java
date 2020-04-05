package com.github.paoue74.kafka.streams.articlejoincommands;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class ArticleStreamsConfiguration {

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kStreamsConfiguration() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "articles-streams");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new KafkaStreamsConfiguration(props);
  }

//  @Primary
//  @Bean
//  public StreamsBuilderFactoryBean articlesKStreamBuilder(KafkaStreamsConfiguration streamsConfig) {
//    return new StreamsBuilderFactoryBean(streamsConfig);
//  }

  @Bean
  public StreamsBuilderFactoryBeanCustomizer customizer() {
    return fb -> fb.setStateListener((newState, oldState) -> {
      log.info("State transition from " + oldState + " to " + newState);
    });
  }

  @Bean
  public KStream<String, EnrichedArticle> kStream(StreamsBuilder articlesKStreamBuilder) {
    KStream<String, Order> orderStream = articlesKStreamBuilder.stream("orders-topic", Consumed.with(Serdes.String(), new JsonSerde<>(Order.class)));
    KTable<String, Article> articlesTable = articlesKStreamBuilder.table("articles-topic", Consumed.with(Serdes.String(), new JsonSerde<>(Article.class)));

    KStream<String, EnrichedArticle> enrichedArticleKStream = orderStream
        .filter((s, order) -> !order.getUserId().isBlank())
        .selectKey((s, order) -> order.getArticleId())
        .leftJoin(articlesTable, this::joinOrderAndArticle)
        .filter((s, enrichedArticle) -> enrichedArticle.getArticleId() != null)
        .groupByKey()
        .aggregate(
            EnrichedArticle::new,
            (s, enrichedArticle, enrichedArticle2) -> buildEnrichedArticle(enrichedArticle, enrichedArticle2),
            Materialized.<String, EnrichedArticle, KeyValueStore<Bytes, byte[]>>as("enriched-articles-store").withKeySerde(Serdes.String()).withValueSerde(new JsonSerde<>(EnrichedArticle.class))
        )
        .toStream();

    enrichedArticleKStream.print(Printed.toSysOut());
    enrichedArticleKStream
        .to("enriched-articles-topic", Produced.with(Serdes.String(), new JsonSerde<>(EnrichedArticle.class)));

    return enrichedArticleKStream;
  }

  private EnrichedArticle buildEnrichedArticle(EnrichedArticle enrichedArticle, EnrichedArticle enrichedArticle2) {
    return new EnrichedArticle()
        .withArticleId(enrichedArticle.getArticleId())
        .withName(enrichedArticle.getName())
        .withDescription(enrichedArticle.getDescription())
        .withContracts(mergeList(enrichedArticle.getContracts(), enrichedArticle2.getContracts()));
  }

  private List<Contract> mergeList(List<Contract> contracts, List<Contract> contracts1) {
    List<Contract> allContracts = new ArrayList<>(contracts);
    allContracts.addAll(contracts1);
    return allContracts;
  }

  private EnrichedArticle joinOrderAndArticle(Order order, Article article) {
    return Optional
        .ofNullable(article)
        .map(art -> new EnrichedArticle()
            .withArticleId(art.getId())
            .withName(art.getName())
            .withDescription(art.getDescription())
            .withContracts(Collections.singletonList(new Contract().withUserId(order.getUserId()).withQuantity(order.getQuantity())))
        )
        .orElseGet(EnrichedArticle::new);
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @With
  private static class Article {
    private String id;
    private String name;
    private String description;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @With
  public static class EnrichedArticle {
    private String articleId;
    private String name;
    private String description;
    private List<Contract> contracts = Collections.emptyList();
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @With
  private static class Order {
    private String id;
    private String userId;
    private String articleId;
    private int quantity;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @With
  private static class Contract {
    private String userId;
    private int quantity;
  }
}
