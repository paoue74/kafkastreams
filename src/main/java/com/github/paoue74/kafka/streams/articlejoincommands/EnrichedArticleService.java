package com.github.paoue74.kafka.streams.articlejoincommands;

import com.github.paoue74.kafka.streams.articlejoincommands.ArticleStreamsConfiguration.EnrichedArticle;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class EnrichedArticleService {

  private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

  ReadOnlyKeyValueStore<String, EnrichedArticle> getStore() {
    KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
    log.info("Topology {}", streamsBuilderFactoryBean.getTopology().describe());
    log.info("Streams Config {}", streamsBuilderFactoryBean.getStreamsConfiguration());
    return kafkaStreams.store("enriched-articles-store", QueryableStoreTypes.keyValueStore());
  }

  public EnrichedArticle get(String id) {
    log.info("Getting article {} enriched data", id);
    return getStore().get(id);
  }
}