package com.github.paoue74.kafka.streams.articlejoincommands;

import com.github.paoue74.kafka.streams.articlejoincommands.ArticleStreamsConfiguration.EnrichedArticle;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping(value = "/articles", produces = MediaType.APPLICATION_JSON_VALUE)
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class EnrichedArticleController {

  private final EnrichedArticleService enrichedArticleService;

  @GetMapping("/{id}")
  public EnrichedArticle get(@PathVariable String id) {
    return enrichedArticleService.get(id);
  }
}
