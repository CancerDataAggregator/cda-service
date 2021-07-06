package bio.terra.cda.app.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class BigQueryClearCache {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryClearCache.class);

  @Autowired QueryService queryService;

  @Scheduled(fixedRate = 120000) // 2 min refresh
  public void task() {
    logger.debug("Scheduler has updated cache");
    queryService.clearSystemStatus();
  }
}
