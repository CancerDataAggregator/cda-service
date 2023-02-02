package bio.terra.cda.app.service;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.util.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class DataSetInfoClearCache {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryClearCache.class);

    @Autowired
    public TableSchema tableSchema;

    @Scheduled(fixedRate = 10, timeUnit = TimeUnit.MINUTES) // 10 min refresh
    public void task() {
        logger.debug("Scheduler has updated cache");
        tableSchema.clearVersionSchemaCache();
    }
}
