package bio.terra.cda.app.util;

import bio.terra.cda.app.models.CountByField;
import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.ForeignKey;
import bio.terra.cda.app.models.SchemaDefinition;
import bio.terra.cda.app.models.TableDefinition;
import bio.terra.cda.app.service.StorageService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.LegacySQLTypeName;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Component
@CacheConfig(cacheNames = "schemas")
public class TableSchema {
  private final StorageService storageService;
  private final CacheManager cacheManager;

    private static final Logger logger = LoggerFactory.getLogger(TableSchema.class);

  public TableSchema(StorageService storageService, CacheManager cacheManager) {
    this.cacheManager = cacheManager;
    this.storageService = storageService;
  }

  @CacheEvict(cacheNames = "schemas")
  public void clearVersionSchemaCache() {
      logger.debug("Scheduler has updated schema cache");
  }

  @Cacheable(cacheNames = "schemas")
  public DataSetInfo getDataSetInfo(String version) throws IOException {
      return DataSetInfo.of(version, this.storageService);
  }

  public Map<String, SchemaDefinition> buildSchemaMap(TableDefinition tableDefinition) {
    Map<String, SchemaDefinition> definitionMap = new HashMap<>();
    addToMap("", Arrays.asList(tableDefinition.getDefinitions()), definitionMap);
    return definitionMap;
  }

  private void addToMap(
      String prefix,
      List<SchemaDefinition> definitions,
      Map<String, SchemaDefinition> definitionMap) {
    definitions.forEach(
        definition -> {
          var mapName =
              prefix.isEmpty()
                  ? definition.getName()
                  : String.format(SqlUtil.ALIAS_FIELD_FORMAT, prefix, definition.getName());
          definitionMap.put(mapName, definition);
          if (definition.getType().equals(LegacySQLTypeName.RECORD.toString())) {
            addToMap(mapName, List.of(definition.getFields()), definitionMap);
          }
        });
  }
  // endregion
}
