package bio.terra.cda.app.service;

import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.generators.EntityCountSqlGenerator;
import bio.terra.cda.app.generators.EntitySqlGenerator;
import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.generated.model.SystemStatus;
import bio.terra.cda.generated.model.SystemStatusSystemsValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

@Component
@CacheConfig(cacheNames = "system-status")
public class QueryService {
  @Autowired public ApplicationConfiguration applicationConfiguration;

  private static final Logger logger = LoggerFactory.getLogger(QueryService.class);

  private final ObjectMapper objectMapper;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
  private SqlGenerator generator;

  @Autowired
  public QueryService(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @CacheEvict
  public void clearSystemStatus() {
    logger.debug("Clear SystemStatus");
  }

  SystemStatus systemStatus = new SystemStatus();



  // For now, hardcode the known list of systems. In the future, we will get this
  // from the
  // database itself, as each published dataset will have a list of contributing
  // systems.
  private enum Source {
    GDC,
    PDC
  }

  public SystemStatus postgresCheck() {
    SystemStatusSystemsValue pgSystemStatus = new SystemStatusSystemsValue();
    boolean success = false;
    try {
      Integer activeConnections = jdbcTemplate
          .query("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'", rs -> {
            return rs.next() ? rs.getInt(1) : 0;
          });
      success = activeConnections > 0;
    } catch (Exception e) {
      logger.error("Status check failed ", e);
    }
    if (success) {
      pgSystemStatus.ok(true).addMessagesItem("everything is fine");
    } else {

      pgSystemStatus
          .ok(false)
          .addMessagesItem("Postgres Status check has indicated the database is currently unreachable from the Service API");
    }
    int mb = 1024 * 1024;
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    long xmx = memoryBean.getHeapMemoryUsage().getMax() / mb;
    long xms = memoryBean.getHeapMemoryUsage().getInit() / mb;
    SystemStatusSystemsValue javaMem = new SystemStatusSystemsValue();
    javaMem.addMessagesItem(String.format("XMX: %d, XMS: %d", xmx, xms));
    systemStatus
        .ok(pgSystemStatus.getOk())
        .putSystemsItem("PostgresStatus", pgSystemStatus)
        .putSystemsItem("JavaMemory", javaMem);

    return systemStatus;
  }


  /**
   * Traverse the json data and collect the number of systems data present in resultsCount.
   *
   * @param jsonData the data to scan
   * @return For each system, the number of rows in jsonData that have data from that system
   */
  private Map<Source, Integer> generateUsageData(List<JsonNode> jsonData) {
    Map<Source, Integer> resultsCount = new EnumMap<>(Source.class);
    // Each node is a single row in the result.
    for (JsonNode jsonNode : jsonData) {
      List<String> systems = jsonNode.findValuesAsText("system");
      Arrays.stream(Source.values())
          .forEach(
              s -> {
                // Each row can match more than one system, so we have to count them all.
                if (systems.contains(s.name())) {
                  resultsCount.put(s, resultsCount.getOrDefault(s, 0) + 1);
                }
              });
    }
    return resultsCount;
  }

  public Long getTotalRowCount(SqlGenerator generator) {
    String sqlCount = SqlTemplate.countWrapper(generator.getSqlStringForMaxRows());
    MapSqlParameterSource param_map = generator.getNamedParameterMap();
    if ((generator instanceof EntitySqlGenerator)){
      String optimizedSqlCount = optimizeIncludeCountQuery(sqlCount, (EntitySqlGenerator) generator);
      return namedParameterJdbcTemplate.queryForObject(
              optimizedSqlCount,
              param_map,
              Long.class);
    }
    else{
      return namedParameterJdbcTemplate.queryForObject(
              sqlCount,
              param_map,
              Long.class);
    }
  }
  public Long getTotalRowCountOG(SqlGenerator generator) {
      return namedParameterJdbcTemplate.queryForObject(
              SqlTemplate.countWrapper(generator.getSqlStringForMaxRows()),
              generator.getNamedParameterMap(),
              Long.class);
  }


  public String optimizeIncludeCountQuery(String sqlCount, EntitySqlGenerator generator){
    try {
      Filter filterObj = new Filter(sqlCount, generator);
      return filterObj.getIncludeCountQuery();
    }catch (Exception exception) {
      logger.warn(String.format("Sql: %s, Exception: %s",sqlCount,exception.getMessage()));
      return sqlCount;
    }
  }



  public List<JsonNode> generateAndRunQuery(SqlGenerator generator) {
    String sqlQuery = SqlTemplate.jsonWrapper(generator.getSqlString());
    MapSqlParameterSource param_map = generator.getNamedParameterMap();
    if ((generator instanceof EntityCountSqlGenerator)){
      String optimizedSqlCount = optimizeCountEndpointQuery(sqlQuery, (EntityCountSqlGenerator) generator);
      return namedParameterJdbcTemplate.query(
              optimizedSqlCount,
              param_map,
              new JsonNodeRowMapper(objectMapper));
    }
    else{
      return namedParameterJdbcTemplate.query(
              sqlQuery,
              param_map,
              new JsonNodeRowMapper(objectMapper));
    }
  }
  public String getReadableOptimizedCountQuery(SqlGenerator generator) {
    String sqlQuery = SqlTemplate.jsonWrapper(generator.getSqlString());
    String optimizedQuery = "";
    if (generator instanceof EntityCountSqlGenerator){
      optimizedQuery = optimizeCountEndpointQuery(sqlQuery, (EntityCountSqlGenerator) generator);
    } else {
      optimizedQuery = sqlQuery;
    }
    return generator.getReadableQuerySqlArg(optimizedQuery);
  }

  public String optimizeCountEndpointQuery(String sqlCount, EntityCountSqlGenerator generator){
    try {
      Filter filterObj = new Filter(sqlCount, generator);
      return filterObj.getCountEndpointQuery();
    } catch (Exception exception){
      logger.warn(String.format("Sql: %s, Exception: %s",sqlCount,exception.getMessage()));
      return sqlCount;
    }
  }

  public List<JsonNode> generateAndRunPagedQuery(SqlGenerator generator, Integer offset, Integer limit) {
    String sqlQuery = SqlTemplate.jsonWrapper(SqlTemplate.addPagingFields(generator.getSqlString(), offset, limit));
    MapSqlParameterSource param_map =  generator.getNamedParameterMap();
    String optimizedPagedQuery = "";
    if (generator instanceof EntitySqlGenerator){
      optimizedPagedQuery = optimizePagedQuery(sqlQuery, (EntitySqlGenerator) generator);
    } else {
      optimizedPagedQuery = sqlQuery;
    }
    return namedParameterJdbcTemplate.query(
        optimizedPagedQuery,
        param_map,
        new JsonNodeRowMapper(objectMapper)
    );
  }
  public String optimizePagedQuery(String sqlQuery, EntitySqlGenerator generator){
    try {
      Filter filterObj = new Filter(sqlQuery, generator);
      return filterObj.getPagedPreselectQuery();
//      return sqlQuery;
    }catch (Exception exception) {
      logger.warn(String.format("Sql: %s, Exception: %s",sqlQuery,exception.getMessage()));
      return sqlQuery;
    }
  }

  public String getReadableOptimizedPagedQuery(SqlGenerator generator, Integer offset, Integer limit) {
    String sqlQuery = SqlTemplate.jsonWrapper(SqlTemplate.addPagingFields(generator.getSqlString(), offset, limit));
    String optimizedPagedQuery = "";
    if (generator instanceof EntitySqlGenerator){
      optimizedPagedQuery = optimizePagedQuery(sqlQuery, (EntitySqlGenerator) generator);
    } else {
      optimizedPagedQuery = sqlQuery;
    }
    return generator.getReadableQuerySqlArg(optimizedPagedQuery);
  }

  public List<JsonNode> runPagedQuery(String sqlStr, Integer offset, Integer limit) {
    return this.runQuery(SqlTemplate.addPagingFields(sqlStr, offset, limit));
  }

  public List<JsonNode> runQuery(String sqlQuery) {
    return jdbcTemplate.query(SqlTemplate.jsonWrapper(sqlQuery), new JsonNodeRowMapper(objectMapper));
  }
  public void logQuery(long duration, String sql, List<JsonNode> jsonData, Optional<Float> countDuration) {
    // Log usage data for this response.
    final Map<Source, Integer> resultsCount = generateUsageData(jsonData);
    float elapsed = duration / 1000.0F;
    var logData = new QueryData(sql, elapsed, resultsCount, countDuration);
    try {
      logger.info(objectMapper.writeValueAsString(logData));
    } catch (JsonProcessingException e) {
      logger.warn("Error converting object to JSON", e);
    }
  }

  private static class QueryData {
    public final String timestamp;
    public final String query;
    public final float duration;
    public final Map<Source, Integer> systemUsage;

    public final Optional<Float> optionalQueryDuration;

    QueryData(String query, float duration, Map<Source, Integer> systemUsage,Optional<Float> optionalQueryDuration) {
      this.timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());
      this.query = query;
      this.duration = duration;
      this.systemUsage = systemUsage;
      this.optionalQueryDuration = optionalQueryDuration;
    }
  }

}

