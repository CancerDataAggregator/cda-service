package bio.terra.cda.app.service;

import bio.terra.cda.app.builders.JoinBuilder;
import bio.terra.cda.app.configuration.ApplicationConfiguration;
import bio.terra.cda.app.generators.EntityCountSqlGenerator;
import bio.terra.cda.app.generators.EntitySqlGenerator;
import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.models.ForeignKey;
import bio.terra.cda.app.models.Join;
import bio.terra.cda.app.util.QueryContext;
import bio.terra.cda.app.util.SqlTemplate;
import bio.terra.cda.generated.model.SystemStatus;
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
import org.springframework.jdbc.core.metadata.SqlServerCallMetaDataProvider;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

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
    Filter filterObj = new Filter(sqlCount, sqlCount, generator, Boolean.TRUE, "");
    if (filterObj.getProblemFlag()){
      return sqlCount;
    } else {
      return filterObj.getIncludeCountQuery();
    }
  }



  public List<JsonNode> generateAndRunQuery(SqlGenerator generator) {
    String sqlQuery = SqlTemplate.jsonWrapper(generator.getSqlString());
    MapSqlParameterSource param_map = generator.getNamedParameterMap();
    if ((generator instanceof EntityCountSqlGenerator)){
      String optimizedSqlCount = optimizeCountQuery(sqlQuery, (EntityCountSqlGenerator) generator);
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

  public String optimizeCountQuery(String sqlCount, EntitySqlGenerator generator){
    Filter filterObj = new Filter(sqlCount, sqlCount, generator, Boolean.TRUE, "");
    if (filterObj.getProblemFlag()){
      return sqlCount;
    } else {
      return sqlCount;
    }
  }

  public List<JsonNode> generateAndRunPagedQuery(SqlGenerator generator, Integer offset, Integer limit) {
    return namedParameterJdbcTemplate.query(
        SqlTemplate.jsonWrapper(
            SqlTemplate.addPagingFields(generator.getSqlString(), offset, limit)),
        generator.getNamedParameterMap(),
        new JsonNodeRowMapper(objectMapper)
    );
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

// Class to construct optimized count preselect SQL statement from the filters in the original count(*) wrapped query
final class Filter{
  final Boolean isRoot;
  private String orginalQuery = "";
  private String filterQuery = "";
  private String filterTableName = "";
  private String operator = "";
  private Filter leftFilter = null;
  private Filter rightFilter = null;
  private Boolean problemFlag = Boolean.FALSE;
  private String filterPreselect = "";
  final private EntitySqlGenerator generator;
  final private JoinBuilder joinBuilder;
  final private String entityTableName;
  final private String entityPK;
  private String mappingTableName = "";
  private String filterTableKey = "";
  private String mappingEntityKey = "";
  private String mappingFilterKey = "";
  private String mappingPreselectName = "";
  private String mappingTablePreselect = "";
  private String filterPreselectName = "";
  private String joinString = "";
  private String includeCountQuery = "";
  private String unionIntersect = "";
  final String id;

  public Filter(String originalQuery, String baseFilterString, EntitySqlGenerator generator, Boolean isRoot, String id){
    this.isRoot = isRoot;
    this.id = id;
    this.orginalQuery = originalQuery;
    if (baseFilterString.contains("WHERE")){
      extractWhereFilter();
    } else {
      this.filterQuery = baseFilterString.trim();
    }
    this.problemFlag = Boolean.FALSE;
    this.generator = generator;
    this.joinBuilder = this.generator.getJoinBuilder();
    this.entityTableName = generator.getEntityTableName();
    this.entityPK = generator.getEntityTableFirstPK();
    constructFilter();
    setVariablesFromChildren();
    setIncludeCountQuery();
  }

  public void extractWhereFilter(){
    String startingFilterString = this.orginalQuery.substring(this.orginalQuery.indexOf("WHERE")+5).trim();
    int openParenthesisCount = 1;
    int indexCursor = 0;
    StringBuilder whereFilter = new StringBuilder();
    whereFilter.append('(');
    try {
      while (openParenthesisCount > 0) {
        indexCursor += 1;
        if (startingFilterString.charAt(indexCursor) == '(') {
          openParenthesisCount += 1;
        } else if (startingFilterString.charAt(indexCursor) == ')') {
          openParenthesisCount -= 1;
        }
        whereFilter.append(startingFilterString.charAt(indexCursor));
      }
     this.filterQuery = whereFilter.toString();
    } catch (Exception e){
        this.problemFlag = Boolean.TRUE;
      }
  }

  public void constructFilter() {
    if (this.filterQuery.startsWith("((") & this.filterQuery.endsWith("))"))
        this.filterQuery = this.filterQuery.substring(1, this.filterQuery.length() - 1);

    if (!(this.filterQuery.contains("AND") | this.filterQuery.contains("OR"))) {
      // Get filter table name
      int tableStartIndex;
      if (this.filterQuery.startsWith("(COALESCE(UPPER(")){
        tableStartIndex = this.filterQuery.indexOf("COALESCE(UPPER(") + 15;
      } else {
        tableStartIndex = 1;
      }
      int tableEndIndex = this.filterQuery.indexOf(".");
      if (tableEndIndex <= 0) this.problemFlag = Boolean.TRUE; // TODO: what if no "."
      this.filterTableName = this.filterQuery.substring(tableStartIndex, tableEndIndex);

      // Remove filter table name from filter query
      this.filterQuery = this.filterQuery.replace(this.filterTableName +".", "");

      // Use JoinPath to generate preselects
      List<Join> joinPath = this.joinBuilder.getPath(this.filterTableName, this.entityTableName, this.entityPK); // TODO: could optimize by building a better joinPath with this one

      // Construct initial preselect from the filter table
      if (joinPath.isEmpty()){
        this.filterTableKey = this.generator.getEntityTable().getForeignKeys().first().getFromField();
      } else {
        this.filterTableKey = joinPath.get(0).getKey().getFromField();
      }
      this.filterPreselectName = replaceKeywords("FILTERTABLENAME_id_preselectIDENTIFIER");
      String preselect_template = "FILTERPRESELECTNAME AS (SELECT FILTERTABLEKEY FROM FILTERTABLENAME WHERE FILTERQUERY)";
      this.filterPreselect = replaceKeywords(preselect_template);

      // Construct Mapping Preselects
      if (joinPath.isEmpty()) {
        this.mappingEntityKey = this.generator.getEntityTable().getForeignKeys().first().getFields()[0];
        this.mappingTableName = this.generator.getEntityTable().getForeignKeys().first().getDestinationTableName();
        this.mappingFilterKey = this.generator.getEntityTable().getForeignKeys().first().getFromField();
        this.mappingPreselectName = replaceKeywords("FILTERTABLENAME_mapping_id_preselectIDENTIFIER");
        String mapping_preselect_template = "MAPPINGPRESELECTNAME AS( SELECT MAPPINGENTITYKEY FROM MAPPINGTABLENAME WHERE MAPPINGFILTERKEY IN( SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
        this.mappingTablePreselect = replaceKeywords(mapping_preselect_template);
      }
      if (joinPath.size() == 2) { // Direct mapping table present -> construct basic mapping preselect
        this.mappingTableName = joinPath.get(0).getKey().getDestinationTableName();
        this.mappingEntityKey = joinPath.get(1).getKey().getFromField();
        this.mappingFilterKey = joinPath.get(0).getKey().getFields()[0];
        this.mappingPreselectName = replaceKeywords("MAPPINGTABLENAME_id_preselectIDENTIFIER");
        String mapping_preselect_template = "MAPPINGPRESELECTNAME AS( SELECT MAPPINGENTITYKEY FROM MAPPINGTABLENAME WHERE MAPPINGFILTERKEY IN( SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
        this.mappingTablePreselect = replaceKeywords(mapping_preselect_template);
      } else if (joinPath.size() > 2) { // Need to apply joins to a mapping table
        this.setJoinString(joinPath);
        this.mappingTableName = joinPath.get(joinPath.size() - 1).getKey().getDestinationTableName();
        this.mappingEntityKey = joinPath.get(joinPath.size() - 1).getKey().getFromField();
        this.mappingFilterKey = joinPath.get(0).getKey().getFields()[0];
        this.mappingPreselectName = replaceKeywords("MAPPINGTABLENAME_FILTERTABLENAME_id_preselectIDENTIFIER");
        String mapping_preselect_template = "MAPPINGPRESELECTNAME AS( SELECT MAPPINGENTITYKEY FROM FILTERTABLENAME AS FILTERTABLENAME JOINSTRING WHERE MAPPINGFILTERKEY IN( SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
        this.mappingTablePreselect = replaceKeywords(mapping_preselect_template);
      }
      // Construct SELECT Statement for UNION/INTESECT opertations
      String union_intersect_template = "SELECT MAPPINGENTITYKEY FROM MAPPINGPRESELECTNAME";
      this.unionIntersect = replaceKeywords(union_intersect_template);

      this.operator = "";
      this.leftFilter = null;
      this.rightFilter = null;
    }else { // Construct Nested left and right filters
      this.filterTableName = "";
      buildLeftRightFilters();
    }

  }
  public void buildLeftRightFilters(){
    // Parse out left filter
    int openParenthesisCount = 1;
    int indexCursor = 0;
    StringBuilder leftFilterString = new StringBuilder();
    leftFilterString.append('(');
    try {
      while (openParenthesisCount > 0){
        indexCursor +=1;
        if (this.filterQuery.charAt(indexCursor) == '('){
          openParenthesisCount +=1;
        } else if(this.filterQuery.charAt(indexCursor) == ')') {
          openParenthesisCount -= 1;
        }
        leftFilterString.append(this.filterQuery.charAt(indexCursor));
      }

      String remainingString = this.filterQuery.substring(indexCursor + 1);
      // Determine what operator (INTERSECT/UNION) to use between left and right filters
      if (remainingString.startsWith(" AND ")){
        this.operator = " INTERSECT ";
        remainingString = remainingString.replaceFirst(" AND ","");
      } else if (remainingString.startsWith(" OR ")) {
        this.operator = " UNION ";
        remainingString = remainingString.replaceFirst(" OR ","");;
      } else {
        this.operator = "";
        this.problemFlag = Boolean.TRUE;
      }
      // Construct nested Filter objects for left and right filters (adding '_0' to ids for left and '_1' to ids for right filters)
      this.leftFilter = new Filter(this.orginalQuery, leftFilterString.toString(), this.generator, Boolean.FALSE, this.id + "_0");
      this.rightFilter = new Filter(this.orginalQuery, remainingString, this.generator, Boolean.FALSE, this.id + "_1");
    }catch (Exception e){
      this.problemFlag = Boolean.TRUE;
    }

  }
  public void setVariablesFromChildren(){ // Concatenate nested filter values
    if (this.leftFilter != null & this.rightFilter != null){ // Check to see that we have left and right child Filters
      this.problemFlag = (this.problemFlag | this.leftFilter.getProblemFlag() | this.rightFilter.getProblemFlag()); // If a problem occurred in any child, set problem flag to True
      this.mappingTablePreselect = this.leftFilter.getMappingPreselect() + ", " + rightFilter.getMappingPreselect();
      this.filterPreselect = this.leftFilter.getFilterPreselect() + ", " + rightFilter.getFilterPreselect();
      this.unionIntersect = "(" + this.leftFilter.getUnionIntersect() + " " + this.operator + " " + this.rightFilter.getUnionIntersect() + ")";

      // Get mapping entity key for final "SELECT COUNT(DISTINCT(KEY))" statement
      this.mappingEntityKey = this.leftFilter.getMappingEntityKey();
      if(!this.leftFilter.getMappingEntityKey().equals(this.rightFilter.getMappingEntityKey())){
        this.problemFlag = Boolean.TRUE;
      }
    }
  }
  public void setIncludeCountQuery(){
    if (this.isRoot & this.leftFilter == null & this.rightFilter == null){
      // Don't need to add mapping table preselect statements and union/intersect statements if the query isn't nested
      String count_template = "WITH FULLFILTERPRESELECT SELECT COUNT(DISTINCT(MAPPINGENTITYKEY)) FROM MAPPINGTABLENAME WHERE MAPPINGFILTERKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME);";
      this.includeCountQuery = replaceKeywords(count_template);
    } else if (this.isRoot) {
      String count_template = "WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT SELECT COUNT(DISTINCT(MAPPINGENTITYKEY)) FROM UNIONINTERSECT as count_result";
      this.includeCountQuery = replaceKeywords(count_template);
    }
  }
  public String replaceKeywords(String template){ // Helper function for replacing constructed string variables with supplied template
    return template
            .replace("IDENTIFIER", this.id)
            .replace("FILTERTABLENAME", this.filterTableName)
            .replace("FILTERTABLEKEY", this.filterTableKey)
            .replace("FILTERQUERY", this.filterQuery)
            .replace("FILTERPRESELECTNAME", this.filterPreselectName)
            .replace("FULLFILTERPRESELECT", this.filterPreselect)
            .replace("JOINSTRING", this.joinString)
            .replace("MAPPINGTABLENAME", this.mappingTableName)
            .replace("MAPPINGFILTERKEY", this.mappingFilterKey)
            .replace("MAPPINGENTITYKEY", this.mappingEntityKey)
            .replace("MAPPINGPRESELECTNAME", this.mappingPreselectName)
            .replace("FULLMAPPINGPRESELECT", this.mappingTablePreselect)
            .replace("UNIONINTERSECT", this.unionIntersect);
  }
  public void setJoinString(List<Join> joinPath){ // Builds out join statements from JoinPath
    StringBuilder fullJoinString = new StringBuilder();
    for (Join join : joinPath) {
      if (join != joinPath.get(joinPath.size() - 1)) { // Don't need final path since it will always be entity table since we have a mapping table before it
        String join_template = " INNER JOIN DESTINATIONTABLENAME AS DESTINATIONTABLENAME ON FROMTABLENAME.FROMFIELD = DESTINATIONTABLENAME.DESTINATIONFIELD";
        String fromTableName = join.getKey().getFromTableName();
        String fromField = join.getKey().getFromField();
        String destinationTableName = join.getKey().getDestinationTableName();
        String destinationField = join.getKey().getFields()[0];
        fullJoinString.append(join_template
                .replace("DESTINATIONTABLENAME", destinationTableName)
                .replace("DESTINATIONFIELD", destinationField)
                .replace("FROMTABLENAME", fromTableName)
                .replace("FROMFIELD", fromField));
      }
    }
    this.joinString = fullJoinString.toString();
  }
  public String getMappingPreselect(){
    return this.mappingTablePreselect;
  }
  public String getFilterPreselect(){
    return this.filterPreselect;
  }

  public Boolean getProblemFlag() {
    return this.problemFlag;
  }
  public String getUnionIntersect(){
    return this.unionIntersect;
  }
  public String getIncludeCountQuery(){
    return this.includeCountQuery;
  }

  public String getMappingEntityKey(){
    return this.mappingEntityKey;
  }
}