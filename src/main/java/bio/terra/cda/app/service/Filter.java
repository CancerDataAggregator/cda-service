package bio.terra.cda.app.service;

import bio.terra.cda.app.models.*;
import bio.terra.cda.app.builders.JoinBuilder;
import bio.terra.cda.app.generators.EntityCountSqlGenerator;
import bio.terra.cda.app.generators.EntitySqlGenerator;
import bio.terra.cda.generated.model.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// Class to construct optimized count preselect SQL statement from the filters in the original count(*) wrapped query
public class Filter {
  protected Boolean isRoot;
  private String originalQuery = "";
  private String filterQuery = "";
  private String filterTableName = "";
  private String operator = "";
  private Filter leftFilter = null;
  private Filter rightFilter = null;
  private String filterPreselect = "";
  private EntitySqlGenerator generator;
  private DataSetInfo dataSetInfo;
  private EntityCountSqlGenerator countGenerator = null;
  private JoinBuilder joinBuilder;
  private String entityTableName;
  private String entityPK;
  private String mappingTableName = "";
  private String filterTableKey = "";
  private String mappingFilterKey = "";
  private String mappingPreselectName = "";
  private String mappingTablePreselect = "";
  private String filterPreselectName = "";
  private String joinString = "";
  private String mappingFileTableName = "";
  private String mappingFileEntityKey = "";
  private String mappingFileMappingKey = "";
  private String commonAlias = "";
  private String entityTableCountPreselect = "";
  private String countPreselect = "";
  private String countSelect = "";
  private String unionIntersect = "";
  protected String id;
  private String originalReplaceFilterQuery = "";
  private String pagedReplacementFilter = "";
  private String includeCountQuery = "";
  private String countEndpointQuery = "";
  private String pagedPreselectQuery = "";


  /***
   * Class to construct optimized count preselect SQL statement from the filters
   * in the original count(*) wrapped query
   * 
   * @throws RuntimeException If there is problem create the filters
   * @param baseFilterString Originally passed in as generated sql but later
   * @param generator
   *
   */
  public Filter(String baseFilterString, EntitySqlGenerator generator) {
    this.isRoot = Boolean.TRUE;
    this.id = "";
    this.originalQuery = baseFilterString;

    String WHERE = Query.NodeTypeEnum.WHERE.getValue();
    if (!this.originalQuery.contains(WHERE)) {
      throw new RuntimeException("This query does not contain a where filter");
    }
    String startingFilterString = this.originalQuery.substring(this.originalQuery.indexOf(WHERE) + WHERE.length()).trim();
    this.filterQuery = FilterUtils.parenthesisSubString(startingFilterString);
    this.originalReplaceFilterQuery = this.originalQuery.replace(this.filterQuery, "(PAGEDREPLACEMENTFILTER)");
    buildFilter(generator);
  }
  protected Filter(String baseFilterString, EntitySqlGenerator generator, String id) {
    this.isRoot = Boolean.FALSE;
    this.id = id;
    this.filterQuery = baseFilterString.trim();
    buildFilter(generator);
  }

  public void buildFilter(EntitySqlGenerator generator){
    this.generator = generator;
    this.dataSetInfo = this.generator.getDataSetInfo();
    this.joinBuilder = this.generator.getJoinBuilder();
    this.entityTableName = generator.getEntityTableName();


    if (this.entityTableName.equals("somatic_mutation")) {
      this.entityPK = "subject_alias";
      this.commonAlias = "subject_alias";
    } else {
      this.entityPK = generator.getEntityTableFirstPK();
      this.commonAlias = String.format("%s_alias", this.entityTableName);
    }
    if (this.entityPK.trim().isEmpty()) {
      throw new RuntimeException("The entity table " + this.entityTableName + " does not contain a primary key or relationship key.");
    }


    constructFilter();
    setVariablesFromChildren();
    if (this.generator instanceof EntityCountSqlGenerator) {
      this.countGenerator = (EntityCountSqlGenerator) this.generator;
      setCountEndpointQuery();
    } else {
      setIncludeCountQuery();
      setPagedPreselectQuery();
    }
  }

  public void constructFilter() {

    String AND = Query.NodeTypeEnum.AND.getValue();
    String OR = Query.NodeTypeEnum.OR.getValue();
    if (!(this.filterQuery.contains(AND) || this.filterQuery.contains(OR))) {
      // Get filter table name
      int tableStartIndex;
      //Sample coalesce statement...
      //COALESCE(UPPER(subject.sex)) <- want to extract "subject" here as the filter table
      if (this.filterQuery.startsWith("(COALESCE(UPPER(") || this.filterQuery.startsWith("COALESCE(UPPER(")) {
        String search = "COALESCE(UPPER(";
        tableStartIndex = this.filterQuery.indexOf(search) + search.length();
      } else {
        tableStartIndex = 1;
      }
      int tableEndIndex = this.filterQuery.indexOf(".");
      if (tableEndIndex <= 0) {
        throw new RuntimeException("tableEndIndex <= 0"); // TODO: what if no "."
      }
      this.filterTableName = this.filterQuery.substring(tableStartIndex, tableEndIndex);
      // Remove filter table name from filter query
      this.filterQuery = this.filterQuery.replace(this.filterTableName +".", "");

      // Use JoinPath to generate preselects
      List<Join> joinPath = this.joinBuilder.getPath(this.filterTableName, this.entityTableName, this.entityPK);


      if (joinPath.size() <= 1){ // Filter on the entity table
        if (this.filterTableName.equals("somatic_mutation")) {
          this.filterTableKey = "subject_alias";
        } else if (this.filterTableName.endsWith("_data_source")) {
          this.filterTableKey = String.format("%s_alias", this.filterTableName.replace("_data_source", ""));
        } else if (this.filterTableName.endsWith("_associated_project")){
          this.filterTableKey = String.format("%s_alias", this.filterTableName.replace("_associated_project", ""));
        }else {
          this.filterTableKey = "integer_id_alias";
        }

        this.filterPreselectName = replaceKeywords("FILTERTABLENAME_id_preselectIDENTIFIER");
        String preselect_template = "FILTERPRESELECTNAME AS (SELECT FILTERTABLEKEY FROM FILTERTABLENAME WHERE FILTERQUERY)";
        this.filterPreselect = replaceKeywords(preselect_template);

        // Construct SELECT Statement for UNION/INTERSECT operations
        String union_intersect_template = "SELECT FILTERTABLEKEY AS COMMONALIAS FROM FILTERPRESELECTNAME";
        this.unionIntersect = replaceKeywords(union_intersect_template);

      } else { // Filter needs to be mapped from filter table to entity table
        this.filterTableKey = joinPath.get(0).getKey().getFromField();
        this.filterPreselectName = replaceKeywords("FILTERTABLENAME_id_preselectIDENTIFIER");
        String preselect_template = "FILTERPRESELECTNAME AS (SELECT FILTERTABLEKEY FROM FILTERTABLENAME WHERE FILTERQUERY)";
        this.filterPreselect = replaceKeywords(preselect_template);
        if (this.filterTableName.endsWith("_data_source") || this.filterTableName.endsWith("_associated_project")){
          this.mappingFilterKey = joinPath.get(0).getKey().getFromField();
        } else {
          this.mappingFilterKey = joinPath.get(0).getKey().getFields()[0];
        }

        // Construct Mapping Preselects
        if (joinPath.size() == 2) { // Direct mapping table present -> construct basic mapping preselect
          this.mappingTableName = joinPath.get(0).getKey().getDestinationTableName();
          List<String> mappingTableColumnNames = Arrays.stream(this.dataSetInfo
                          .getTableInfo(this.mappingTableName)
                          .getColumnDefinitions())
                  .sequential().map(ColumnDefinition::getName).collect(Collectors.toList());
          if (!mappingTableColumnNames.contains(commonAlias)){
            throw new RuntimeException(String.format("Common alias '%s' not found in joinPath from %s table", this.commonAlias, this.filterTableName));
          }
          this.mappingPreselectName = replaceKeywords("MAPPINGTABLENAME_id_preselectIDENTIFIER");
          String mapping_preselect_template = "MAPPINGPRESELECTNAME AS (SELECT COMMONALIAS FROM MAPPINGTABLENAME WHERE MAPPINGFILTERKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
          this.mappingTablePreselect = replaceKeywords(mapping_preselect_template);
        } else if (joinPath.size() > 2) { // Need to apply joins to a mapping table
          this.setJoinString(joinPath);
          this.mappingTableName = joinPath.get(joinPath.size() - 1).getKey().getDestinationTableName();
          this.mappingPreselectName = replaceKeywords("MAPPINGTABLENAME_FILTERTABLENAME_id_preselectIDENTIFIER");
          String mapping_preselect_template = "";
          if (this.filterTableName.equals("somatic_mutation")){
            mapping_preselect_template = "MAPPINGPRESELECTNAME AS (SELECT COMMONALIAS FROM FILTERTABLENAME AS FILTERTABLENAME JOINSTRING WHERE subject.MAPPINGFILTERKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
          } else if (this.filterTableName.endsWith("_data_source") || this.filterTableName.endsWith("_associated_project")){
            mapping_preselect_template = "MAPPINGPRESELECTNAME AS (SELECT COMMONALIAS FROM FILTERTABLENAME AS FILTERTABLENAME JOINSTRING WHERE FILTERTABLENAME.MAPPINGFILTERKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
          } else {
            mapping_preselect_template = "MAPPINGPRESELECTNAME AS (SELECT COMMONALIAS FROM FILTERTABLENAME AS FILTERTABLENAME JOINSTRING WHERE MAPPINGFILTERKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
          }
          this.mappingTablePreselect = replaceKeywords(mapping_preselect_template);
        }
        // Construct SELECT Statement for UNION/INTESECT opertations
        String union_intersect_template = "SELECT COMMONALIAS  FROM MAPPINGPRESELECTNAME";
        this.unionIntersect = replaceKeywords(union_intersect_template);
      }

      this.operator = "";
      this.leftFilter = null;
      this.rightFilter = null;
    } else { // Construct Nested left and right filters
      this.filterQuery = FilterUtils.trimExtraneousParentheses(this.filterQuery);
      this.filterTableName = "";
      buildLeftRightFilters();
    }

  }
  public void buildLeftRightFilters(){
    String leftFilterString = FilterUtils.parenthesisSubString(this.filterQuery);

    String remainingString = this.filterQuery.substring(leftFilterString.length());
    // Determine what operator (INTERSECT/UNION) to use between left and right filters
    String SPACED_AND = " " + Query.NodeTypeEnum.AND.getValue() + " ";
    String SPACED_OR = " " + Query.NodeTypeEnum.OR.getValue() + " ";

    if (remainingString.startsWith(SPACED_AND)){
      this.operator = " INTERSECT ";
      remainingString = remainingString.replaceFirst(SPACED_AND,"");
    } else if (remainingString.startsWith(SPACED_OR)) {
      this.operator = " UNION ";
      remainingString = remainingString.replaceFirst(SPACED_OR,"");;
    } else {
      this.operator = "";
      throw new RuntimeException(String.format("AND/OR expected at start of : %s", remainingString));
    }
    // Construct nested Filter objects for left and right filters (adding '_0' to ids for left and '_1' to ids for right filters)
    this.leftFilter = new Filter(leftFilterString, this.generator, this.id + "_0");
    this.rightFilter = new Filter(remainingString, this.generator, this.id + "_1");
  }
  public void setVariablesFromChildren(){ // Concatenate nested filter values
    if (this.leftFilter != null & this.rightFilter != null){ // Check to see that we have left and right child Filters
      // Build out Mapping Table Preselects
      if (this.leftFilter.getMappingPreselect().isEmpty() & this.rightFilter.getMappingPreselect().isEmpty()) {
        this.mappingTablePreselect = "";
      } else if (this.leftFilter.getMappingPreselect().isEmpty()) {
        this.mappingTablePreselect = this.rightFilter.getMappingPreselect();
      } else if (this.rightFilter.getMappingPreselect().isEmpty()) {
        this.mappingTablePreselect = this.leftFilter.getMappingPreselect();
      } else {
        this.mappingTablePreselect = this.leftFilter.getMappingPreselect() + ", " + rightFilter.getMappingPreselect();
      }
      this.filterPreselect = this.leftFilter.getFilterPreselect() + ", " + rightFilter.getFilterPreselect();
      this.unionIntersect = "(" + this.leftFilter.getUnionIntersect() + " " + this.operator + " " + this.rightFilter.getUnionIntersect() + ")";
    }
  }
  public void setIncludeCountQuery(){
    if (this.isRoot && this.leftFilter == null && this.rightFilter == null){
      // Don't need to add mapping table preselect statements and union/intersect statements if the query isn't nested
      if (this.entityTableName.equals(this.filterTableName) || this.filterTableName.endsWith("_data_source") || this.filterTableName.endsWith("_associated_project")){
        String count_template = "WITH FULLFILTERPRESELECT SELECT COUNT(DISTINCT(FILTERTABLEKEY)) FROM FILTERPRESELECTNAME";
        this.includeCountQuery = replaceKeywords(count_template);
      } else {
        if (this.mappingTablePreselect.isEmpty()) {
          String count_template = "WITH FULLFILTERPRESELECT SELECT COUNT(DISTINCT(FILTERTABLEKEY)) FROM FILTERTABLENAME WHERE FILTERTABLEKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME)";
          this.includeCountQuery = replaceKeywords(count_template);
        } else {
          if (this.mappingTableName.equals(this.entityTableName)){
            String count_template = "WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT SELECT COUNT(DISTINCT(FILTERTABLEKEY)) FROM MAPPINGTABLENAME WHERE FILTERTABLEKEY IN (SELECT COMMONALIAS FROM MAPPINGPRESELECTNAME)";
            this.includeCountQuery = replaceKeywords(count_template);
          } else {
            String count_template = "WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT SELECT COUNT(DISTINCT(COMMONALIAS)) FROM MAPPINGTABLENAME WHERE COMMONALIAS IN (SELECT COMMONALIAS FROM MAPPINGPRESELECTNAME)";
            this.includeCountQuery = replaceKeywords(count_template);
          }
        }
      }


    } else if (this.isRoot) {
      if (this.mappingTablePreselect.isEmpty()){ // Filters only applied to entity table
        String count_template = "WITH FULLFILTERPRESELECT SELECT COUNT(DISTINCT(COMMONALIAS)) FROM (UNIONINTERSECT) as count_result";
        this.includeCountQuery = replaceKeywords(count_template);
      } else {
        String count_template = "WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT SELECT COUNT(DISTINCT(COMMONALIAS)) FROM (UNIONINTERSECT) as count_result";
        this.includeCountQuery = replaceKeywords(count_template);
      }

    }
  }
  public void setCountEndpointQuery() {
    if (!this.isRoot){
      return;
    }
    String count_template = "";
    if (this.mappingTablePreselect.isEmpty()) { // Filters only applied to entity table
      count_template = "SELECT row_to_json(json) FROM (WITH FULLFILTERPRESELECT, ENTITYTABLENAME_preselect_ids AS (UNIONINTERSECT), ENTITYTABLECOUNTPRESELECT, COUNTPRESELECT COUNTSELECT) as json";
    } else {
      count_template = "SELECT row_to_json(json) FROM (WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT, ENTITYTABLENAME_preselect_ids AS (UNIONINTERSECT), ENTITYTABLECOUNTPRESELECT, COUNTPRESELECT COUNTSELECT) as json";
    }
    setEntityTableCountPreselect();
    setCountPreselectAndSelect();
    this.countEndpointQuery = replaceKeywords(count_template);
  }

  public void setPagedPreselectQuery(){
    if (!this.isRoot){
      return;
    }
    String preselect_template = "";
    if (this.mappingTablePreselect.isEmpty()){
      preselect_template = "WITH FULLFILTERPRESELECT";
    } else{
      preselect_template = "WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT";
    }
    //Build new WHERE filter within paged query
    String replace_filter_template = "";
    List<String> joinTableColumnNames = Arrays.stream(this.dataSetInfo
                    .getTableInfo(this.entityTableName)
                    .getColumnDefinitions())
            .sequential().map(ColumnDefinition::getName).collect(Collectors.toList());
    if (joinTableColumnNames.contains("integer_id_alias")){
      replace_filter_template = "ENTITYTABLENAME.integer_id_alias IN (UNIONINTERSECT)";
    } else if (joinTableColumnNames.contains(this.commonAlias)) {
      replace_filter_template = String.format("ENTITYTABLENAME.%s  IN (UNIONINTERSECT)", this.commonAlias);
    } else {
      throw new RuntimeException("Unknown column to use for filter");
    }
    this.pagedReplacementFilter = replaceKeywords(replace_filter_template);

    //Remove unnecessary joins
    String originalJoinString = this.originalQuery
                                .substring(this.originalQuery
                                      .indexOf(replaceKeywords("FROM ENTITYTABLENAME AS ENTITYTABLENAME")),
                                                          this.originalQuery.indexOf("WHERE"));
    String originalSelectString = this.originalQuery
                                  .substring(0, this.originalQuery
                                      .indexOf(replaceKeywords("FROM ENTITYTABLENAME AS ENTITYTABLENAME")));
    List<String> joinList = List.of(originalJoinString.split("(?=(LEFT|INNER|RIGHT|FULL)\\s+JOIN)"));
    for (String joinString : joinList){
      String search = "JOIN";
      if (!joinString.contains(search)) continue;
      int tableStartIndex = joinString.indexOf(search) + search.length();
      int tableEndIndex = joinString.indexOf("AS");
      String joinTableName = joinString.substring(tableStartIndex, tableEndIndex).trim();
      if (!originalSelectString.contains(joinTableName)){
        this.originalReplaceFilterQuery = this.originalReplaceFilterQuery.replace(joinString,"");
      }
    }
    // Combine everything for new preselect paged query
    this.pagedPreselectQuery = replaceKeywords(preselect_template + " " + this.originalReplaceFilterQuery);

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
            .replace("MAPPINGPRESELECTNAME", this.mappingPreselectName)
            .replace("FULLMAPPINGPRESELECT", this.mappingTablePreselect)
            .replace("COMMONALIAS", this.commonAlias)
            .replace("UNIONINTERSECT", this.unionIntersect)
            .replace("ENTITYTABLENAME", this.entityTableName)
            .replace("MAPPINGFILETABLENAME", this.mappingFileTableName)
            .replace("MAPPINGFILEENTITYKEY", this.mappingFileEntityKey)
            .replace("ENTITYTABLECOUNTPRESELECT", this.entityTableCountPreselect)
            .replace("MAPPINGFILEMAPPINGKEY", this.mappingFileMappingKey)
            .replace("COUNTPRESELECT", this.countPreselect)
            .replace("COUNTSELECT", this.countSelect)
            .replace("PAGEDREPLACEMENTFILTER", this.pagedReplacementFilter);
  }
  public void setJoinString(List<Join> joinPath){ // Builds out join statements from JoinPath
    StringBuilder fullJoinString = new StringBuilder();
    boolean isCommonAliasFound = Boolean.FALSE;
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
      List<String> joinTableColumnNames = Arrays.stream(this.dataSetInfo
                              .getTableInfo(join.getKey()
                              .getDestinationTableName())
                              .getColumnDefinitions())
                              .sequential().map(ColumnDefinition::getName).collect(Collectors.toList());
      if (joinTableColumnNames.contains(commonAlias)){
        isCommonAliasFound = Boolean.TRUE;
        break;
      }
    }
    if (!isCommonAliasFound){
      throw new RuntimeException(String.format("Common alias '%s' not found in joinPath from %s table", this.commonAlias, this.filterTableName));
    }
    this.joinString = fullJoinString.toString();
  }

  public void setEntityTableCountPreselect(){
    String entity_preselect_template = "ENTITYTABLENAME_preselect AS (ENTITYSELECT FROMTABLES WHERECLAUSE)";
    StringBuilder entitySelect = new StringBuilder();
    StringBuilder fromTables = new StringBuilder("FROM ENTITYTABLENAME");
    StringBuilder whereClause = new StringBuilder();
    if (this.entityTableName.equals("somatic_mutation")){
      entitySelect.append("SELECT DISTINCT ENTITYTABLENAME.subject_alias");
      whereClause.append("WHERE subject_alias IN (SELECT COMMONALIAS FROM ENTITYTABLENAME_preselect_ids)");
    } else {
      entitySelect.append("SELECT DISTINCT ENTITYTABLENAME.integer_id_alias AS COMMONALIAS");
      whereClause.append("WHERE integer_id_alias IN (SELECT COMMONALIAS FROM ENTITYTABLENAME_preselect_ids)");
    }
    ArrayList<ColumnDefinition> allCountFields = new ArrayList<>();
    allCountFields.addAll(this.countGenerator.getTotalCountFields());
    allCountFields.addAll(this.countGenerator.getGroupedCountFields());
    for (ColumnDefinition countField : allCountFields) {
      String count_field_select_template = ", FIELDNAME";
      String fieldName = countField.getName();
      String fieldTableName = countField.getTableName();
      if (!this.entityTableName.equals("file") && fieldTableName.contains("file")){
        continue;
      }
      if (!fieldTableName.equals(this.entityTableName)) {
        count_field_select_template = ", FIELDTABLENAME.FIELDNAME";
        List<Join> joinPath = this.joinBuilder.getPath(this.entityTableName, fieldTableName, this.commonAlias);
        if (joinPath.size() != 1) {
          throw new RuntimeException(String.format("No direct path from %s to %s for entity_preselect construction", this.entityTableName, fieldTableName));
        }
        String fieldTableJoinKey = joinPath.get(0).getKey().getFields()[0];
        String where_clause_template = "AND integer_id_alias = FIELDTABLENAME.FIELDTABLEJOINKEY";
        if (!fromTables.toString().contains(fieldTableName)) {
          fromTables.append(", ").append(fieldTableName);
          whereClause.append(where_clause_template
                  .replace("FIELDTABLENAME",fieldTableName)
                  .replace("FIELDTABLEJOINKEY",fieldTableJoinKey));
        }
      }
      entitySelect.append(count_field_select_template
              .replace("FIELDTABLENAME",fieldTableName)
              .replace("FIELDNAME",fieldName));
    }
    this.entityTableCountPreselect = entity_preselect_template
            .replace("ENTITYSELECT", entitySelect.toString())
            .replace("FROMTABLES", fromTables.toString())
            .replace("WHERECLAUSE", whereClause.toString());
    this.entityTableCountPreselect = replaceKeywords(this.entityTableCountPreselect);
  }

  public void setCountPreselectAndSelect(){
    String countMethod = "";
    if (this.entityTableName.equals("somatic_mutation")) {
      countMethod = "COUNT(*)";
    } else {
      countMethod = String.format("COUNT(DISTINCT %s)", this.commonAlias);
    }
    StringBuilder count_preselect = new StringBuilder();
    StringBuilder count_select = new StringBuilder("SELECT (SELECT COUNTMETHOD FROM ENTITYTABLENAME_preselect) as total_count,");

    for (ColumnDefinition totalCountField : this.countGenerator.getTotalCountFields()){

      if (!this.entityTableName.equals(totalCountField.getTableName())){
        List<Join> joinPath = this.joinBuilder.getPath(totalCountField.getTableName(), this.entityTableName, this.commonAlias);
        if (joinPath.size() == 1){
          this.mappingFileTableName = joinPath.get(0).getKey().getFromTableName();
          String field_select = "(SELECT COUNT(DISTINCT(TOTALCOUNTFIELDNAME)) FROM TOTALCOUNTFIELDTABLENAME WHERE COMMONALIAS IN (SELECT COMMONALIAS FROM ENTITYTABLENAME_preselect)) AS file_id,";
          field_select = field_select
                  .replace("TOTALCOUNTFIELDNAME", totalCountField.getName())
                  .replace("TOTALCOUNTFIELDTABLENAME", totalCountField.getTableName());
          count_select.append(replaceKeywords(field_select));
        }
        if (joinPath.size() == 3){
          this.mappingFileTableName = joinPath.get(1).getKey().getDestinationTableName();
          this.mappingFileEntityKey = joinPath.get(2).getKey().getFromField();
          this.mappingFileMappingKey = joinPath.get(0).getKey().getFromField();
          String field_preselect = "ENTITYTABLENAME_file_alias AS (SELECT file_mapping.MAPPINGFILEMAPPINGKEY FROM MAPPINGFILETABLENAME file_mapping, ENTITYTABLENAME_preselect entity_preselect WHERE file_mapping.MAPPINGFILEENTITYKEY = entity_preselect.COMMONALIAS),";
          count_preselect.append(replaceKeywords(field_preselect));
          String field_select = "(SELECT COUNT(DISTINCT(file_mapping.TOTALCOUNTFIELDNAME)) FROM ENTITYTABLENAME_file_alias file_preselect, TOTALCOUNTFIELDTABLENAME file_mapping WHERE file_mapping.MAPPINGFILEMAPPINGKEY = file_preselect.MAPPINGFILEMAPPINGKEY) AS file_id,";
          field_select = field_select
                  .replace("TOTALCOUNTFIELDNAME", totalCountField.getName())
                  .replace("TOTALCOUNTFIELDTABLENAME", totalCountField.getTableName());
          count_select.append(replaceKeywords(field_select));
        } // TODO determine what happens if joinpath not 3 or 1 ANSWER: Does not happen in current schema
      } else if (!totalCountField.getName().equals("id")) {
        String field_select = "(SELECT COUNTMETHOD FROM ENTITYTABLENAME_preselect) AS ENTITYTABLENAME_id,";
        field_select = field_select
                .replace("TOTALCOUNTFIELDNAME", totalCountField.getName());
        count_select.append(replaceKeywords(field_select));
      }
    }
    for (ColumnDefinition groupedCountField : this.countGenerator.getGroupedCountFields()){
      String field_preselect = "";
      String field_select = "";
      if (this.entityTableName.equals(groupedCountField.getTableName())){
        field_preselect = "GROUPEDCOUNTFIELDNAME_count AS (SELECT row_to_json(subquery) AS json_GROUPEDCOUNTFIELDNAME FROM (SELECT GROUPEDCOUNTFIELDNAME, COUNTMETHOD AS count FROM ENTITYTABLENAME_preselect GROUP BY GROUPEDCOUNTFIELDNAME) AS subquery),";
        field_select = "(SELECT array_agg(json_GROUPEDCOUNTFIELDNAME) FROM GROUPEDCOUNTFIELDNAME_count) AS GROUPEDCOUNTFIELDNAME,";
      } else {
        field_preselect = "GROUPEDCOUNTFIELDTABLENAME_GROUPEDCOUNTFIELDNAME_count AS (SELECT row_to_json(subquery) AS json_GROUPEDCOUNTFIELDTABLENAME_GROUPEDCOUNTFIELDNAME FROM (SELECT GROUPEDCOUNTFIELDNAME, COUNTMETHOD AS count FROM ENTITYTABLENAME_preselect GROUP BY GROUPEDCOUNTFIELDNAME) AS subquery),";
        field_select = "(SELECT array_agg(json_GROUPEDCOUNTFIELDTABLENAME_GROUPEDCOUNTFIELDNAME) FROM GROUPEDCOUNTFIELDTABLENAME_GROUPEDCOUNTFIELDNAME_count) AS GROUPEDCOUNTFIELDTABLENAME_GROUPEDCOUNTFIELDNAME,";
      }
      field_preselect = field_preselect
              .replace("GROUPEDCOUNTFIELDNAME", groupedCountField.getName())
              .replace("GROUPEDCOUNTFIELDTABLENAME", groupedCountField.getTableName());
      count_preselect.append(replaceKeywords(field_preselect));

      field_select = field_select
              .replace("GROUPEDCOUNTFIELDNAME", groupedCountField.getName())
              .replace("GROUPEDCOUNTFIELDTABLENAME", groupedCountField.getTableName());
      count_select.append(replaceKeywords(field_select));
    }
    this.countPreselect = replaceKeywords(count_preselect.toString().replace("COUNTMETHOD", countMethod));
    if (this.countPreselect.endsWith(",")){
      this.countPreselect = this.countPreselect.substring(0, this.countPreselect.length() - 1);
    }
    this.countSelect = replaceKeywords(count_select.toString().replace("COUNTMETHOD", countMethod));
    if (this.countSelect.endsWith(",")){
      this.countSelect = this.countSelect.substring(0, this.countSelect.length() - 1);
    }
  }



  public String getMappingPreselect(){
    return this.mappingTablePreselect;
  }
  public String getFilterPreselect(){
    return this.filterPreselect;
  }
  public String getUnionIntersect(){
    return this.unionIntersect;
  }
  public String getIncludeCountQuery(){
    return this.includeCountQuery;
  }
  public String getCountEndpointQuery(){
    return this.countEndpointQuery;
  }
//  public String getFileFilters() {return this.fileFilters;}
//  public String getNonFileFilters() {return this.nonFileFilters;}
  public String getPagedPreselectQuery() {return this.pagedPreselectQuery;}
}
