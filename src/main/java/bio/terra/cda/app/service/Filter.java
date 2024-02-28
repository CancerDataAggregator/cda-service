package bio.terra.cda.app.service;

import bio.terra.cda.app.builders.JoinBuilder;
import bio.terra.cda.app.generators.EntityCountSqlGenerator;
import bio.terra.cda.app.generators.EntitySqlGenerator;
import bio.terra.cda.app.generators.SqlGenerator;
import bio.terra.cda.app.models.ColumnDefinition;
import bio.terra.cda.app.generators.EntitySqlGenerator;
import bio.terra.cda.app.models.ForeignKey;
import bio.terra.cda.app.models.Join;

import java.util.List;

// Class to construct optimized count preselect SQL statement from the filters in the original count(*) wrapped query
public class Filter {
  final Boolean isRoot;
  private String originalQuery = "";
  private String filterQuery = "";
  private String filterTableName = "";
  private String operator = "";
  private Filter leftFilter = null;
  private Filter rightFilter = null;
  private String filterPreselect = "";
  final EntitySqlGenerator generator;
  private EntityCountSqlGenerator countGenerator = null;
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
  private String mappingFileTableName = "";
  private String mappingFileEntityKey = "";
  private String fileCountPreselect = "";
  private String includeCountQuery = "";
  private String countEndpointQuery = "";
  private String unionIntersect = "";
  final String id;

  /***
   * Class to construct optimized count preselect SQL statement from the filters
   * in the original count(*) wrapped query
   * 
   * @throws RuntimeException If there is problem create the filters
   * @param baseFilterString Originally passed in as generated sql but later
   * @param generator
   * @param isRoot
   * @param id
   */
  public Filter(String baseFilterString, EntitySqlGenerator generator, Boolean isRoot, String id) {
    this.isRoot = isRoot;
    this.id = id;
    if (this.isRoot) {
      this.originalQuery = baseFilterString;
      if (!this.originalQuery.contains("WHERE")) {
        throw new RuntimeException("This query does not contain a where filter");
      }
      String startingFilterString = this.originalQuery.substring(this.originalQuery.indexOf("WHERE") + 5).trim();
      this.filterQuery = parenthesisSubString(startingFilterString);
    } else {
      this.filterQuery = baseFilterString.trim();
    }
    this.generator = generator;
    this.joinBuilder = this.generator.getJoinBuilder();
    this.entityTableName = generator.getEntityTableName();
    this.entityPK = generator.getEntityTableFirstPK();
    constructFilter();
    setVariablesFromChildren();
    setIncludeCountQuery();
  }
  public Filter(String originalQuery, String baseFilterString, EntityCountSqlGenerator generator, Boolean isRoot, String id){
    this.isRoot = isRoot;
    this.id = id;
    this.originalQuery = originalQuery;
    if (baseFilterString.contains("WHERE")){
      String startingFilterString = this.originalQuery.substring(this.originalQuery.indexOf("WHERE")+5).trim();
      this.filterQuery = paranthesisSubString(startingFilterString);
    } else {
      this.filterQuery = baseFilterString.trim();
    }
    this.problemFlag = Boolean.FALSE;
    this.generator = generator;
    this.countGenerator = generator;
    this.joinBuilder = this.generator.getJoinBuilder();
    this.entityTableName = generator.getEntityTableName();
    this.entityPK = generator.getEntityTableFirstPK();
    constructFilter();
    setVariablesFromChildren();
    setCountEndpointQuery();
  }

  public void constructFilter() {
    if (this.filterQuery.startsWith("((") && this.filterQuery.endsWith("))"))
      this.filterQuery = this.filterQuery.substring(1, this.filterQuery.length() - 1);
    
    if (!(this.filterQuery.contains("AND") || this.filterQuery.contains("OR"))) {
      // Get filter table name
      int tableStartIndex;
      if (this.filterQuery.startsWith("(COALESCE(UPPER(")) {
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


      if (joinPath.isEmpty()){ // Filter on the entity table
        Boolean found_alias = Boolean.FALSE;
        for (ForeignKey fk : this.generator.getEntityTable().getForeignKeys()){
          if (fk.getFromField().equals("integer_id_alias")) {
            this.filterTableKey = "integer_id_alias";
            found_alias = Boolean.TRUE;
            this.mappingEntityKey = fk.getFields()[0];
            this.filterPreselectName = replaceKeywords("FILTERTABLENAME_id_preselectIDENTIFIER");
            String preselect_template = "FILTERPRESELECTNAME AS (SELECT FILTERTABLEKEY FROM FILTERTABLENAME WHERE FILTERQUERY)";
            this.filterPreselect = replaceKeywords(preselect_template);

            // Construct SELECT Statement for UNION/INTESECT opertations
            String union_intersect_template = "SELECT FILTERTABLEKEY AS MAPPINGENTITYKEY FROM FILTERPRESELECTNAME";
            this.unionIntersect = replaceKeywords(union_intersect_template);
            break;
          }
        }
        // If there is no integer_id_alias, the count query will not work
        if (!found_alias) {
          throw new RuntimeException(
              String.format("integer_id_alias not found in foreign keys of %s", this.filterTableName));
        }
      } else { // Filter needs to be mapped from filter table to entity table
        this.filterTableKey = joinPath.get(0).getKey().getFromField();
        this.filterPreselectName = replaceKeywords("FILTERTABLENAME_id_preselectIDENTIFIER");
        String preselect_template = "FILTERPRESELECTNAME AS (SELECT FILTERTABLEKEY FROM FILTERTABLENAME WHERE FILTERQUERY)";
        this.filterPreselect = replaceKeywords(preselect_template);

        // Construct Mapping Preselects
        if (joinPath.size() == 2) { // Direct mapping table present -> construct basic mapping preselect
          this.mappingTableName = joinPath.get(0).getKey().getDestinationTableName();
          this.mappingEntityKey = joinPath.get(1).getKey().getFromField();
          this.mappingFilterKey = joinPath.get(0).getKey().getFields()[0];
          this.mappingPreselectName = replaceKeywords("MAPPINGTABLENAME_id_preselectIDENTIFIER");
          String mapping_preselect_template = "MAPPINGPRESELECTNAME AS (SELECT MAPPINGENTITYKEY FROM MAPPINGTABLENAME WHERE MAPPINGFILTERKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
          this.mappingTablePreselect = replaceKeywords(mapping_preselect_template);
        } else if (joinPath.size() > 2) { // Need to apply joins to a mapping table
          this.setJoinString(joinPath);
          this.mappingTableName = joinPath.get(joinPath.size() - 1).getKey().getDestinationTableName();
          this.mappingEntityKey = joinPath.get(joinPath.size() - 1).getKey().getFromField();
          this.mappingFilterKey = joinPath.get(0).getKey().getFields()[0];
          this.mappingPreselectName = replaceKeywords("MAPPINGTABLENAME_FILTERTABLENAME_id_preselectIDENTIFIER");
          String mapping_preselect_template = "MAPPINGPRESELECTNAME AS (SELECT MAPPINGENTITYKEY FROM FILTERTABLENAME AS FILTERTABLENAME JOINSTRING WHERE MAPPINGFILTERKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME))";
          this.mappingTablePreselect = replaceKeywords(mapping_preselect_template);
        }
        // Construct SELECT Statement for UNION/INTESECT opertations
        String union_intersect_template = "SELECT MAPPINGENTITYKEY  FROM MAPPINGPRESELECTNAME";
        this.unionIntersect = replaceKeywords(union_intersect_template);
      }

      this.operator = "";
      this.leftFilter = null;
      this.rightFilter = null;
    } else { // Construct Nested left and right filters
      this.filterTableName = "";
      buildLeftRightFilters();
    }

  }
  public void buildLeftRightFilters(){
    String leftFilterString = paranthesisSubString(this.filterQuery);

    String remainingString = this.filterQuery.substring(leftFilterString.length());
    // Determine what operator (INTERSECT/UNION) to use between left and right filters
    if (remainingString.startsWith(" AND ")){
      this.operator = " INTERSECT ";
      remainingString = remainingString.replaceFirst(" AND ","");
    } else if (remainingString.startsWith(" OR ")) {
      this.operator = " UNION ";
      remainingString = remainingString.replaceFirst(" OR ","");;
    } else {
      this.operator = "";
      throw new RuntimeException(String.format("AND/OR expected at start of : %s", remainingString));
    }
    // Construct nested Filter objects for left and right filters (adding '_0' to ids for left and '_1' to ids for right filters)
    this.leftFilter = new Filter(this.originalQuery, leftFilterString, this.generator, Boolean.FALSE, this.id + "_0");
    this.rightFilter = new Filter(this.originalQuery, remainingString, this.generator, Boolean.FALSE, this.id + "_1");
    

  }
  public void setVariablesFromChildren(){ // Concatenate nested filter values
    if (this.leftFilter != null & this.rightFilter != null){ // Check to see that we have left and right child Filters
      // Ensure root has problemFlag True if any nested child does
      this.problemFlag = (this.problemFlag | this.leftFilter.getProblemFlag() | this.rightFilter.getProblemFlag());
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

      // Get mapping entity key for final "SELECT COUNT(DISTINCT(KEY))" statement
      this.mappingEntityKey = this.leftFilter.getMappingEntityKey();
      // Ensure that the final id key on all the child preselects match otherwise the
      // Union/Intersect statement will break
      if (!this.leftFilter.getMappingEntityKey().equals(this.rightFilter.getMappingEntityKey())) {
        throw new RuntimeException(
            String.format("Mapping entity keys between left and right filters don't match: %s %s",
                this.leftFilter.getMappingEntityKey(), this.rightFilter.getMappingEntityKey()));
      }
    }
  }
  public void setIncludeCountQuery(){
    if (this.isRoot && this.leftFilter == null && this.rightFilter == null){
      // Don't need to add mapping table preselect statements and union/intersect statements if the query isn't nested
      if (this.entityTableName.equals(this.filterTableName)){
        String count_template = "WITH FULLFILTERPRESELECT SELECT COUNT(DISTINCT(FILTERTABLEKEY)) FROM FILTERPRESELECTNAME;";
        this.includeCountQuery = replaceKeywords(count_template);
      } else {

        String count_template = "WITH FULLFILTERPRESELECT SELECT COUNT(DISTINCT(MAPPINGENTITYKEY)) FROM MAPPINGTABLENAME WHERE MAPPINGFILTERKEY IN (SELECT FILTERTABLEKEY FROM FILTERPRESELECTNAME);";
        this.includeCountQuery = replaceKeywords(count_template);
      }

    } else if (this.isRoot) {
      if (this.mappingTablePreselect.isEmpty()){ // Filters only applied to entity table
        String count_template = "WITH FULLFILTERPRESELECT SELECT COUNT(DISTINCT(MAPPINGENTITYKEY)) FROM UNIONINTERSECT as count_result";
        this.includeCountQuery = replaceKeywords(count_template);
        System.out.print("");
      } else {
        String count_template = "WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT SELECT COUNT(DISTINCT(MAPPINGENTITYKEY)) FROM UNIONINTERSECT as count_result";
        this.includeCountQuery = replaceKeywords(count_template);
      }

    }
  }
  public void setCountEndpointQuery() {
    if (!this.isRoot){
      return;
    }
    if (this.mappingTablePreselect.isEmpty()) { // Filters only applied to entity table
      String count_template = "WITH FULLFILTERPRESELECT ENTITYTABLENAME_preselect AS UNIONINTERSECT";
      this.countEndpointQuery = replaceKeywords(count_template);
    } else {
      String count_template = "WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT, ENTITYTABLENAME_preselect_ids AS UNIONINTERSECT, FILECOUNTPRESELECT";
//      if (!entityTableName.equals("file")){
//        List<Join> joinPath = this.joinBuilder.getPath(this.entityTableName, "file", this.mappingEntityKey); // TODO: could optimize by building a better joinPath with this one
//        if (joinPath.size() == 2){
//          this.mappingFileTableName = joinPath.get(0).getKey().getDestinationTableName();
//          this.mappingFileEntityKey = joinPath.get(0).getKey().getFields()[0];
//          String file_count_template = "ENTITYTABLENAME_file_alias AS (SELECT file_mapping.MAPPINGFILEENTITYKEY FROM MAPPINGFILETABLENAME file_mapping, ENTITYTABLENAME_preselect entity_preselect WHERE file_mapping.MAPPINGFILEENTITYKEY = entity_preselect.MAPPINGENTITYKEY)";
//          this.fileCountPreselect = replaceKeywords(file_count_template);
//        }
//
//      }
      String entity_preselect_template = "ENTITYTABLENAME AS (SELECT DISTINCT ENTITYTABLENAME.integer_id_preselect AS MAPPINGENTITYKEY, ";
      for (ColumnDefinition countField : this.countGenerator.getTotalCountFields()){
        String count_field_preselect = countField.getName();

      }
//      this.countGenerator.getTotalCountFields();
      this.countEndpointQuery = replaceKeywords(count_template);
      System.out.print("");
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
            .replace("UNIONINTERSECT", this.unionIntersect)
            .replace("ENTITYTABLENAME", this.entityTableName)
            .replace("MAPPINGFILETABLENAME", this.mappingFileTableName)
            .replace("MAPPINGFILEENTITYKEY", this.mappingFileEntityKey)
            .replace("FILECOUNTPRESELECT", this.fileCountPreselect);
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

  public String parenthesisSubString(String startingString) { // Helper function to extract the string between the first
                                                              // parenthesis and it's closing one
    int openParenthesisCount = 1;
    int indexCursor = 0;
    StringBuilder retString = new StringBuilder();
    retString.append('(');
    while (openParenthesisCount > 0) {
    indexCursor += 1;
    if (startingString.charAt(indexCursor) == '(') {
      openParenthesisCount += 1;
    } else if (startingString.charAt(indexCursor) == ')') {
      openParenthesisCount -= 1;
    }
    retString.append(startingString.charAt(indexCursor));
    }
    return retString.toString();
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
  public String getCountEndpointQuery(){
    return this.countEndpointQuery;
  }

  public String getMappingEntityKey(){
    return this.mappingEntityKey;
  }
}
