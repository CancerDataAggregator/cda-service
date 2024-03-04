package bio.terra.cda.app.service;

import bio.terra.cda.app.builders.JoinBuilder;
import bio.terra.cda.app.generators.EntityCountSqlGenerator;
import bio.terra.cda.app.generators.EntitySqlGenerator;
import bio.terra.cda.app.models.ColumnDefinition;
import bio.terra.cda.app.models.ForeignKey;
import bio.terra.cda.app.models.Join;
import bio.terra.cda.generated.model.Query;

import java.util.ArrayList;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.List;
import java.util.Objects;

// Class to construct optimized count preselect SQL statement from the filters in the original count(*) wrapped query
public class Filter {
  final Boolean isRoot;
  private String originalQuery = "";
  private String filterQuery = "";
  private String filterTableName = "";
  private String operator = "";
  private ChildFilter leftFilter = null;
  private ChildFilter rightFilter = null;
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
  private String mappingFileMappingKey = "";
  private String entityTableCountPreselect = "";
  private String countPreselect = "";
  private String countSelect = "";
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
   * @param id
   */
  public Filter(String baseFilterString, EntitySqlGenerator generator, String id) {
//    this.isRoot = isRoot;
    this.isRoot = !(this instanceof ChildFilter);

    this.id = id;
    if (this.isRoot) {
      this.originalQuery = baseFilterString;
      String WHERE = Query.NodeTypeEnum.WHERE.getValue();
      if (!this.originalQuery.contains(WHERE)) {
        throw new RuntimeException("This query does not contain a where filter");
      }
      String startingFilterString = this.originalQuery.substring(this.originalQuery.indexOf(WHERE) + WHERE.length()).trim();
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
    if (this.generator instanceof EntityCountSqlGenerator) {
      this.countGenerator = (EntityCountSqlGenerator) this.generator;
      setCountEndpointQuery();
    } else {
      setIncludeCountQuery();
    }
  }

  public String trimExtraneousParentheses(String query) {
    if(query.startsWith("(") && query.endsWith(")")){
      //Determine if the opening and closing parens match with each other...
      CharacterIterator it = new StringCharacterIterator(query);
      it.next();
      int count = 1;
      while (it.current() != CharacterIterator.DONE) {
        if(it.current() == '(')
          count++;
        if(it.current() == ')') {
          count--;
          //this case occurs when the opening paren has been matched before we
          //get to the end. E.g.: "((a =4)) OR (b=10)"
          if(count == 0 && (it.getIndex() < (query.length()-1)))
            return query;
        }
        it.next();
      }
      //This case means that the opening paren matches the closing paren,
      //E.g.: "(((a=4) OR (b=10)))". We recurse to continue stripping off
      //these extraneous parens
      if(count == 0)
        return trimExtraneousParentheses(query.substring(1, query.length()-1));
    }
    //If we don't have opening and closing parens, there isn't anything to trim
    return query;
  }

  public void constructFilter() {

//    if (this.filterQuery.startsWith("((") && this.filterQuery.endsWith("))"))
//      this.filterQuery = this.filterQuery.substring(1, this.filterQuery.length() - 1);

    String AND = Query.NodeTypeEnum.AND.getValue();
    String OR = Query.NodeTypeEnum.OR.getValue();
    if (!(this.filterQuery.contains(AND) || this.filterQuery.contains(OR))) {
      // Get filter table name
      int tableStartIndex;
      if (this.filterQuery.startsWith("(COALESCE(UPPER(")) {
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

            // Construct SELECT Statement for UNION/INTERSECT operations
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
      this.filterQuery = trimExtraneousParentheses(this.filterQuery);
      this.filterTableName = "";
      buildLeftRightFilters();
    }

  }
  public void buildLeftRightFilters(){
    String leftFilterString = parenthesisSubString(this.filterQuery);

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
    this.leftFilter = new ChildFilter(leftFilterString, this.generator, this.id + "_0");
    this.rightFilter = new ChildFilter(remainingString, this.generator, this.id + "_1");
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
    String count_template = "";
    if (this.mappingTablePreselect.isEmpty()) { // Filters only applied to entity table
      count_template = "SELECT row_to_json(json) FROM (WITH FULLFILTERPRESELECT ENTITYTABLENAME_preselect AS UNIONINTERSECT, ENTITYTABLECOUNTPRESELECT, COUNTPRESELECT COUNTSELECT) as json";
    } else {
      count_template = "SELECT row_to_json(json) FROM (WITH FULLFILTERPRESELECT, FULLMAPPINGPRESELECT, ENTITYTABLENAME_preselect_ids AS UNIONINTERSECT, ENTITYTABLECOUNTPRESELECT, COUNTPRESELECT COUNTSELECT) as json";
    }
    setEntityTableCountPreselect();
    setCountPreselectAndSelect();
    this.countEndpointQuery = replaceKeywords(count_template);
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
            .replace("ENTITYTABLECOUNTPRESELECT", this.entityTableCountPreselect)
            .replace("MAPPINGFILEMAPPINGKEY", this.mappingFileMappingKey)
            .replace("COUNTPRESELECT", this.countPreselect)
            .replace("COUNTSELECT", this.countSelect);
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

  public void setEntityTableCountPreselect(){
    String entity_preselect_template = "ENTITYTABLENAME_preselect AS (ENTITYSELECT FROMTABLES WHERECLAUSE)";
    StringBuilder entitySelect = new StringBuilder("SELECT DISTINCT ENTITYTABLENAME.integer_id_alias AS MAPPINGENTITYKEY");
    StringBuilder fromTables = new StringBuilder("FROM ENTITYTABLENAME");
    StringBuilder whereClause = new StringBuilder("WHERE integer_id_alias IN (SELECT MAPPINGENTITYKEY FROM ENTITYTABLENAME_preselect_ids)");
    ArrayList<ColumnDefinition> allCountFields = new ArrayList<>();
    allCountFields.addAll(this.countGenerator.getTotalCountFields());
    allCountFields.addAll(this.countGenerator.getGroupedCountFields());
    for (ColumnDefinition countField : allCountFields) {
      String count_field_select_template = ", FIELDNAME";
      String fieldName = countField.getName();
      String fieldTableName = countField.getTableName();
      if (!this.entityTableName.equals("file") && fieldTableName.contains("file")){ //TODO check expected behavior with file endpoint
        continue;
      }
      if (!fieldTableName.equals(this.entityTableName)) {
        count_field_select_template = ", FIELDTABLENAME.FIELDNAME";
        List<Join> joinPath = this.joinBuilder.getPath(this.entityTableName, fieldTableName, this.mappingEntityKey);
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
    StringBuilder count_preselect = new StringBuilder();
    StringBuilder count_select = new StringBuilder("SELECT ");

    for (ColumnDefinition totalCountField : this.countGenerator.getTotalCountFields()){

      if (!this.entityTableName.equals(totalCountField.getTableName())){
        List<Join> joinPath = this.joinBuilder.getPath(totalCountField.getTableName(), this.entityTableName, this.mappingEntityKey); // TODO: could optimize by building a better joinPath with this one
        if (joinPath.size() == 3){
          this.mappingFileTableName = joinPath.get(1).getKey().getDestinationTableName();
          this.mappingFileEntityKey = joinPath.get(2).getKey().getFromField();
          this.mappingFileMappingKey = joinPath.get(0).getKey().getFromField();
          String field_preselect = "ENTITYTABLENAME_file_alias AS (SELECT file_mapping.MAPPINGFILEMAPPINGKEY FROM MAPPINGFILETABLENAME file_mapping, ENTITYTABLENAME_preselect entity_preselect WHERE file_mapping.MAPPINGFILEENTITYKEY = entity_preselect.MAPPINGENTITYKEY),";
          count_preselect.append(replaceKeywords(field_preselect));
          String field_select = "(SELECT COUNT(DISTINCT(file_mapping.TOTALCOUNTFIELDNAME)) FROM ENTITYTABLENAME_file_alias file_preselect, TOTALCOUNTFIELDTABLENAME file_mapping WHERE file_mapping.MAPPINGFILEMAPPINGKEY = file_preselect.MAPPINGFILEMAPPINGKEY) AS file_id,";
          field_select = field_select
                  .replace("TOTALCOUNTFIELDNAME", totalCountField.getName())
                  .replace("TOTALCOUNTFIELDTABLENAME", totalCountField.getTableName());
          count_select.append(replaceKeywords(field_select));
        }//TODO determine what happens if joinpath not 3
      } else {
        String field_select = "(SELECT COUNT(TOTALCOUNTFIELDNAME) FROM ENTITYTABLENAME_preselect) AS ENTITYTABLENAME_id,";
        field_select = field_select
                .replace("TOTALCOUNTFIELDNAME", totalCountField.getName());
        count_select.append(replaceKeywords(field_select));
      }
    }
    for (ColumnDefinition groupedCountField : this.countGenerator.getGroupedCountFields()){
      String field_preselect = "";
      String field_select = "";
      if (this.entityTableName.equals(groupedCountField.getTableName())){
        field_preselect = "GROUPEDCOUNTFIELDNAME_count AS (SELECT row_to_json(subquery) AS json_GROUPEDCOUNTFIELDNAME FROM (SELECT GROUPEDCOUNTFIELDNAME, COUNT(MAPPINGENTITYKEY) AS count FROM ENTITYTABLENAME_preselect GROUP BY GROUPEDCOUNTFIELDNAME) AS subquery),";
        field_select = "(SELECT array_agg(json_GROUPEDCOUNTFIELDNAME) FROM GROUPEDCOUNTFIELDNAME_count) AS GROUPEDCOUNTFIELDNAME,";
      } else {
        field_preselect = "GROUPEDCOUNTFIELDTABLENAME_GROUPEDCOUNTFIELDNAME_count AS (SELECT row_to_json(subquery) AS json_GROUPEDCOUNTFIELDTABLENAME_GROUPEDCOUNTFIELDNAME FROM (SELECT GROUPEDCOUNTFIELDNAME, COUNT(MAPPINGENTITYKEY) AS count FROM ENTITYTABLENAME_preselect GROUP BY GROUPEDCOUNTFIELDNAME) AS subquery),";
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
    this.countPreselect = count_preselect.toString();
    if (this.countPreselect.endsWith(",")){
      this.countPreselect = this.countPreselect.substring(0, this.countPreselect.length() - 1);
    }
    this.countSelect = count_select.toString();
    if (this.countSelect.endsWith(",")){
      this.countSelect = this.countSelect.substring(0, this.countSelect.length() - 1);
    }
    System.out.print("");
  }

  public String parenthesisSubString(String startingString) { // Helper function to extract the string between the first
    // parenthesis and it's closing one
    int openParenthesisCount = 1;
    int indexCursor = 0;
    while (openParenthesisCount > 0) {
      indexCursor += 1;
      if (startingString.charAt(indexCursor) == '(') {
        openParenthesisCount += 1;
      } else if (startingString.charAt(indexCursor) == ')') {
        openParenthesisCount -= 1;
      }
    }
    return startingString.substring(0, indexCursor+1);
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

  public String getMappingEntityKey(){
    return this.mappingEntityKey;
  }
}
