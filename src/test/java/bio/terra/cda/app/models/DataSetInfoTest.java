package bio.terra.cda.app.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class DataSetInfoTest {
  @Autowired RdbmsSchema rdbmsSchema;

  //  public DataSetInfoTest() throws IOException {}

  @Test
  void testAllTablesLoaded() {
    DataSetInfo dataSetInfo = rdbmsSchema.getDataSetInfo();
    TableInfo subjectTable = dataSetInfo.getTableInfo("subject");
    TableInfo researchSubjectTable = dataSetInfo.getTableInfo("researchsubject");
    TableInfo subjectAssociatedProject = dataSetInfo.getTableInfo("subject_associated_project");
    TableInfo fileTable = dataSetInfo.getTableInfo("file");

    assertTrue(Objects.nonNull(subjectTable));
    assertTrue(Objects.nonNull(fileTable));
    assertTrue(Objects.nonNull(researchSubjectTable));
    assertTrue(Objects.nonNull(subjectAssociatedProject));
  }

  @Test
  void testSearchableFields() {
    DataSetInfo dataSetInfo = rdbmsSchema.getDataSetInfo();
    assertTrue(Objects.isNull(dataSetInfo.getColumnDefinitionByFieldName("id")));
    assertTrue(Objects.nonNull(dataSetInfo.getColumnDefinitionByFieldName("subject_id")));
    assertTrue(Objects.nonNull(dataSetInfo.getColumnDefinitionByFieldName("file_id")));
    assertTrue(
        Objects.nonNull(dataSetInfo.getColumnDefinitionByFieldName("member_of_research_project")));

    assertEquals(
        dataSetInfo.getTableInfo("subject").getTableName(),
        dataSetInfo.getTableInfoFromField("subject_id").getTableName());
    assertEquals(
        dataSetInfo.getTableInfo("researchsubject").getTableName(),
        dataSetInfo.getTableInfoFromField("member_of_research_project").getTableName());
  }
}
