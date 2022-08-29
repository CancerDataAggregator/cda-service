package bio.terra.cda.app.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.cda.app.helpers.Schemas;
import java.io.IOException;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class DataSetInfoTest {
  private final Schemas schemas =
      new Schemas.SchemaBuilder("all_Subjects_v3_0_final", "all_Files_v3_0_final").build();
  private final DataSetInfo dataSetInfo =
      new DataSetInfo.DataSetInfoBuilder()
          .addTableSchema("all_Subjects_v3_0_final", schemas.getSchema())
          .build();

  public DataSetInfoTest() throws IOException {}

  @Test
  void testAllTablesLoaded() {
    TableInfo subjectTable = dataSetInfo.getTableInfo("Subject");
    TableInfo researchSubjectTable = dataSetInfo.getTableInfo("ResearchSubject");
    TableInfo subjectAssociatedProject = dataSetInfo.getTableInfo("subject_associated_project");
    TableInfo fileTable = dataSetInfo.getTableInfo("File");

    assertTrue(Objects.nonNull(subjectTable));
    assertTrue(Objects.nonNull(fileTable));
    assertTrue(Objects.nonNull(researchSubjectTable));
    assertTrue(Objects.nonNull(subjectAssociatedProject));

    assertEquals("all_Subjects_v3_0_final", subjectTable.getAdjustedTableName());

    assertEquals(TableInfo.TableInfoTypeEnum.TABLE, subjectTable.getType());
    assertEquals(TableInfo.TableInfoTypeEnum.TABLE, fileTable.getType());
    assertEquals(TableInfo.TableInfoTypeEnum.NESTED, researchSubjectTable.getType());
    assertEquals(TableInfo.TableInfoTypeEnum.ARRAY, subjectAssociatedProject.getType());
  }

  @Test
  void testSearchableFields() {
    assertTrue(Objects.isNull(dataSetInfo.getSchemaDefinitionByFieldName("id")));
    assertTrue(Objects.nonNull(dataSetInfo.getSchemaDefinitionByFieldName("subject_id")));
    assertTrue(Objects.nonNull(dataSetInfo.getSchemaDefinitionByFieldName("file_id")));
    assertTrue(
        Objects.nonNull(dataSetInfo.getSchemaDefinitionByFieldName("member_of_research_project")));

    assertEquals(
        dataSetInfo.getTableInfo("Subject").getTableName(),
        dataSetInfo.getTableInfoFromField("subject_id").getTableName());
    assertEquals(
        dataSetInfo.getTableInfo("ResearchSubject").getTableName(),
        dataSetInfo.getTableInfoFromField("member_of_research_project").getTableName());
  }
}
