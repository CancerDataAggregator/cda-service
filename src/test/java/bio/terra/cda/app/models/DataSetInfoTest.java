package bio.terra.cda.app.models;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Objects;

import bio.terra.cda.app.helpers.TableSchemaHelper;
import bio.terra.cda.app.service.StorageService;
import bio.terra.cda.app.util.TableSchema;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.Test;

public class DataSetInfoTest {
  private DataSetInfo getDataSetInfo() throws IOException {
    return TableSchemaHelper.getNewTableSchema("v3").getDataSetInfo("v3");
  }

  public DataSetInfoTest() throws IOException {}

  @Test
  void testAllTablesLoaded() throws IOException {
    DataSetInfo dataSetInfo = getDataSetInfo();
    TableInfo subjectTable = dataSetInfo.getTableInfo("Subject");
    TableInfo researchSubjectTable = dataSetInfo.getTableInfo("ResearchSubject");
    TableInfo subjectAssociatedProject = dataSetInfo.getTableInfo("subject_associated_project");
    TableInfo fileTable = dataSetInfo.getTableInfo("File");

    assertTrue(Objects.nonNull(subjectTable));
    assertTrue(Objects.nonNull(fileTable));
    assertTrue(Objects.nonNull(researchSubjectTable));
    assertTrue(Objects.nonNull(subjectAssociatedProject));

    assertEquals("Subject", subjectTable.getAdjustedTableName());
    assertEquals("all_Subjects_v3_0_final", subjectTable.getTableName());

    assertEquals(TableInfo.TableInfoTypeEnum.TABLE, subjectTable.getType());
    assertEquals(TableInfo.TableInfoTypeEnum.TABLE, fileTable.getType());
    assertEquals(TableInfo.TableInfoTypeEnum.NESTED, researchSubjectTable.getType());
    assertEquals(TableInfo.TableInfoTypeEnum.ARRAY, subjectAssociatedProject.getType());
  }

  @Test
  void testSearchableFields() throws IOException {
    DataSetInfo dataSetInfo = getDataSetInfo();
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
