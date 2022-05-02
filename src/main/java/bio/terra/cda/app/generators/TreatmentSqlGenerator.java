package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

import java.io.IOException;

@QueryGenerator(Entity = "Treatment", ExcludedFields = {})
public class TreatmentSqlGenerator extends SqlGenerator {
    public TreatmentSqlGenerator(String qualifiedTable, Query rootQuery, String version) throws IOException {
        super(qualifiedTable, rootQuery, version);
    }
}
