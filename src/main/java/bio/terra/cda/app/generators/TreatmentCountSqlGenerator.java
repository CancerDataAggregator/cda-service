package bio.terra.cda.app.generators;

import bio.terra.cda.generated.model.Query;

import java.io.IOException;

@CountQueryGenerator(Entity = "Treatment",
        FieldsToCount = {"identifier.system", "treatment_type", "treatment_effect"},
        ExcludedFields = {})
public class TreatmentCountSqlGenerator extends EntityCountSqlGenerator {
    public TreatmentCountSqlGenerator(String qualifiedTable, Query rootQuery, String version)
            throws IOException {
        super(qualifiedTable, rootQuery, version);
    }
}
