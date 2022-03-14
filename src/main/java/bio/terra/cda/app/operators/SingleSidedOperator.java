package bio.terra.cda.app.operators;

import bio.terra.cda.app.util.TableSchema;

import java.util.Map;
import java.util.stream.Stream;

public class SingleSidedOperator extends BasicOperator {
    @Override
    public Stream<String> getUnnestColumns(String table, Map<String, TableSchema.SchemaDefinition> tableSchemaMap) {
        return ((BasicOperator)getL()).getUnnestColumns(table, tableSchemaMap);
    }
}
