package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Operator;
import bio.terra.cda.generated.model.Query;

import java.util.List;
import java.util.Objects;

public class QueryUtil {
    private QueryUtil() {}

    /**
     * Becaouse selcet is now a array this will clear the value with a blank array
     * @param query
     * @return
     */
    public static Query deSelectifyQuery(Query query) {
        query.setSelect(List.of());
        return query;
    }
}
