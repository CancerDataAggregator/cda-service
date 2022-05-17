package bio.terra.cda.app.util;

import bio.terra.cda.generated.model.Query;

public class QueryUtil {
    private QueryUtil() {}

    public static Query DeSelectifyQuery(Query query) {
        return RemoveSelectsFromQuery(query);
    }

    private static Query RemoveSelectsFromQuery(Query currentQuery) {
        if (currentQuery.getNodeType().equals(Query.NodeTypeEnum.SELECT)) {
            return RemoveSelectsFromQuery(currentQuery.getR());
        } else {
            currentQuery.setL(RemoveSelectsFromQuery(currentQuery.getL()));
            currentQuery.setR(RemoveSelectsFromQuery(currentQuery.getR()));
            return currentQuery;
        }
    }
}
