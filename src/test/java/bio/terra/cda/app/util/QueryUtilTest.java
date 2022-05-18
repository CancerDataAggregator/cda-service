package bio.terra.cda.app.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.cda.generated.model.Query;
import org.junit.jupiter.api.Test;

public class QueryUtilTest {
    @Test
    void testRemoveFromTop() {
        Query query = new Query().nodeType(Query.NodeTypeEnum.SELECT)
                                 .l(new Query().nodeType(Query.NodeTypeEnum.SELECTVALUES).value("test"))
                                 .r(new Query().nodeType(Query.NodeTypeEnum.EQUAL)
                                               .l(new Query().nodeType(Query.NodeTypeEnum.COLUMN).value("test_column"))
                                               .r(new Query().nodeType(Query.NodeTypeEnum.QUOTED).value("test_value")));

        Query newQuery = QueryUtil.DeSelectifyQuery(query);

        assertEquals(Query.NodeTypeEnum.EQUAL, newQuery.getNodeType());
        assertEquals("test_column", newQuery.getL().getValue());
    }

    @Test
    void testRemoveNested() {
        Query query = new Query().nodeType(Query.NodeTypeEnum.EQUAL)
                                 .l(new Query().nodeType(Query.NodeTypeEnum.COLUMN)
                                               .value("test_column"))
                                 .r(new Query().nodeType(Query.NodeTypeEnum.SELECT)
                                               .l(new Query().nodeType(Query.NodeTypeEnum.SELECTVALUES)
                                                             .value("test_column"))
                                               .r(new Query().nodeType(Query.NodeTypeEnum.QUOTED)
                                                             .value("test_value")));

        Query newQuery = QueryUtil.DeSelectifyQuery(query);

        assertEquals(Query.NodeTypeEnum.EQUAL, newQuery.getNodeType());
        assertEquals("test_value", newQuery.getR().getValue());
    }

    @Test
    void testNoSelect() {
        Query query =
            new Query().nodeType(Query.NodeTypeEnum.EQUAL)
                .l(new Query().nodeType(Query.NodeTypeEnum.COLUMN)
                        .value("test_column"))
                .r(new Query().nodeType(Query.NodeTypeEnum.QUOTED)
                        .value("test_value"));

        Query newQuery = QueryUtil.DeSelectifyQuery(query);

        assertEquals(Query.NodeTypeEnum.EQUAL, newQuery.getNodeType());
        assertEquals("test_value", newQuery.getR().getValue());
    }

    @Test
    void testMultipleSelect() {
        Query query =
                new Query().nodeType(Query.NodeTypeEnum.SELECT)
                        .l(new Query().nodeType(Query.NodeTypeEnum.SELECTVALUES)
                                .value("test_column"))
                        .r(new Query().nodeType(Query.NodeTypeEnum.EQUAL)
                                .l(new Query().nodeType(Query.NodeTypeEnum.COLUMN)
                                        .value("test_column"))
                                .r(new Query().nodeType(Query.NodeTypeEnum.SELECT)
                                        .l(new Query().nodeType(Query.NodeTypeEnum.SELECTVALUES)
                                                .value("test_column"))
                                        .r(new Query().nodeType(Query.NodeTypeEnum.QUOTED)
                                                .value("test_value"))));

        Query newQuery = QueryUtil.DeSelectifyQuery(query);

        assertEquals(Query.NodeTypeEnum.EQUAL, newQuery.getNodeType());
        assertEquals("test_value", newQuery.getR().getValue());
    }
}
