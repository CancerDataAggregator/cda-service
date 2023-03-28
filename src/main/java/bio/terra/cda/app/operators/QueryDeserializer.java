package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Operator;
import bio.terra.cda.generated.model.Query;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryDeserializer extends JsonDeserializer<Query> {
    @Override
    public Query deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        // Get reference to ObjectCodec
        ObjectCodec codec = p.getCodec();

        // Parse "object" node into Jackson's tree model
        JsonNode node = codec.readTree(p);

        if (node.isNull()) {
            return null;
        }

        JsonNode selectNode = node.get("select");
        JsonNode whereNode = node.get("where");
        JsonNode orderByNode = node.get("orderBy");
        JsonNode groupByNode = node.get("groupBy");

        DeserializedQuery query = new DeserializedQuery();
        query.setWhereOperator(((BasicOperator) codec.treeToValue(whereNode, Operator.class)));

        if (selectNode.isArray()) {
            List<Operator> selectList = new ArrayList<>();
            for (JsonNode select : selectNode) {
                Select newSelect = new Select();
                newSelect.setNodeType(Operator.NodeTypeEnum.SELECT);

                if (select.hasNonNull("node_type")) {
                    newSelect.setOperators(List.of(codec.treeToValue(select, Operator.class)));
                    if (select.hasNonNull("modifier")) {
                        newSelect.setValue(select.get("modifier").textValue());
                    }
                    selectList.add(newSelect);
                } else {
                    Column column = new Column();
                    column.setNodeType(Operator.NodeTypeEnum.COLUMN);
                    column.setValue(select.textValue());
                    newSelect.setOperators(List.of(column));
                    selectList.add(column);
                }

                newSelect.setOperators(List.of(newSelect));
            }

            query.setSelect(selectList);
        }

        return query;
    }

    private List<>
}
