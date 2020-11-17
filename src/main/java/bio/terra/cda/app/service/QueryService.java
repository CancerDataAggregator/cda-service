package bio.terra.cda.app.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryService {
    interface Node {
        Collection<String> columns();
    }

    static class Column implements Node {
        public final String name;

        Column(String name) {
            this.name = name;
        }

        public Collection<String> columns() {
            return Collections.singleton(this.name);
        }

        public String toString() {
            return name;
        }
    }

    static class Value implements Node {
        public final Object value;

        Value(Object value) {
            this.value = value;
        }

        public Collection<String> columns() {
            return Collections.emptySet();
        }

        public String toString() {
            if (value instanceof String) {
                return String.format("'%s'", value);
            }
            return value.toString();
        }
    }

    static class Where implements Node {
        public final Node left;
        public final String operator;
        public final Node right;

        public Where(Node left, String operator, Node right) {
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        public Collection<String> columns() {
            var cols = new ArrayList<String>();
            cols.addAll(left.columns());
            cols.addAll(right.columns());
            return cols;
        }

        @Override
        public String toString() {
            return String.format("(%s %s %s)", left, operator, right);
        }
    }

    static class Select {
        public final Collection<String> columns;

        Select(String... columns) {
            this.columns = Arrays.asList(columns);
        }

        public String toString() {
            return String.join(", ", columns);
        }
    }

    static class Field {
        public String description;
        public String mode;
        public String name;
        public String type;
        public Field[] fields;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Schema {
        public Field[] fields;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SchemaRoot {
        public Schema schema;
    }

    static Map<String, String> makeUnnestDictionary() throws IOException {
        var schemaFile = new File("queryt/schema.json");
        var root = new ObjectMapper().readValue(schemaFile, SchemaRoot.class);
        var unnestDictionary = new HashMap<String, String>();
        for (Field v : root.schema.fields) {
            if (v.type.equals("RECORD")) {
                for (Field field : v.fields) {
                    unnestDictionary.put(field.name, v.name);
                }
            }
        }
        return unnestDictionary;
    }

    static class Query {
        public final String table;
        public final Select select;
        public final Where where;

        public Query(String table, Select select, Where where) {
            this.table = table;
            this.select = select;
            this.where = where;
        }

        String translate(Map<String, String> unnestDict) {
            var fromClause =
                    Stream.concat(Stream.of(table),
                            Stream.concat(select.columns.stream(), where.columns().stream())
                                    .filter(unnestDict::containsKey)
                                    .map(unnestDict::get)
                                    .distinct()
                                    .map(s -> String.format("unnest(%s)", s)))
                            .collect(Collectors.joining(", "));

            return String.format("SELECT %s FROM %s WHERE %s", select, fromClause, where);
        }
    }

    public static void main(String[] args) throws IOException {

        var unnestDict = makeUnnestDictionary();

        var w1 = new Where(new Column("age_at_index"), ">=", new Value(50));
        var w2 = new Where(new Column("project_id"), "like", new Value("TCGA%"));
        var w3 = new Where(new Column("figo_stage"), "=", new Value("Stage IIIC"));

        var w4 = new Where(w1, "and", w2);
        var w5 = new Where(w4, "and", w3);

        var s = new Select("case_id", "age_at_index", "gender", "race", "project_id", "figo_stage");

        var q = new Query("gdc-bq-sample.gdc_metadata.r24_clinical", s, w5);

        System.out.println(q.translate(unnestDict));
    }
}
