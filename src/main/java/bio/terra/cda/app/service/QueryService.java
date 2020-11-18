package bio.terra.cda.app.service;

import bio.terra.cda.generated.model.QueryNode;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryService {

    final BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

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

    private Job runJob(Job queryJob) {
        try {
            // Wait for the query to complete.
            queryJob = queryJob.waitFor();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error while polling for job completion", e);
        }

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
        return queryJob;
    }

    private List<String> getJobResults(Job queryJob) {
        try {
            // Get the results.
            TableResult result = queryJob.getQueryResults();

            List<String> jsonData = new ArrayList<>();

            // Print all pages of the results.
            for (FieldValueList row : result.iterateAll()) {
                jsonData.add(row.get("value").getStringValue());
            }

            return jsonData;
        } catch (InterruptedException e) {
            throw new RuntimeException("Error while getting query results", e);
        }
    }

    public List<String> runQuery(QueryNode queryNode) {
        String query = createQueryFromNode(queryNode);
        // Wrap query so it returns JSON
        String jsonQuery = String.format("SELECT TO_JSON_STRING(t,true) from (%s) as t", query);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(jsonQuery)
                        .setUseLegacySql(true)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        queryJob = runJob(queryJob);

        return getJobResults(queryJob);
    }

    private String createQueryFromNode(QueryNode queryNode) {
        // FIXME need to implement
        return null;
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
