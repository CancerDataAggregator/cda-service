package bio.terra.cda.app.operators;

import bio.terra.cda.app.helpers.QueryHelper;
import bio.terra.cda.app.util.QueryContext;
import java.io.IOException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ColumnTest {
    @Test
    void testStringColumnNoDefault() throws IOException {
        BasicOperator columnOperator = new Column().setValue("subject_id");

        QueryContext ctx =
                QueryHelper.getNewQueryContext(
                        "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

        assertEquals("UPPER(Subject.id)", columnOperator.buildQuery(ctx));
    }

    @Test
    void testStringColumnWithDefault() throws IOException {
        BasicOperator columnOperator = new Column().setValue("subject_id");
        columnOperator.setDefaultValue("test");

        QueryContext ctx =
                QueryHelper.getNewQueryContext(
                        "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

        assertEquals("IFNULL(UPPER(Subject.id), @subject_id_1)", columnOperator.buildQuery(ctx));
        assertEquals("test", ctx.getParameterBuilder().getParameterValueMap().get("subject_id_1").getValue());
    }

    @Test
    void testNonStringColumnWithDefault() throws IOException {
        BasicOperator columnOperator = new Column().setValue("days_to_birth");
        columnOperator.setDefaultValue("1");

        QueryContext ctx =
                QueryHelper.getNewQueryContext(
                        "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

        assertEquals("IFNULL(Subject.days_to_birth, @days_to_birth_1)", columnOperator.buildQuery(ctx));
        assertEquals("1", ctx.getParameterBuilder().getParameterValueMap().get("days_to_birth_1").getValue());
    }

    @Test
    void testNonStringColumnNoDefault() throws IOException {
        BasicOperator columnOperator = new Column().setValue("days_to_birth");

        QueryContext ctx =
                QueryHelper.getNewQueryContext(
                        "all_Subjects_v3_0_final", "all_Files_v3_0_final", "Subject", "project", true);

        assertEquals("Subject.days_to_birth", columnOperator.buildQuery(ctx));
    }
}
