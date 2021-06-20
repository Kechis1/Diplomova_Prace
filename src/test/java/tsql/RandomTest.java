package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.MetadataException;
import DP.Transformations.Query;
import DP.Transformations.TransformationBuilder;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class RandomTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        try {
            metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
            transformationBuilder = new TransformationBuilder(metadata);
        } catch (MetadataException exception) {
            Assertions.fail(exception.getMessage());
        }
    }

    @ParameterizedTest(name = "doRandomTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doRandomSource")
    void doRandomTest(String requestQuery, String fullRunResultQuery) {
        Query query = new Query(requestQuery, requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertEquals(query.getOutputQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
    }

    public static Stream<Arguments> doRandomSource() {
        return Stream.of(
                Arguments.arguments("SELECT PID FROM PREDMET INTERSECT SELECT PID FROM STUDUJE",
                        "SELECT PID FROM PREDMET INTERSECT SELECT PID FROM STUDUJE"),
                Arguments.arguments("SELECT PID FROM PREDMET EXCEPT SELECT PID FROM STUDUJE",
                        "SELECT PID FROM PREDMET EXCEPT SELECT PID FROM STUDUJE"),
                Arguments.arguments("SELECT CASE WHEN SID = (SELECT 3) THEN 1 ELSE 2 END FROM STUDENT JOIN PREDMET ON STUDENT.SID = PREDMET.SID WHERE 1 = 1",
                        "SELECT CASE WHEN SID = (SELECT 3) THEN 1 ELSE 2 END FROM STUDENT JOIN PREDMET ON STUDENT.SID = PREDMET.SID"),
                Arguments.arguments("SELECT CASE WHEN SID = 3 THEN 1 ELSE 2 END FROM STUDENT",
                        "SELECT CASE WHEN SID = 3 THEN 1 ELSE 2 END FROM STUDENT"),
                Arguments.arguments("SELECT JMENO INTO PID FROM STUDENT",
                        "SELECT JMENO INTO PID FROM STUDENT"),
                Arguments.arguments("SELECT JMENO, PID FROM PREDMET WHERE PID = ANY (SELECT PID FROM STUDUJE)",
                        "SELECT JMENO, PID FROM PREDMET WHERE PID = ANY (SELECT PID FROM STUDUJE)"),
                Arguments.arguments("SELECT JMENO, PID FROM PREDMET WHERE PID = ALL (SELECT 1)",
                        "SELECT JMENO, PID FROM PREDMET WHERE PID = ALL (SELECT 1)"),
                Arguments.arguments("SELECT JMENO, PID FROM PREDMET WHERE PID = ANY (SELECT 1)",
                        "SELECT JMENO, PID FROM PREDMET WHERE PID = ANY (SELECT 1)"),
                Arguments.arguments("SELECT JMENO, PID FROM PREDMET WHERE PID = 1 UNION ALL SELECT JMENO, PID FROM PREDMET WHERE 1 = 1",
                        "SELECT JMENO, PID FROM PREDMET WHERE PID = 1 UNION ALL SELECT JMENO, PID FROM PREDMET WHERE 1 = 1"),
                Arguments.arguments("SELECT COUNT(DISTINCT JMENO) FROM STUDENT",
                        "SELECT COUNT(DISTINCT JMENO) FROM STUDENT"),
                Arguments.arguments("SELECT COUNT(DISTINCT JMENO) FROM STUDENT",
                        "SELECT COUNT(DISTINCT JMENO) FROM STUDENT"),
                Arguments.arguments("SELECT SID * SID + SID FROM STUDENT",
                        "SELECT SID * SID + SID FROM STUDENT"),
                Arguments.arguments("SELECT SID * (SID + ISNULL(SID, 0)) FROM STUDENT",
                        "SELECT SID * (SID + ISNULL(SID, 0)) FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO LIKE 'Ahoj'",
                        "SELECT JMENO FROM STUDENT WHERE JMENO LIKE 'Ahoj'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO IN ('Ahoj')",
                        "SELECT JMENO FROM STUDENT WHERE JMENO IN ('Ahoj')"),
                Arguments.arguments("SELECT SID FROM STUDENT WHERE SID BETWEEN #07/01/1996# AND #07/31/1996#",
                        "SELECT SID FROM STUDENT WHERE SID BETWEEN #07/01/1996# AND #07/31/1996#"),
                Arguments.arguments("SELECT SID FROM STUDENT WHERE #07/01/1996# BETWEEN #07/01/1996# AND #07/31/1996#",
                        "SELECT SID FROM STUDENT WHERE #07/01/1996# BETWEEN #07/01/1996# AND #07/31/1996#"),
                Arguments.arguments("SELECT SID FROM STUDENT WHERE 1 NOT BETWEEN 1 AND 2",
                        "SELECT SID FROM STUDENT WHERE 1 NOT BETWEEN 1 AND 2"),
                Arguments.arguments("SELECT SID FROM STUDENT WHERE 1 NOT BETWEEN 3 AND 4",
                        "SELECT SID FROM STUDENT WHERE 1 NOT BETWEEN 3 AND 4"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO NOT LIKE 'Ahoj'",
                        "SELECT JMENO FROM STUDENT WHERE JMENO NOT LIKE 'Ahoj'"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO IS NULL",
                        "SELECT JMENO FROM STUDENT WHERE JMENO IS NULL"),
                Arguments.arguments("SELECT TOP 100 JMENO FROM STUDENT",
                        "SELECT TOP 100 JMENO FROM STUDENT"),
                Arguments.arguments("SELECT TOP 100 PERCENT JMENO FROM STUDENT",
                        "SELECT TOP 100 PERCENT JMENO FROM STUDENT"),
                Arguments.arguments("SELECT MAX(SID) FROM STUDENT",
                        "SELECT MAX(SID) FROM STUDENT"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE JMENO IS NOT NULL",
                        "SELECT JMENO FROM STUDENT WHERE JMENO IS NOT NULL"),
                Arguments.arguments("SELECT JMENO FROM STUDENT WHERE NOT JMENO = 'ADAM'",
                        "SELECT JMENO FROM STUDENT WHERE NOT JMENO = 'ADAM'"),
                Arguments.arguments("INSERT INTO STUDENT (SID, JMENO) VALUES (1, 'Adam')",
                        "INSERT INTO STUDENT (SID, JMENO) VALUES (1, 'Adam')"),
                Arguments.arguments("SELECT COUNT(*) AS DISTINCT_JMENO FROM (SELECT DISTINCT JMENO FROM STUDENT)",
                        "SELECT COUNT(*) AS DISTINCT_JMENO FROM (SELECT DISTINCT JMENO FROM STUDENT)")
        );
    }
}
