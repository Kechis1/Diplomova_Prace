package tsql;

import DP.Database.DatabaseMetadata;
import DP.Database.Respond.Respond;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlRunner;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;

import org.junit.jupiter.api.BeforeEach;

import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class GroupByTest {
    @Mock
    private DatabaseMetadata metadata;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
    }

    @ParameterizedTest(name="doFindUnnecessaryGroupByTest {index} query = {0}")
    @MethodSource("doFindUnnecessaryGroupBySource")
    void doFindUnnecessaryGroupByTest(String query) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runGroupBy(metadata, respond);
        assertTrue(respond.isChanged());
        assertNotEquals(respond.getCurrentQuery(), respond.getOriginalQuery());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " GROUP BY", respond.getQueryTransforms().get(0).getMessage());
    }

    @ParameterizedTest(name="doFindNecessaryGroupByTest {index} query = {0}")
    @MethodSource("doFindNecessaryGroupBySource")
    void doFindNecessaryGroupByTest(String query) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runGroupBy(metadata, respond);
        assertFalse(respond.isChanged());
        assertEquals(respond.getCurrentQuery(), respond.getOriginalQuery());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals("OK", respond.getQueryTransforms().get(0).getMessage());
    }

    @ParameterizedTest(name="doFindRewrittenableAggregateFunctionsTest {index} query = {0}, message = {1}")
    @MethodSource("doFindRewrittenableAggregateFunctionsSource")
    void doFindRewrittenableAggregateFunctionsTest(String query, String message) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runGroupBy(metadata, respond);
        assertTrue(respond.isChanged());
        assertNotEquals(respond.getCurrentQuery(), respond.getOriginalQuery());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals(message, respond.getQueryTransforms().get(0).getMessage());
    }


    public static Stream<Arguments> doFindUnnecessaryGroupBySource() {
        return Stream.of(Arguments.arguments("SELECT PID, JMENO FROM DBO.PREDMET GROUP BY PID, JMENO"),
                Arguments.arguments("SELECT pr.pId, stt.sID, ste.sID, ste.pID FROM dbo.student stt JOIN dbo.studuje ste ON stt.sID = ste.sID JOIN dbo.predmet pr ON ste.pID = pr.pID GROUP BY pr.pID, stt.sID, ste.pID, ste.sID, ste.rok")
        );
    }

    public static Stream<Arguments> doFindNecessaryGroupBySource() {
        return Stream.of(Arguments.arguments("SELECT pId, jmeno FROM dbo.predmet GROUP BY pid, jmeno HAVING sum(pid) > 3"),
                Arguments.arguments("SELECT pId, jmeno FROM dbo.predmet GROUP BY pid, jmeno ORDER BY sum(pid)"),
                Arguments.arguments("SELECT pr.pId, stt.sID FROM dbo.student stt JOIN dbo.studuje ste ON stt.sID = ste.sID JOIN dbo.predmet pr ON ste.pID = pr.pID GROUP BY pr.pID, stt.sID"),
                Arguments.arguments("SELECT jmeno, count(pt.pID) FROM predmet pt JOIN studuje st on pt.pID = st.pID GROUP BY jmeno, pt.pID")
        );
    }

    public static Stream<Arguments> doFindRewrittenableAggregateFunctionsSource() {
        return Stream.of(
                Arguments.arguments("SELECT pId, jmeno, sum(pid) FROM dbo.predmet GROUP BY pid, jmeno", "SUM(PID) " + UnnecessaryStatementException.messageCanBeRewrittenTo + " PID"),
                Arguments.arguments("SELECT pId, jmeno, count(*) FROM dbo.predmet GROUP BY pid, sqrt(jmeno), jmeno", "COUNT(*) " + UnnecessaryStatementException.messageCanBeRewrittenTo + " 1"),
                Arguments.arguments("SELECT pId, jmeno, count(pId) FROM dbo.predmet GROUP BY pid, jmeno", "COUNT(PID) " + UnnecessaryStatementException.messageCanBeRewrittenTo + " 1")
        );
    }

}
