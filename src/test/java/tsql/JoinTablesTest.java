package tsql;

import DP.Database.ColumnItem;
import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Database.Respond.Respond;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlRunner;
import name.falgout.jeffrey.testing.junit.mockito.MockitoExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

import java.util.stream.Stream;

import static org.junit.Assert.*;

@ExtendWith(MockitoExtension.class)
public class JoinTablesTest {
    @Mock
    private DatabaseMetadata metadata;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
    }

    @ParameterizedTest(name = "doFindNecessaryConditionWithNullableTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionWithNullableSource")
    void doFindNecessaryConditionWithNullableTest(String query) {
        DatabaseTable table = metadata.findTable("STUDENT", null);
        ColumnItem sId = table.findColumn("SID");
        sId.setNullable(true);
        Respond respond = new Respond(query, query);
        TSqlRunner.runRedundantJoinTables(metadata, respond);
        assertEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals("OK", respond.getQueryTransforms().get(0).getMessage());
        assertFalse(respond.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryConditionTest {index} query = {0}, message = {1}, resultQuery = {2}")
    @MethodSource("doFindUnnecessaryConditionSource")
    void doFindUnnecessaryConditionTest(String query, String message, String resultQuery) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runRedundantJoinTables(metadata, respond);
        assertNotEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertEquals(respond.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " " + message, respond.getQueryTransforms().get(0).getMessage());
        assertTrue(respond.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryConditionTest {index} query = {0}")
    @MethodSource("doFindNecessaryConditionSource")
    void doFindNecessaryConditionTest(String query) {
        Respond respond = new Respond(query, query);
        TSqlRunner.runRedundantJoinTables(metadata, respond);
        assertEquals(respond.getCurrentQuery().toUpperCase(), respond.getOriginalQuery().toUpperCase());
        assertTrue(respond.getQueryTransforms() != null && respond.getQueryTransforms().size() == 1);
        assertEquals("OK", respond.getQueryTransforms().get(0).getMessage());
        assertFalse(respond.isChanged());
    }

    public static Stream<Arguments> doFindNecessaryConditionWithNullableSource() {
        return Stream.of(
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDT.* " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDE.SID, SDE.body " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDE.* " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID", "LEFT JOIN"),
                Arguments.arguments("SELECT distinct SDT.* " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID", "LEFT JOIN"),
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID", "FULL OUTER JOIN"),
                Arguments.arguments("SELECT distinct SDT.* " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID", "FULL OUTER JOIN"),
                Arguments.arguments("SELECT distinct SDE.SID, SDE.body " +
                        "FROM dbo.student SDT " +
                        "RIGHT JOIN dbo.STUDUJE SDE ON SDT.SID = SDE.SID", "RIGHT JOIN"),
                Arguments.arguments("SELECT distinct SDE.* " +
                        "FROM dbo.student SDT " +
                        "RIGHT JOIN dbo.STUDUJE SDE ON SDT.SID = SDE.SID", "RIGHT JOIN"),
                Arguments.arguments("SELECT distinct SDE.SID, SDE.body " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID", "INNER JOIN"),
                Arguments.arguments("SELECT distinct SDE.* " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID", "INNER JOIN")
        );
    }

    public static Stream<Arguments> doFindNecessaryConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDT.SID, SDT.JMENO " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDT.* " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT distinct SDE.* " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID")
        );
    }
}
