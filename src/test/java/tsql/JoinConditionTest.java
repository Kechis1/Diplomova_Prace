package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.JoinConditionTransformation;
import DP.Transformations.Query;
import DP.Transformations.TransformationBuilder;
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
public class JoinConditionTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private JoinConditionTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new JoinConditionTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessaryJoinConditionOneRunTest {index} query = {0}")
    @MethodSource("doFindNecessaryJoinConditionSource")
    void doFindNecessaryJoinConditionOneRunTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryJoinConditionOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryJoinConditionSource")
    void doFindUnnecessaryJoinConditionOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " DUPLICATE CONDITION", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryJoinConditionFullRunTest {index} query = {0}, resultQuery = {2}")
    @MethodSource("doFindUnnecessaryJoinConditionSource")
    void doFindUnnecessaryJoinConditionFullRunTest(String requestQuery, String oneRunResultQuery, String fullRunResultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), 4);
        assertEquals(query.getQueryTransforms().get(2).size(), 3);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " DUPLICATE CONDITION", query.getQueryTransforms().get(1).get(1).getMessage());
        assertTrue(query.isChanged());
    }


    public static Stream<Arguments> doFindUnnecessaryJoinConditionSource() {
        return Stream.of(
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE SDT.SID = SDE.SID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID WHERE",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDT.SID = SDE.SID AND SDE.PID = PDT.PID",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND",
                        "SELECT * FROM DBO.STUDENT SDT INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID LEFT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID",
                        "SELECT * FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID LEFT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND",
                        "SELECT * FROM DBO.STUDENT SDT LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID LEFT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDE.PID = PDT.PID",
                        "SELECT * FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND",
                        "SELECT * FROM DBO.STUDENT SDT RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID"),
                Arguments.arguments("SELECT * FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDE.PID = PDT.PID",
                        "SELECT * FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND",
                        "SELECT * FROM DBO.STUDENT SDT FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID")
        );
    }

    public static Stream<Arguments> doFindNecessaryJoinConditionSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "RIGHT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "LEFT JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "INNER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "INNER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "RIGHT JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID " +
                        "WHERE SDT.SID = SDE.SID"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDENT SDT " +
                        "FULL OUTER JOIN DBO.STUDUJE SDE ON SDT.SID = SDE.SID " +
                        "FULL OUTER JOIN DBO.PREDMET PDT ON SDE.PID = PDT.PID AND SDT.SID = SDE.SID")
        );
    }
}
