package tsql;

import DP.Database.DatabaseMetadata;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.GroupByTransformation;
import DP.Transformations.Query;
import DP.Transformations.TransformationBuilder;
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
    @Mock
    GroupByTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new GroupByTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessaryGroupByOneRunTest {index} query = {0}")
    @MethodSource("doFindNecessaryGroupBySource")
    void doFindNecessaryGroupByOneRunTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertFalse(query.isChanged());
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
    }

    @ParameterizedTest(name = "doFindUnnecessaryGroupByOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryGroupBySource")
    void doFindUnnecessaryGroupByOneRunTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertTrue(query.isChanged());
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " GROUP BY", query.getQueryTransforms().get(1).get(0).getMessage());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
    }

    @ParameterizedTest(name = "doFindUnnecessaryGroupByFullRunTest {index} query = {0}, resultQuery = {2}")
    @MethodSource("doFindUnnecessaryGroupBySource")
    void doFindUnnecessaryGroupByFullRunTest(String requestQuery, String oneRunResultQuery, String fullRunResultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), 5);
        assertEquals(query.getQueryTransforms().get(2).size(), 3);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " GROUP BY", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindRewritableAggregateFunctionsTest {index} query = {0}, resultQuery = {1}, message = {2}")
    @MethodSource("doFindRewritableAggregateFunctionsSource")
    void doFindRewrittenableAggregateFunctionsTest(String requestQuery, String resultQuery, String message) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertTrue(query.isChanged());
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertEquals(query.getQueryTransforms().get(1).get(0).getMessage(), message);
    }


    public static Stream<Arguments> doFindUnnecessaryGroupBySource() {
        return Stream.of(Arguments.arguments("SELECT PID, JMENO FROM DBO.PREDMET GROUP BY PID, JMENO",
                "SELECT PID, JMENO FROM DBO.PREDMET",
                "SELECT PID, JMENO FROM DBO.PREDMET"),
                Arguments.arguments("SELECT pr.pId, stt.sID, ste.sID, ste.pID FROM dbo.student stt JOIN dbo.studuje ste ON stt.sID = ste.sID JOIN dbo.predmet pr ON ste.pID = pr.pID GROUP BY pr.pID, stt.sID, ste.pID, ste.sID, ste.rok",
                        "SELECT PR.PID, STT.SID, STE.SID, STE.PID FROM DBO.STUDENT STT JOIN DBO.STUDUJE STE ON STT.SID = STE.SID JOIN DBO.PREDMET PR ON STE.PID = PR.PID",
                        "SELECT PR.PID, STT.SID, STE.SID, STE.PID FROM DBO.STUDENT STT JOIN DBO.STUDUJE STE ON STT.SID = STE.SID JOIN DBO.PREDMET PR ON STE.PID = PR.PID")
        );
    }

    public static Stream<Arguments> doFindRewritableAggregateFunctionsSource() {
        return Stream.of(
                Arguments.arguments("SELECT pId, jmeno, sum(pid) FROM dbo.predmet GROUP BY pid, jmeno",
                        "SELECT PID, JMENO, PID FROM DBO.PREDMET GROUP BY PID, JMENO",
                        "SUM(PID) " + UnnecessaryStatementException.messageCanBeRewrittenTo + " PID"),
                Arguments.arguments("SELECT pId, jmeno, count(*) FROM dbo.predmet GROUP BY pid, sqrt(jmeno), jmeno",
                        "SELECT PID, JMENO, 1 FROM DBO.PREDMET GROUP BY PID, SQRT(JMENO), JMENO",
                        "COUNT(*) " + UnnecessaryStatementException.messageCanBeRewrittenTo + " 1"),
                Arguments.arguments("SELECT pId, jmeno, count(pId) FROM dbo.predmet GROUP BY pid, jmeno",
                        "SELECT PID, JMENO, 1 FROM DBO.PREDMET GROUP BY PID, JMENO",
                        "COUNT(PID) " + UnnecessaryStatementException.messageCanBeRewrittenTo + " 1")
        );
    }

    public static Stream<Arguments> doFindNecessaryGroupBySource() {
        return Stream.of(Arguments.arguments("SELECT pId, jmeno FROM dbo.predmet GROUP BY pid, jmeno HAVING sum(pid) > 3"),
                Arguments.arguments("SELECT pId, jmeno FROM dbo.predmet GROUP BY pid, jmeno ORDER BY sum(pid)"),
                Arguments.arguments("SELECT pr.pId, stt.sID FROM dbo.student stt JOIN dbo.studuje ste ON stt.sID = ste.sID JOIN dbo.predmet pr ON ste.pID = pr.pID GROUP BY pr.pID, stt.sID"),
                Arguments.arguments("SELECT jmeno, count(pt.pID) FROM predmet pt JOIN studuje st on pt.pID = st.pID GROUP BY jmeno, pt.pID")
        );
    }

}
