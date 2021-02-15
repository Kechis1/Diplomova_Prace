package tsql;

import DP.Database.ColumnItem;
import DP.Database.DatabaseMetadata;
import DP.Database.DatabaseTable;
import DP.Exceptions.UnnecessaryStatementException;
import DP.Transformations.ExistsTransformation;
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
public class ExistsTest {
    @Mock
    private DatabaseMetadata metadata;
    @Mock
    private ExistsTransformation transformation;
    @Mock
    private TransformationBuilder transformationBuilder;

    @BeforeEach
    void init() {
        metadata = DatabaseMetadata.LoadFromJson("databases/db_student_studuje_predmet.json");
        transformation = new ExistsTransformation(null, metadata);
        transformationBuilder = new TransformationBuilder(metadata);
    }

    @ParameterizedTest(name = "doFindNecessaryExistsOneRunTest {index} query = {0}")
    @MethodSource("doFindNecessaryExistsSource")
    void doFindNecessaryExistsOneRunTest(String requestQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertFalse(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryExistsOneRunTest {index} query = {0}, resultQuery = {1}")
    @MethodSource("doFindUnnecessaryExistsSource")
    void doFindUnnecessaryExistsTest(String requestQuery, String resultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryExistsFullRunTest {index} query = {0}, resultQuery = {2}")
    @MethodSource("doFindUnnecessaryExistsSource")
    void doFindUnnecessaryExistsTest(String requestQuery, String oneRunResultQuery, String fullRunResultQuery) {
        Query query = new Query(requestQuery, requestQuery);
        transformationBuilder.makeQuery(query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), fullRunResultQuery.toUpperCase());
        assertEquals(query.getCurrentRunNumber(), 2);
        assertNotNull(query.getQueryTransforms());
        assertEquals(query.getQueryTransforms().get(1).size(), 3);
        assertEquals(query.getQueryTransforms().get(2).size(), 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindUnnecessaryExistsBasedOnRecordsCountTest {index} query = {0}, recordsCount = {1}, resultQuery = {2}")
    @MethodSource("doFindUnnecessaryExistsBasedOnRecordsCountSource")
    void doFindUnnecessaryExistsBasedOnRecordsCountTest(String requestQuery, int recordsCount, String resultQuery) {
        DatabaseTable table = metadata.findTable("STUDUJE", "SDT");
        Query query = new Query(requestQuery, requestQuery);
        table.setRecordsCount(recordsCount);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertNotEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertEquals(query.getCurrentQuery().toUpperCase(), resultQuery.toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals(UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS", query.getQueryTransforms().get(1).get(0).getMessage());
        assertTrue(query.isChanged());
    }

    @ParameterizedTest(name = "doFindNecessaryExistsBasedOnNullableForeignKeyTest {index} query = {0}")
    @MethodSource("doFindNecessaryExistsBasedOnNullableForeignKeySource")
    void doFindUnnecessaryExistsBasedOnNullableForeignKeyTest(String requestQuery) {
        DatabaseTable table = metadata.findTable("STUDUJE", "SDT");
        ColumnItem column = table.findColumn("PID");
        column.setNullable(true);
        Query query = new Query(requestQuery, requestQuery);
        query.addRun(1, false);
        query.setCurrentRunNumber(1);
        transformation.transformQuery(metadata, query);
        assertEquals(query.getCurrentQuery().toUpperCase(), query.getOriginalQuery().toUpperCase());
        assertTrue(query.getQueryTransforms() != null && query.getQueryTransforms().get(1).size() == 1);
        assertEquals("OK", query.getQueryTransforms().get(1).get(0).getMessage());
        assertFalse(query.isChanged());
    }

    public static Stream<Arguments> doFindUnnecessaryExistsBasedOnRecordsCountSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET PDT WHERE NOT EXISTS (SELECT * FROM STUDUJE SDT WHERE PDT.PID = SDT.PID)",
                0,
                "SELECT * FROM DBO.PREDMET PDT WHERE"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT * FROM STUDUJE)",
                        46,
                        "SELECT * FROM DBO.PREDMET WHERE")
        );
    }

    public static Stream<Arguments> doFindUnnecessaryExistsSource() {
        return Stream.of(Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT 1)",
                "SELECT * FROM DBO.PREDMET WHERE",
                "SELECT * FROM DBO.PREDMET"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT 0)",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT null)",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET"),
                Arguments.arguments("SELECT * FROM DBO.PREDMET WHERE EXISTS (SELECT t1.a FROM (SELECT 1 as a) t1)",
                        "SELECT * FROM DBO.PREDMET WHERE",
                        "SELECT * FROM DBO.PREDMET"),
                Arguments.arguments("SELECT * FROM DBO.STUDUJE SDT WHERE EXISTS (SELECT * FROM DBO.PREDMET PDT WHERE SDT.PID = PDT.PID)",
                        "SELECT * FROM DBO.STUDUJE SDT WHERE",
                        "SELECT * FROM DBO.STUDUJE SDT")
        );
    }

    public static Stream<Arguments> doFindNecessaryExistsBasedOnNullableForeignKeySource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                "FROM DBO.STUDUJE SDT " +
                "WHERE EXISTS (SELECT * " +
                "FROM DBO.PREDMET PDT " +
                "WHERE SDT.PID = PDT.PID " +
                ")")
        );
    }

    public static Stream<Arguments> doFindNecessaryExistsSource() {
        return Stream.of(Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET PDT " +
                        "WHERE EXISTS (SELECT * " +
                        "FROM DBO.STUDUJE SDT " +
                        "WHERE PDT.PID = SDT.PID " +
                        ")"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.PREDMET " +
                        "WHERE NOT EXISTS (SELECT 1)"),
                Arguments.arguments("SELECT * " +
                        "FROM DBO.STUDUJE SDT " +
                        "WHERE EXISTS (SELECT * " +
                        "FROM DBO.PREDMET PDT " +
                        "WHERE SDT.PID = PDT.PID AND PDT.JMENO = 'DAIS' " +
                        ")")
        );
    }
}
