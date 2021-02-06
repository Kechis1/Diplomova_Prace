package DP.Transformations;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public abstract class QueryHandler implements ITransformation {
    private final QueryHandler next;
    private final DatabaseMetadata databaseMetadata;

    public QueryHandler(QueryHandler next, DatabaseMetadata databaseMetadata) {
        this.next = next;
        this.databaseMetadata = databaseMetadata;
    }

    public void handleQuery(Query query) {
        if (shouldTransform(query)) {
            do {
                query = transformQuery(getDatabaseMetadata(), query);
                normalizeQuery(query);
            } while (query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size() - 1).changed);
        }
        if (next != null) {
            next.handleQuery(query);
        }
    }

    public void normalizeQuery(Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        String parsedQuery = select.getText();
        int index = -1;
        do {
            index = query.getCurrentQuery().indexOf(' ', index + 1);
            if (index != -1) {
                parsedQuery = (parsedQuery.substring(0, index) + " " + parsedQuery.substring(index)).trim();
            }
        } while (index >= 0);
        query.setCurrentQuery(parsedQuery);
        query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size() - 1).setOutputQuery(parsedQuery);
    }

    public DatabaseMetadata getDatabaseMetadata() {
        return databaseMetadata;
    }

    public static TSqlParser parseQuery(String query) {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }
}
