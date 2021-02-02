package DP.Transformations;

import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public abstract class QueryHandler implements ITransformation {
    private final QueryHandler next;

    public QueryHandler(QueryHandler next) {
        this.next = next;
    }

    public void handleQuery(Query query) {
        if (next != null) {
            next.handleQuery(query);
        }
    }

    public static TSqlParser parseQuery(String query) {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }
}
