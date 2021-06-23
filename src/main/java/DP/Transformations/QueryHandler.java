package DP.Transformations;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.text.StringEscapeUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            } while (query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size() - 1).isChanged());
        }
        if (next != null) {
            next.handleQuery(query);
        }
    }

    private void normalizeQuery(Query query) {
        TSqlParser parser = parseQuery(query.getOutputQuery());
        ParseTree select = parser.select_statement();
        String parsedQuery = select.getText();
        parsedQuery = restoreConstants(query.getOriginalQuery(), parsedQuery);
        parsedQuery = restoreSpaces(query.getOutputQuery(), parsedQuery);
        query.setOutputQuery(parsedQuery);
        query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size() - 1).setOutputQuery(parsedQuery);
    }

    public static String restoreConstants(String from, String to) {
        String output = "";
        Pattern p = Pattern.compile("(?:^|\\s)'([^']*?)'(?:$|\\s)");
        Matcher m = p.matcher(from);
        while (m.find()) {
            output = to.replaceAll(Pattern.quote("'" + m.group(1).toUpperCase() + "'"), Matcher.quoteReplacement("'" + m.group(1) + "'"));
        }
        if (output.equals("")) return to;
        return output;
    }

    public static String restoreSpaces(String withSpaces, String withoutSpaces) {
        int index = -1;
        do {
            index = withSpaces.indexOf(' ', index + 1);
            if (index != -1) {
                try {
                    withoutSpaces = (withoutSpaces.substring(0, index) + " " + withoutSpaces.substring(index)).trim();
                } catch (StringIndexOutOfBoundsException ignored) {
                    return withSpaces;
                }
            }
        } while (index >= 0);
        return withoutSpaces;
    }

    public DatabaseMetadata getDatabaseMetadata() {
        return databaseMetadata;
    }

    public static TSqlParser parseQuery(String query) {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }
}
