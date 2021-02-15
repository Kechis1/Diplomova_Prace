package DP.Transformations;

import DP.Database.DatabaseMetadata;
import DP.antlr4.tsql.parser.TSqlLexer;
import DP.antlr4.tsql.parser.TSqlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.List;
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
            } while (query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size() - 1).changed);
        }
        if (next != null) {
            next.handleQuery(query);
        }
    }

    public void normalizeQuery(Query query) {
        String validatedQuery = getRidOfInvalidCommas(query.getCurrentQuery());
        TSqlParser parser = parseQuery(validatedQuery);
        ParseTree select = parser.select_statement();
        String parsedQuery = select.getText();
        int index = -1;
        do {
            index = validatedQuery.indexOf(' ', index + 1);
            if (index != -1) {
                parsedQuery = (parsedQuery.substring(0, index) + " " + parsedQuery.substring(index)).trim();
            }
        } while (index >= 0);
        query.setCurrentQuery(parsedQuery);
        query.getQueryTransforms().get(query.getCurrentRunNumber()).get(query.getQueryTransforms().get(query.getCurrentRunNumber()).size() - 1).setOutputQuery(parsedQuery);
    }

    private String getRidOfInvalidCommas(String query) {
        List<String> originals = new ArrayList<>();
        List<String> modified = new ArrayList<>();
        Matcher m = Pattern.compile("(?<=SELECT)(.+?)(?=FROM)")
                .matcher(query);
        while (m.find()) {
            modified.add(m.group().replaceAll("\\s+",""));
            originals.add(m.group().trim());
        }

        for (int k = 0; k < modified.size(); k++) {
            for (int i = 0; i < modified.get(k).length() - 1; i++) {
                if (modified.get(k).charAt(i+1) == ',') {
                    if (modified.get(k).charAt(i) == ',') {
                        query = query.replace(originals.get(k), modified.get(k).substring(0, i) + modified.get(k).substring(i+1));
                    } else if (modified.get(k).length() - 1 == i + 1) {
                        query = query.replace(originals.get(k), modified.get(k).substring(0, i+1));
                    }
                }
            }
        }
        return query;
    }

    public DatabaseMetadata getDatabaseMetadata() {
        return databaseMetadata;
    }

    public static TSqlParser parseQuery(String query) {
        TSqlLexer lexer = new TSqlLexer(CharStreams.fromString(query.toUpperCase()));
        return new TSqlParser(new CommonTokenStream(lexer));
    }
}
