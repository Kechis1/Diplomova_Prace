package DP.Transformations;

import DP.Database.*;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.TSqlParseWalker;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.List;

public class ExistsTransformation extends QueryHandler {
    private final String action = "ExistsTransformation";

    public ExistsTransformation(QueryHandler handler, DatabaseMetadata databaseMetadata) {
        super(handler, databaseMetadata);
    }

    @Override
    public boolean shouldTransform(Query query) {
        return query.getCurrentQuery().contains("EXISTS");
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        DatabaseTable fromTable = TSqlParseWalker.findFromTable(metadata, select).get(0);
        TSqlParseWalker.findTablesList(metadata, select);
        final List<ExistItem> existTables = new ArrayList<>();

        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition_and(TSqlParser.Search_condition_andContext ctx) {
                for (int i = 0; i < ctx.search_condition_not().size(); i++) {
                    if (ctx.search_condition_not(i).predicate() != null && ctx.search_condition_not(i).predicate().EXISTS() != null) {
                        ExistItem eItem = new ExistItem();
                        eItem.setNot(ctx.search_condition_not(i).NOT() != null);
                        eItem.setPredicateStartAt(ctx.search_condition_not(i).getStart().getStartIndex());
                        eItem.setPredicateStopAt(ctx.search_condition_not(i).getStop().getStopIndex());
                        TSqlParser.Query_specificationContext qSpecContext = ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification();
                        if (qSpecContext.FROM() != null && qSpecContext.table_sources().table_source().get(0).table_source_item_joined().table_source_item().table_name_with_hint() != null) {
                            eItem.setTable(metadata.findTable(qSpecContext.table_sources().table_source().get(0).table_source_item_joined().table_source_item().table_name_with_hint().table_name().table.getText(),
                                    qSpecContext.table_sources().table_source().get(0).table_source_item_joined().table_source_item().as_table_alias() == null
                                            ? null
                                            : qSpecContext.table_sources().table_source().get(0).table_source_item_joined().table_source_item().as_table_alias().getText()));
                        }
                        if (qSpecContext.WHERE() != null) {
                            List<ConditionItem> eConditions = new ArrayList<>();
                            for (TSqlParser.Search_condition_notContext scnContext : qSpecContext.search_condition().get(0).search_condition_and().get(0).search_condition_not()) {
                                ConditionItem item = new ConditionItem(scnContext.predicate().getStart().getStartIndex(),
                                        scnContext.predicate().getStop().getStopIndex() + 1,
                                        ConditionItem.findDataType(scnContext.predicate().expression().get(0)),
                                        ConditionItem.findSideValue(scnContext.predicate().expression().get(0)),
                                        ConditionItem.findDataType(scnContext.predicate().expression().get(1)),
                                        ConditionItem.findSideValue(scnContext.predicate().expression().get(1)),
                                        scnContext.predicate().comparison_operator() != null
                                                ? scnContext.predicate().comparison_operator().getText()
                                                : null
                                );
                                if (item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                                    item.setLeftSideColumnItem(ColumnItem.findOrCreate(metadata, scnContext, 0));
                                    item.setRightSideColumnItem(ColumnItem.findOrCreate(metadata, scnContext, 1));
                                }
                                eConditions.add(item);
                            }
                            eItem.setConditions(eConditions);
                        }

                        existTables.add(eItem);
                    }
                }
            }
        }, select);

        for (ExistItem exist : existTables) {
            if ((exist.isNot() && exist.getTable() != null && exist.getTable().isEmpty()) ||
                    (!exist.isNot() && (exist.getTable() == null ||
                            (!exist.getTable().isEmpty() &&
                                    (exist.getConditions() == null || exist.getConditions().size() == 0 ||
                                            (exist.getConditions().size() == 1 && ConditionItem.isComparingForeignKey(fromTable, exist.getTable(), exist.getConditions().get(0)))))))) {
                query.addTransform(new Transformation(query.getCurrentQuery(),
                        (query.getCurrentQuery().substring(0, exist.getPredicateStartAt()) + query.getCurrentQuery().substring(exist.getPredicateStopAt() + 1)).trim(),
                        UnnecessaryStatementException.messageUnnecessaryStatement + " EXISTS",
                        action,
                        true
                ));
                query.setCurrentQuery(query.getQueryTransforms().get(query.getQueryTransforms().size()-1).getOutputQuery());
                query.setChanged(true);
                return query;
            }
        }
        query.addTransform(new Transformation(query.getCurrentQuery(),
                query.getCurrentQuery(),
                "OK",
                action,
                false
        ));
        query.setChanged(false);
        return query;
    }
}
