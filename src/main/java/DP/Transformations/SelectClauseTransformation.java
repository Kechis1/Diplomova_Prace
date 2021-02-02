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

public class SelectClauseTransformation extends QueryHandler {
    private final String action = "SelectClauseTransformation";

    public SelectClauseTransformation(QueryHandler handler) {
        super(handler);
    }

    @Override
    public void handleQuery(Query query) {
        super.handleQuery(query);
    }

    @Override
    public Query transformQuery(final DatabaseMetadata metadata, Query query) {
        TSqlParser parser = parseQuery(query.getCurrentQuery());
        ParseTree select = parser.select_statement();
        final List<DatabaseTable> allTables = TSqlParseWalker.findTablesList(metadata, select);
        final List<ExistItem> foundExistsNotConstant = new ArrayList<>();
        final List<Boolean> foundUnion = new ArrayList<>();
        DatabaseMetadata metadataWithTables = metadata.withTables(allTables);
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterSearch_condition_and(TSqlParser.Search_condition_andContext ctx) {
                for (int i = 0; i < ctx.search_condition_not().size(); i++) {
                    if (ctx.search_condition_not(i).predicate() != null && ctx.search_condition_not(i).predicate().EXISTS() != null && ctx.search_condition_not(i).predicate().subquery() != null &&
                            (ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().select_list_elem().size() > 1 ||
                                    ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().select_list_elem().get(0).expression_elem() == null ||
                                    ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().select_list_elem().get(0).expression_elem().expression().primitive_expression().constant() == null)) {
                        foundExistsNotConstant.add(new ExistItem(ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().getStart().getStartIndex(),
                                ctx.search_condition_not(i).predicate().subquery().select_statement().query_expression().query_specification().select_list().getStop().getStopIndex()));
                    }
                }
            }

            @Override
            public void enterSql_union(TSqlParser.Sql_unionContext ctx) {
                foundUnion.add(true);
            }
        }, select);

        if (!foundExistsNotConstant.isEmpty()) {
            query.addTransform(new Transformation(query.getCurrentQuery(),
                    (query.getCurrentQuery().substring(0, foundExistsNotConstant.get(0).getSelectListStartAt()) + "1" + query.getCurrentQuery().substring(foundExistsNotConstant.get(0).getSelectListStopAt() + 1)).trim(),
                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                    action,
                    true
            ));
            query.setCurrentQuery(query.getQueryTransforms().get(query.getQueryTransforms().size() - 1).getOutputQuery());
            query.setChanged(true);
            return query;
        }

        final List<ColumnItem> allColumnsInSelect = TSqlParseWalker.findColumnsInSelect(metadataWithTables, select);

        ColumnItem equals = ColumnItem.duplicatesExists(allColumnsInSelect);
        if (equals != null) {
            query.addTransform(new Transformation(query.getCurrentQuery(),
                    (query.getCurrentQuery().substring(0, equals.getStartAt()) + query.getCurrentQuery().substring(equals.getStopAt() + 1)).trim(),
                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE",
                    action,
                    true
            ));
            query.setCurrentQuery(query.getQueryTransforms().get(query.getQueryTransforms().size() - 1).getOutputQuery());
            query.setChanged(true);
            return query;
        }

        for (ColumnItem item : allColumnsInSelect) {
            if (item.isConstant() && foundUnion.isEmpty()) {
                query.addTransform(new Transformation(query.getCurrentQuery(),
                        (query.getCurrentQuery().substring(0, item.getStartAt()) + query.getCurrentQuery().substring(item.getStopAt() + 1)).trim(),
                        UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE",
                        action,
                        true
                ));
                query.setCurrentQuery(query.getQueryTransforms().get(query.getQueryTransforms().size() - 1).getOutputQuery());
                query.setChanged(true);
                return query;
            }
        }

        final List<ConditionItem> conditions = TSqlParseWalker.findConditions(metadataWithTables, select);
        final List<ConditionItem> uniqueAttributesConditions = ConditionItem.removeMultipleAttributeConditions(conditions);

        for (ConditionItem item : conditions) {
            if (item.getOperator().equals("=") && item.getLeftSideDataType() == ConditionDataType.COLUMN && item.getRightSideDataType() == ConditionDataType.COLUMN) {
                int bothInSelect = -1;
                List<ColumnItem> inSelect = new ArrayList<>();
                ColumnItem columnInSelect = null;
                for (ColumnItem column : allColumnsInSelect) {
                    if (column.equals(item.getLeftSideColumnItem()) || column.equals(item.getRightSideColumnItem())) {
                        columnInSelect = column;
                        if (column.equals(item.getLeftSideColumnItem())) {
                            inSelect.add(item.getRightSideColumnItem());
                        } else {
                            inSelect.add(item.getLeftSideColumnItem());
                        }
                        bothInSelect++;
                    }
                    if (bothInSelect == 1) {
                        query.addTransform(new Transformation(query.getCurrentQuery(),
                                (query.getCurrentQuery().substring(0, column.getStartAt()) + query.getCurrentQuery().substring(column.getStopAt() + 1)).trim(),
                                UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE",
                                action,
                                true
                        ));
                        query.setCurrentQuery(query.getQueryTransforms().get(query.getQueryTransforms().size() - 1).getOutputQuery());
                        query.setChanged(true);
                        return query;
                    }
                }
                if (bothInSelect == 0) {
                    for (ConditionItem condition : conditions) {
                        if (item.getOperator().equals("=") &&
                                ((condition.getLeftSideColumnItem().equals(inSelect.get(0)) && condition.getRightSideDataType() != ConditionDataType.COLUMN) ||
                                        (condition.getRightSideColumnItem().equals(inSelect.get(0)) && condition.getLeftSideDataType() != ConditionDataType.COLUMN))) {
                            String value = condition.getLeftSideDataType() != ConditionDataType.COLUMN ? condition.getLeftSideValue() : condition.getRightSideValue();
                            query.addTransform(new Transformation(query.getCurrentQuery(),
                                    (query.getCurrentQuery().substring(0, columnInSelect.getStartAt()) + value + " AS " + columnInSelect.getName() + query.getCurrentQuery().substring(columnInSelect.getStopAt() + 1)).trim(),
                                    UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                                    action,
                                    true
                            ));
                            query.setCurrentQuery(query.getQueryTransforms().get(query.getQueryTransforms().size() - 1).getOutputQuery());
                            query.setChanged(true);
                            return query;
                        }
                    }
                }
            }
        }

        for (ConditionItem item : uniqueAttributesConditions) {
            if (item.getOperator().equals("=") && (item.getLeftSideDataType() == ConditionDataType.COLUMN || item.getRightSideDataType() == ConditionDataType.COLUMN)) {
                for (ColumnItem column : allColumnsInSelect) {
                    if ((item.getLeftSideDataType() == ConditionDataType.COLUMN && column.equals(item.getLeftSideColumnItem()) && item.getRightSideDataType() != ConditionDataType.COLUMN) ||
                            (item.getRightSideDataType() == ConditionDataType.COLUMN && column.equals(item.getRightSideColumnItem()) && item.getLeftSideDataType() != ConditionDataType.COLUMN)) {
                        String value = item.getLeftSideDataType() != ConditionDataType.COLUMN ? item.getLeftSideValue() : item.getRightSideValue();
                        query.addTransform(new Transformation(query.getCurrentQuery(),
                                (query.getCurrentQuery().substring(0, column.getStartAt()) + value + " AS " + column.getName() + query.getCurrentQuery().substring(column.getStopAt() + 1)).trim(),
                                UnnecessaryStatementException.messageUnnecessarySelectClause + " ATTRIBUTE " + UnnecessaryStatementException.messageCanBeRewrittenTo + " CONSTANT",
                                action,
                                true
                        ));
                        query.setCurrentQuery(query.getQueryTransforms().get(query.getQueryTransforms().size() - 1).getOutputQuery());
                        query.setChanged(true);
                        return query;
                    }
                }
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
