package DP.Database;

import DP.antlr4.tsql.parser.TSqlParser;

import java.util.*;
import java.util.stream.Collectors;

public class TableItem {
    String name;
    String alias;

    public TableItem(String name, String alias) {
        this.name = name;
        this.alias = alias;
    }

    public static TableItem create(TSqlParser.Table_source_itemContext ctx) {
        return new TableItem(ctx.table_name_with_hint().table_name().table.getText(),
                ctx.as_table_alias() != null
                        ? ctx.as_table_alias().getText()
                        : ctx.table_name_with_hint().table_name().table.getText());
    }

    public static List<TableItem> difference(List<TableItem> allItems, List<TableItem> filteredItems) {
        List<TableItem> newItems = new ArrayList<>(allItems);

        for (TableItem filteredItem: filteredItems) {
            newItems.removeIf(item -> item.equals(filteredItem));
        }

        return newItems;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableItem tableItem = (TableItem) o;
        return Objects.equals(name, tableItem.name) &&
                Objects.equals(alias, tableItem.alias);
    }

    @Override
    public String toString() {
        return "TableItem{" +
                "name='" + name + '\'' +
                ", alias='" + alias + '\'' +
                '}';
    }
}
