package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.Select;
import bio.terra.cda.app.models.TableInfo;
import bio.terra.cda.app.models.Unnest;
import bio.terra.cda.app.models.View;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ViewBuilder {
    private final String project;
    private final DataSetInfo dataSetInfo;
    private List<Unnest> unnestList;
    private String whereClause;
    private TableInfo table;
    private List<Select> selectList;
    private String viewName;
    private View.ViewType viewType;
    private boolean includeAlias;

    public ViewBuilder(
            DataSetInfo dataSetInfo,
            String project) {
        this.dataSetInfo = dataSetInfo;
        this.project = project;

        this.unnestList = new ArrayList<>();
        this.selectList = new ArrayList<>();
    }

    public ViewBuilder setTable(TableInfo table) {
        this.table = table;
        return this;
    }

    public ViewBuilder setViewName(String viewName) {
        this.viewName = viewName;
        return this;
    }

    public ViewBuilder setViewType(View.ViewType viewType) {
        this.viewType = viewType;
        return this;
    }

    public ViewBuilder setWhereClause(String whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    public ViewBuilder setIncludeAlias(boolean includeAlias) {
        this.includeAlias = includeAlias;
        return this;
    }

    public ViewBuilder addUnnests(Stream<Unnest> unnestStream) {
        this.unnestList = Stream.concat(this.unnestList.stream(), unnestStream).collect(Collectors.toList());
        return this;
    }

    public ViewBuilder addSelect(Select select) {
        this.selectList.add(select);
        return this;
    }

    public View build() {
        String fromClause =
                Stream.concat(
                        Stream.of(
                            String.format(
                                    "%s.%s AS %s", project, table.getTableName(), table.getTableAlias(this.dataSetInfo))),
                        this.unnestList.stream().map(Unnest::toString))
                        .collect(Collectors.joining(" "));

        String select = !selectList.isEmpty()
                ? selectList.stream().map(Select::toString).collect(Collectors.joining(", "))
                : String.format("%s.*", table.getTableAlias(this.dataSetInfo));

        return new View(
                this.viewName,
                this.viewType,
                select,
                fromClause,
                this.whereClause,
                this.includeAlias);
    }
}
