package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.*;
import com.google.api.client.util.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;

public class ViewBuilder {
  @Autowired RdbmsSchema rdbmsSchema;

  private final DataSetInfo dataSetInfo;
  private String whereClause;
  private TableInfo table;
  private String fromClause;
  private String fromAlias;
  private List<Select> selectList;
  private String viewName;
  private View.ViewType viewType;
  private boolean includeAlias;

  private String groupBy;

  public ViewBuilder() {
    this.dataSetInfo = rdbmsSchema.getDataSetInfo();
    this.selectList = new ArrayList<>();
  }

  public ViewBuilder setTable(TableInfo table) {
    this.table = table;
    return this;
  }

  public ViewBuilder setFromClause(String from, String fromAlias) {
    this.fromClause = String.format("%s as %s", from, fromAlias);
    return this;
  }

  public ViewBuilder setFromClause(String formattedFromClause) {
    this.fromClause = formattedFromClause;
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

  public ViewBuilder setGroupBy(String groupBy) {
    this.groupBy = groupBy;
    return this;
  }

  public ViewBuilder addSelect(Select select) {
    this.selectList.add(select);
    return this;
  }

  public View build() {
    String formattedFromClause =
        Strings.isNullOrEmpty(fromClause)
            ? Stream.of(
                    String.format(
                        "%s AS %s", table.getTableName(), table.getTableAlias(this.dataSetInfo)))
                .collect(Collectors.joining(" "))
            : fromClause;

    String select =
        !selectList.isEmpty()
            ? selectList.stream().map(Select::toString).collect(Collectors.joining(", "))
            : String.format(
                "%s.*",
                Strings.isNullOrEmpty(fromAlias) ? table.getTableAlias(dataSetInfo) : fromAlias);

    return new View(
        this.viewName,
        this.viewType,
        select,
        formattedFromClause,
        this.whereClause,
        this.groupBy,
        this.includeAlias);
  }
}
