package bio.terra.cda.app.models;

import java.util.Objects;

public class View {
    public enum ViewType {
        WITH,
        INLINE
    }

    private final String viewName;
    private final String select;
    private final String fromClause;
    private final String whereClause;
    private final ViewType viewType;
    private final boolean includeAlias;

    public View(String viewName, ViewType viewType, String select, String fromClause, String whereClause, boolean includeAlias) {
        this.viewName = viewName;
        this.viewType = viewType;
        this.select = select;
        this.fromClause = fromClause;
        this.whereClause = whereClause;
        this.includeAlias = includeAlias;
    }

    public String getViewName() {
        return this.viewName;
    }

    public ViewType getViewType() {
        return this.viewType;
    }

    @Override
    public String toString() {
        String view = this.viewType == ViewType.WITH
                ? String.format("%1$s as (SELECT %2$s FROM %3$s", viewName, select, fromClause)
                : String.format("(SELECT %1$s FROM %2$s", select, fromClause);

        if (Objects.nonNull(this.whereClause) && !this.whereClause.equals("")) {
            view += String.format(" WHERE %s", this.whereClause);
        }

        view += ")";

        view += this.viewType == ViewType.INLINE && includeAlias
                ? " as %1$s"
                : "";

        return view;
    }
}
