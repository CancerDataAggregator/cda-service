package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.View;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ViewListBuilder<V extends View, T extends ViewBuilder> {
    private final List<V> viewList;
    private final DataSetInfo dataSetInfo;
    private final String project;
    private final Class<? extends ViewBuilder> viewBuilderClass;

    public ViewListBuilder(Class<? extends ViewBuilder> viewBuilderClass, DataSetInfo dataSetInfo, String project) {
        this.viewBuilderClass = viewBuilderClass;
        this.dataSetInfo = dataSetInfo;
        this.project = project;

        this.viewList = new ArrayList<>();
    }

    public void addView(V view) {
        if (this.viewList.stream().noneMatch(v -> v.getViewName().equals(view.getViewName()))) {
            this.viewList.add(view);
        }
    }

    public boolean contains(String viewName) {
        return this.viewList.stream().anyMatch(v -> v.getViewName().equals(viewName));
    }

    public ViewBuilder getViewBuilder() {
        Constructor<? extends ViewBuilder> constructor = null;

        try {
            constructor = viewBuilderClass.getConstructor(DataSetInfo.class, String.class);
            return constructor.newInstance(this.dataSetInfo, this.project);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

        return null;
    }

    public boolean hasAny() {
        return !this.viewList.isEmpty();
    }

    public List<V> build() {
        return this.viewList;
    }
}
