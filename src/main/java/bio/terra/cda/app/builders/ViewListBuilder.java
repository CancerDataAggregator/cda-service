package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.DataSetInfo;
import bio.terra.cda.app.models.RdbmsSchema;
import bio.terra.cda.app.models.View;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ViewListBuilder<V extends View, T extends ViewBuilder> {
  RdbmsSchema rdbmsSchema;

  private List<V> viewList;
  private final DataSetInfo dataSetInfo;
  private final Class<? extends ViewBuilder> viewBuilderClass;

  public ViewListBuilder(
      Class<? extends ViewBuilder> viewBuilderClass) {
    this.viewBuilderClass = viewBuilderClass;
    this.dataSetInfo = rdbmsSchema.getDataSetInfo();

    this.viewList = new ArrayList<>();
  }

  public void clearViews() {
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
      constructor = viewBuilderClass.getConstructor();
      return constructor.newInstance();
    } catch (NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException e) {
      return null;
    }
  }

  public boolean hasAny() {
    return !this.viewList.isEmpty();
  }

  public List<V> build() {
    return this.viewList;
  }
}
