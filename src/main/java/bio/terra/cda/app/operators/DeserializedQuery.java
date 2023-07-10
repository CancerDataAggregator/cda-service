package bio.terra.cda.app.operators;

import bio.terra.cda.generated.model.Query;
import java.util.List;

public class DeserializedQuery extends Query {
  private List<Select> selectList;
  private List<OrderBy> orderByList;
  private BasicOperator where;

  public List<Select> getSelectOperators() {
    return this.selectList;
  }

  public void setSelectOperators(List<Select> selectList) {
    this.selectList = selectList;
  }

  public List<OrderBy> getOrderByOperators() {
    return this.orderByList;
  }

  public void setOrderByList(List<OrderBy> orderByList) {
    this.orderByList = orderByList;
  }

  public BasicOperator getWhereOperator() {
    return this.where;
  }

  public void setWhereOperator(BasicOperator whereOperator) {
    this.where = whereOperator;
  }
}
