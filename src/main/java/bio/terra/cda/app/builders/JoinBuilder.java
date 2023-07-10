package bio.terra.cda.app.builders;

import bio.terra.cda.app.models.*;
import bio.terra.cda.app.util.SqlUtil;
import java.util.*;
import java.util.stream.Collectors;

public class JoinBuilder {
  private final DataSetInfo dataSetInfo;

  public JoinBuilder() {
    dataSetInfo = RdbmsSchema.getDataSetInfo();
  }

  public List<Join> getPath(
      String fromTablename, String toTablename, String toFieldname, SqlUtil.JoinType joinType) {
    return findPath(
            dataSetInfo.getTableInfo(fromTablename),
            dataSetInfo.getTableInfo(toTablename),
            toFieldname)
        .stream()
        .map(key -> new Join(key, joinType))
        .collect(Collectors.toList());
  }

  public List<Join> getPath(String fromTablename, String toTablename, String toFieldname) {
    List<ForeignKey> path =
        findPath(
            dataSetInfo.getTableInfo(fromTablename),
            dataSetInfo.getTableInfo(toTablename),
            toFieldname);
    return path.stream()
        .map(
            node -> {
              SqlUtil.JoinType jtype = SqlUtil.JoinType.LEFT;
              if (node.getFromTableName().equals(fromTablename)) {
                jtype = SqlUtil.JoinType.INNER;
              }
              return new Join(node, jtype);
            })
        .collect(Collectors.toList());
  }

  protected List<ForeignKey> findPath(TableInfo fromTable, TableInfo toTable, String toFieldname) {
    SortedSet<ForeignKey> newKeys = fromTable.getForeignKeys();
    Set<String> tablesVisited = new HashSet<>();
    tablesVisited.add(fromTable.getTableName());
    LinkedList<Deque<ForeignKey>> queue = new LinkedList<>();

    // initialize queue with all possible starting paths
    queue.addAll(
        newKeys.stream()
            .map(
                key -> {
                  Deque<ForeignKey> path = new ArrayDeque<>();
                  path.addFirst(key);
                  return path;
                })
            .collect(Collectors.toList()));

    List<List<ForeignKey>> goodPaths = new ArrayList<>();

    List<ForeignKey> goodPath = new ArrayList<>();
    while (queue.size() > 0) { // when we find a path, we break out of the loop

      Deque<ForeignKey> tryPath = queue.removeFirst();
      ForeignKey trykey = tryPath.getLast();
      if (foundMatch(trykey, toTable.getTableName())) {
        goodPath = new ArrayList<>(tryPath);
        goodPaths.add(goodPath);
        goodPath = new ArrayList<>();
        //        break;
      }
      ForeignKey foundMatch = getMatchingMappingFK(trykey, toTable.getTableName(), toFieldname);
      if (foundMatch != null) {
        tryPath.addLast(foundMatch);
        goodPath = new ArrayList<>(tryPath);
        goodPaths.add(goodPath);
        goodPath = new ArrayList<>();
        //        break;
        foundMatch = null;
      }

      // build next paths to try
      if (tablesVisited.contains(trykey.getDestinationTableName())) {
        continue;
      }
      tablesVisited.add(trykey.getDestinationTableName());

      // get keys from the destination table specified in the key we just checked
      SortedSet<ForeignKey> keysToAppend =
          dataSetInfo.getTableInfo(trykey.getDestinationTableName()).getForeignKeys();

      keysToAppend.forEach(
          key -> {
            if (!tablesVisited.contains(key.getDestinationTableName())
                || !tablesVisited.contains(key.getFromTableName())) {
              Deque<ForeignKey> newTryPath = new ArrayDeque<>(tryPath);
              newTryPath.addLast(key);
              queue.addLast(newTryPath);
            }
          });
    }
    // should this be a get or else throw exception? or maybe check the size of goodPaths first
    if (!goodPaths.isEmpty()) {
      goodPath = goodPaths.stream().min(Comparator.comparingInt(List::size)).get();
    }
    return goodPath;
  }

  protected boolean foundMatch(ForeignKey key, String toTable) {
    return key.getDestinationTableName().equals(toTable);
  }

  protected ForeignKey getMatchingMappingFK(ForeignKey key, String toTable, String toFieldname) {
    TableInfo destTable = dataSetInfo.getTableInfo(key.getDestinationTableName());
    if (destTable.isMapppingTable()) {
      // remove the FK that got us to this mapping table
      return destTable.getForeignKeys().stream()
          .filter(
              mk ->
                  !key.getFields()[0].equals(mk.getFromField())
                      && mk.getFields()[0].equals(toFieldname)
                      && mk.getDestinationTableName().equals(toTable))
          .findFirst()
          .orElse(null);
    }
    return null;
  }

  public List<Join> getJoinsFromQueryField(String fromTablename, QueryField queryField) {
    return getJoinsFromQueryField(fromTablename, queryField, SqlUtil.JoinType.LEFT);
  }

  public List<Join> getJoinsFromQueryField(
      String fromTablename, QueryField queryField, SqlUtil.JoinType joinType) {
    String toTablename = queryField.getTableName();
    return fromTablename.equals(toTablename)
        ? Collections.emptyList()
        : getPath(fromTablename, toTablename, queryField.getName(), joinType);
  }
}
