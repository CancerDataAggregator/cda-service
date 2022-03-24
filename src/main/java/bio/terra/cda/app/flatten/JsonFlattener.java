package bio.terra.cda.app.flatten;

import bio.terra.cda.app.flatten.model.Row;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import java.io.*;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This utility converts a Json document in a 2D spreadsheet like CSV. This class is strongly
 * borrowed from https://github.com/opendevl/Json2Flat but modified to handle the ResultObject of
 * type List<Object> which reflects multiple rows that are coming out of BigQuery.
 *
 * <p>The field separators can be modified from ("/t" "," "|") etc by utilizing
 */
public class JsonFlattener {

  private List<Object[]> sheetMatrix = null;

  private List<String> pathList = null;

  private HashSet<String> primitivePath = null;
  private HashSet<String> primitiveUniquePath = null;
  private List<String> unique = null;

  private final String ARRAY_INDEX_REGEX = "(\\[[0-9]*\\])";
  private final String INDEX_REGEX_EOL = "(\\[[0-9]*\\]$)";

  private Pattern pattern = Pattern.compile(ARRAY_INDEX_REGEX, Pattern.MULTILINE);

  private JsonElement element = null;

  private OrderJson makeOrder = new OrderJson();

  public JsonFlattener() {}

  /**
   * This method does some pre processing and then calls make2D() to get the spreadsheet
   * representation of Json document.
   *
   * @return returns a JsonFlattener object
   */
  public JsonFlattener json2Sheet(String jsonString, String includeHeaders) {

    Configuration.setDefaults(
        new Configuration.Defaults() {
          private final JsonProvider jsonProvider = new JacksonJsonProvider();
          private final MappingProvider mappingProvider = new JacksonMappingProvider();

          // @Override
          public JsonProvider jsonProvider() {
            return jsonProvider;
          }

          // @Override
          public MappingProvider mappingProvider() {
            return mappingProvider;
          }

          // @Override
          public Set options() {
            return EnumSet.noneOf(Option.class);
          }
        });

    Configuration conf =
        Configuration.defaultConfiguration()
            .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
            .addOptions(Option.SUPPRESS_EXCEPTIONS);

    Configuration pathConf =
        Configuration.defaultConfiguration()
            .addOptions(Option.AS_PATH_LIST)
            .addOptions(Option.ALWAYS_RETURN_LIST);

    DocumentContext parse = null;

    sheetMatrix = new ArrayList<Object[]>();

    element = new JsonParser().parse(jsonString);

    pathList = JsonPath.using(pathConf).parse(jsonString).read("$..*");

    parse = JsonPath.using(conf).parse(jsonString);

    primitivePath = new LinkedHashSet<>();
    primitiveUniquePath = new LinkedHashSet<>();

    for (String o : pathList) {
      Object tmp = parse.read(o);

      if (tmp == null) {
        primitivePath.add(o);

      } else {
        String dataType = tmp.getClass().getSimpleName();
        if (dataType.equals("Boolean")
            || dataType.equals("Integer")
            || dataType.equals("String")
            || dataType.equals("Double")
            || dataType.equals("Long")) {
          primitivePath.add(o);
        } else {
          // it's not a primitive data type
        }
      }
    }

    for (String o : primitivePath) {

      Matcher m = pattern.matcher(o);

      if (m.find()) {
        String[] tmp = o.replace("$", "").split(INDEX_REGEX_EOL);
        tmp[0] = tmp[0].replaceAll(ARRAY_INDEX_REGEX, "");
        primitiveUniquePath.add(
            (tmp[0] + m.group())
                .replace("'][", ".")
                .replace("[", "")
                .replace("]", "")
                .replace("''", ".")
                .replace("'", ""));
      } else {
        primitiveUniquePath.add(
            o.replace("$", "")
                .replaceAll(ARRAY_INDEX_REGEX, "")
                .replace("[", "")
                .replace("]", "")
                .replace("''", ".")
                .replace("'", ""));
      }
    }

    unique = new ArrayList<String>(primitiveUniquePath);

    // choose to suppress the header row if we are aggregating multiple input results downstream.
    if (includeHeaders.equals("true")) {
      Object[] header = new Object[unique.size()];
      int i = 0;
      for (String o : unique) {
        header[i] = o;
        i++;
      }

      // header of the csv
      sheetMatrix.add(header);
    }

    // adding all the content of csv
    sheetMatrix.add(make2D(new Object[unique.size()], new Object[unique.size()], element, "$"));

    Object [] last = sheetMatrix.get(sheetMatrix.size() - 1);
    Object [] secondLast = sheetMatrix.get(sheetMatrix.size() - 2);

    boolean delete = true;

    for (Object o : last) {
      if (o != null) {
        delete = false;
        break;
      }
    }

    if (!delete) {
      delete = true;
      for (int DEL = 0; DEL < last.length; DEL++) {
        if (last[DEL] != null && !last[DEL].equals(secondLast[DEL])) {
          delete = false;
          break;
        }
      }
    }

    if (delete) sheetMatrix.remove(sheetMatrix.size() - 1);

    return this;
  }

  /**
   * This function transforms the JSON document to its equivalent 2D representation.
   *
   * @param current it's the logical current row of the Json being processed
   * @param old it keeps the old row which is always assigned to the current row.
   * @param element this keeps the part of json being parsed to 2D.
   * @param path this mantains the path of the Json element being processed.
   * @return
   */
  private Object[] make2D(Object[] current, Object[] old, JsonElement element, String path) {
    String tmpPath = null;
    current = old.clone();

    boolean gotArray = false;

    if (element.isJsonObject()) {

      /*
       * applying order to JSON. Order -
       * 1) JSON primitive
       * 2) JSON Array
       * 3) JSON Object
       */
      element = makeOrder.orderJson(element);

      for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {

        if (entry.getValue().isJsonPrimitive()) {
          tmpPath = path + "['" + entry.getKey() + "']";
          Matcher m = pattern.matcher(tmpPath);

          if (m.find()) {
            String[] tmp = tmpPath.replace("$", "").split(INDEX_REGEX_EOL);
            tmp[0] = tmp[0].replaceAll(ARRAY_INDEX_REGEX, "");
            tmpPath =
                ((tmp[0] + m.group())
                    .replace("'][", ".")
                    .replace("[", "")
                    .replace("]", "")
                    .replace("''", ".")
                    .replace("'", ""));
          } else {
            tmpPath =
                (tmpPath
                    .replace("$", "")
                    .replaceAll(ARRAY_INDEX_REGEX, "")
                    .replace("[", "")
                    .replace("]", "")
                    .replace("''", ".")
                    .replace("'", ""));
          }

          if (unique.contains(tmpPath)) {
            int index = unique.indexOf(tmpPath);
            current[index] = entry.getValue().getAsJsonPrimitive();
          }
          tmpPath = null;
        } else if (entry.getValue().isJsonObject()) {
          current =
              make2D(
                  new Object[unique.size()],
                  current,
                  entry.getValue().getAsJsonObject(),
                  path + "['" + entry.getKey() + "']");
        } else if (entry.getValue().isJsonArray()) {
          current =
              make2D(
                  new Object[unique.size()],
                  current,
                  entry.getValue().getAsJsonArray(),
                  path + "['" + entry.getKey() + "']");
        }
      }

    } else if (element.isJsonArray()) {
      int arrIndex = 0;

      for (JsonElement tmp : element.getAsJsonArray()) {

        if (tmp.isJsonPrimitive()) {
          tmpPath = path + "['" + arrIndex + "']";
          Matcher m = pattern.matcher(tmpPath);

          if (m.find()) {
            String [] tmp1 = tmpPath.replace("$", "").split(INDEX_REGEX_EOL);
            tmp1[0] = tmp1[0].replaceAll(ARRAY_INDEX_REGEX, "");
            tmpPath =
                ((tmp1[0] + m.group())
                    .replace("'][", ".")
                    .replace("[", "")
                    .replace("]", "")
                    .replace("''", ".")
                    .replace("'", ""));
          } else {
            tmpPath =
                (tmpPath
                    .replace("$", "")
                    .replaceAll(ARRAY_INDEX_REGEX, "")
                    .replace("[", "")
                    .replace("]", "")
                    .replace("''", ".")
                    .replace("'", ""));
          }

          if (unique.contains(tmpPath)) {
            int index = unique.indexOf(tmpPath);
            current[index] = tmp.getAsJsonPrimitive();
          }
          tmpPath = null;
        } else {
          if (tmp.isJsonObject()) {
            gotArray = isInnerArray(tmp);

            sheetMatrix.add(
                make2D(
                    new Object[unique.size()],
                    current,
                    tmp.getAsJsonObject(),
                    path + "[" + arrIndex + "]"));
            if (gotArray) {
              sheetMatrix.remove(sheetMatrix.size() - 1);
            }
          } else if (tmp.isJsonArray()) {
            make2D(
                new Object[unique.size()],
                current,
                tmp.getAsJsonArray(),
                path + "[" + arrIndex + "]");
          }
        }
        arrIndex++;
      }
    }
    return current;
  }

  /**
   * This method checks whether object inside an array contains an array or not.
   *
   * @param element it a Json object inside an array
   * @return it returns true if Json object inside an array contains an array or else false
   */
  private boolean isInnerArray(JsonElement element) {

    for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
      if (entry.getValue().isJsonArray()) {
        if (entry.getValue().getAsJsonArray().size() > 0)
          for (JsonElement checkPrimitive : entry.getValue().getAsJsonArray()) {

            if (checkPrimitive.isJsonObject()) {
              return true;
            }
          }
      }
    }
    return false;
  }

  /**
   * This method replaces the default header separator i.e. "." with a custom separator provided by
   * user.
   *
   * @param separator
   * @return JFlat
   * @throws Exception
   */
  public JsonFlattener headerSeparator(String separator) throws Exception {
    try {

      int sheetMatrixLen = this.sheetMatrix.get(0).length;

      for (int I = 0; I < sheetMatrixLen; I++) {

        this.sheetMatrix.get(0)[I] =
            this.sheetMatrix
                .get(0)[I]
                .toString()
                .replaceFirst("^\\/", "")
                .replaceAll(".", separator)
                .trim();
      }

    } catch (NullPointerException nullex) {
      throw new Exception(
          "The JSON document hasn't been transformed yet. Try using json2Sheet() before using headerSeparator");
    }
    return this;
  }

  /**
   * This method returns the sheet matrix.
   *
   * @return List<Object>
   */
  public List<Object[]> getJsonAsSheet() {
    return this.sheetMatrix;
  }

  /**
   * This method returns the spreadsheet as List<String>
   *
   * @return List<Row>
   */
  public List<String> getJsonAsSpreadsheet() {
    List<String> spreadsheet = new ArrayList<>();
    for (Object[] sheetRow : this.sheetMatrix) {
      spreadsheet.add(Row.toSpreadsheetRow(sheetRow));
    }
    return spreadsheet;
  }

  /**
   * This method returns unique fields of the json
   *
   * @return List<String>
   */
  public List<String> getUniqueFields() {
    return this.unique;
  }

  /**
   * This method writes the spreadsheet representation in csv format with ',' as default delimiter.
   *
   * @param destination it takes the destination path for the csv file.
   * @throws FileNotFoundException
   * @throws UnsupportedEncodingException
   */
  public void write2csv(String destination)
      throws FileNotFoundException, UnsupportedEncodingException {
    this.write2csv(destination, '|');
  }

  /**
   * This method writes the spreadsheet representation in csv format with custom delimiter set by
   * user.
   *
   * @param destination it takes the destination path for the csv file.
   * @param delimiter it represents the delimiter set by user.
   * @throws FileNotFoundException
   * @throws UnsupportedEncodingException
   */
  public void write2csv(String destination, char delimiter)
      throws FileNotFoundException, UnsupportedEncodingException {
    PrintWriter writer = new PrintWriter(new File(destination), "UTF-8");
    writer.write(write2csv(delimiter));
    writer.close();
  }

  /**
   * This method returns the spreadsheet representation in csv format as string with custom
   * delimiter set by user.
   *
   * @param delimiter it represents the delimiter set by user.
   */
  public String write2csv(char delimiter) {
    boolean comma = false;
    StringBuffer buffer = new StringBuffer();
    for (Object[] o : this.sheetMatrix) {
      comma = false;
      for (Object t : o) {
        if (t == null) {
          buffer.append(comma ? String.valueOf(delimiter) : "");
        } else {
          buffer.append(comma ? delimiter + t.toString() : t.toString());
        }
        if (!comma) comma = true;
      }
      buffer.append("\n");
    }
    return buffer.toString();
  }
}
