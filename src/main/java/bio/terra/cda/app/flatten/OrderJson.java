package bio.terra.cda.app.flatten;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderJson {
  private static final Logger logger = LoggerFactory.getLogger(OrderJson.class);
  Type type = new TypeToken<Map<String, Object>>() {}.getType();

  Map<String, Object> origMap = null;

  Map<String, Object> jsonPrimitive = null;
  Map<String, Object> jsonArray = null;
  Map<String, Object> jsonObject = null;

  Gson gson = null;

  public OrderJson() {
    gson = new Gson();
  }

  public JsonElement orderJson(JsonElement element) {

    // LinkedHashMap to maintain insertion order
    origMap = new LinkedHashMap<String, Object>();

    jsonPrimitive = new LinkedHashMap<String, Object>();
    jsonArray = new LinkedHashMap<String, Object>();
    jsonObject = new LinkedHashMap<String, Object>();

    // converting JsonElement to Map
    origMap = gson.fromJson(element, type);

    // Iterating the Map object to get type of Object
    for (Map.Entry<String, Object> entry : origMap.entrySet()) {

      try {
        // adding check if value of key in json is null
        if (entry.getValue() == null || entry.getValue().getClass().isInstance("ArrayList")) {

          // if Object is of type ArrayList push it to jsonArray Map
          jsonArray.put(entry.getKey(), entry.getValue());

        } else {

          // if Object is of type Primitive push to the jsonPrimitive Map
          jsonPrimitive.put(entry.getKey(), entry.getValue());
        }
      } catch (Exception ex) {
        logger.error(ex.getMessage());
      }
    }

    /* Keeping Order -
     * 		1) JSON primitive
     * 		2) JSON Array
     * 		3) JSON Object ( order of JSON Object is yet to be decided)
     * */

    // appending jsonArray map to jsonPrimitive map in order to mantain order.
    jsonPrimitive.putAll(jsonArray);

    // reconstructing the JSON from Map Objects and returning
    return gson.toJsonTree(jsonPrimitive, LinkedHashMap.class);
  }
}
