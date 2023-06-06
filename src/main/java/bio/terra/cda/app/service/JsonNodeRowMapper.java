package bio.terra.cda.app.service;
// convenient Spring JDBC RowMapper for when you want the flexibility of Jackson's TreeModel API
// Note: Jackson can also serialize standard Java Collections (Maps and Lists) to JSON: if you don't need JsonNode,
//   it's simpler and more portable to have Spring JDBC simply return a Map or List<Map>.

//package org.springframework.jdbc.core;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.stereotype.Component;


public class JsonNodeRowMapper implements RowMapper<JsonNode> {

  ObjectMapper mapper;


  public JsonNodeRowMapper(ObjectMapper objectMapper) {
    this.mapper = objectMapper;
  }


  @Override
  public JsonNode mapRow(ResultSet rs, int rowNum) throws SQLException {
    try {
      String jsonstr = rs.getString(1);
      return mapper.readTree(jsonstr);
    } catch (JsonProcessingException ex) {
      throw new SQLException(ex);
    }
//    ObjectNode objectNode = mapper.createObjectNode();
//    ResultSetMetaData rsmd = rs.getMetaData();
//    int columnCount = rsmd.getColumnCount();
//    for (int index = 1; index <= columnCount; index++) {
//      String column = JdbcUtils.lookupColumnName(rsmd, index);
//      Object value = rs.getObject(column);
//      if (value == null) {
//        objectNode.putNull(column);
//      } else if (value instanceof Integer) {
//        objectNode.put(column, (Integer) value);
//      } else if (value instanceof String) {
//        objectNode.put(column, (String) value);
//      } else if (value instanceof Boolean) {
//        objectNode.put(column, (Boolean) value);
//      } else if (value instanceof Date) {
//        objectNode.put(column, ((Date) value).getTime());
//      } else if (value instanceof Long) {
//        objectNode.put(column, (Long) value);
//      } else if (value instanceof Double) {
//        objectNode.put(column, (Double) value);
//      } else if (value instanceof Float) {
//        objectNode.put(column, (Float) value);
//      } else if (value instanceof BigDecimal) {
//        objectNode.put(column, (BigDecimal) value);
//      } else if (value instanceof Byte) {
//        objectNode.put(column, (Byte) value);
//      } else if (value instanceof byte[]) {
//        objectNode.put(column, (byte[]) value);
//      } else {
//        throw new IllegalArgumentException("Unmappable object type: " + value.getClass());
//      }
//    }
//    return objectNode;
  }

}