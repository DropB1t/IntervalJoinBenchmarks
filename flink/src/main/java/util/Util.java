package util;

import java.io.File;
import java.io.IOException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;

public class Util {
    public static void appendJson(JsonNode jsonNode, String fileName) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectWriter objectWriter = objectMapper.writer(new DefaultPrettyPrinter());

        // Read existing JSON file
        File file = new File(fileName);
        ArrayNode jsonArray;
        if (file.exists()) {
            JsonNode rootNode = objectMapper.readTree(file);
            if (rootNode.getNodeType() == JsonNodeType.ARRAY) {
                jsonArray = (ArrayNode) rootNode;
            } else {
                jsonArray = objectMapper.createArrayNode();
            }
        } else {
            jsonArray = objectMapper.createArrayNode();
        }

        // Append new JSON object
        jsonArray.add(jsonNode);

        // Write JSON file
        objectWriter.writeValue(file, jsonArray);
    }
}
