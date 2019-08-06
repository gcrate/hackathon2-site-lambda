package com.edo.hackathonsiteapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author grahamcrate
 */
public abstract class AbstractHandler {

    public static final JSONObject HEADERS = new JSONObject();

    static {
        HEADERS.put("Content-Type", "application/json");
        HEADERS.put("Access-Control-Allow-Origin", "*");
    }
    
    private JSONParser parser = new JSONParser();

    protected InboundRequest getInboundRequest(InputStream inputStream) throws IOException, ParseException{
        System.out.println("here");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        
        JSONObject event = (JSONObject) parser.parse(reader);
        String reqBody = event.get("body") == null || event.get("body").toString().equals("") ? "{}" :
                event.get("body").toString();
        System.out.println("body:" + reqBody);
        return new InboundRequest(event.get("path").toString(), (JSONObject) parser.parse(reqBody));
    }

    protected void writeResponse(JSONObject jsonResponse,
            OutputStream outputStream
    ) throws IOException {
        OutputStreamWriter writer
                = new OutputStreamWriter(outputStream,
                        "UTF-8");
        writer.write(jsonResponse
                .toJSONString());
        writer.close();

    }

    protected JSONObject createErrorResponse(String message) {
        return createErrorResponse(message, 500);
    }

    protected JSONObject createErrorResponse(String message, int statusCode) {
        return createResponse("{ \"error\": \"" + message + "\"}", statusCode);
    }

    protected JSONObject createEmptySuccessResponse() {
        return createResponse("{ \"success\": true }", 200);
    }
    
    protected JSONObject createSuccessResponse(String body) {
        return createResponse(body, 200);
    }
    
    protected JSONObject createSuccessResponse(Map<String,Object> body) {
        return createResponse(mapToJsonString(body), 200);
    }
    
    protected JSONObject createResponse(String body, int statusCode) {
        JSONObject response = new JSONObject();
        response.put("statusCode", statusCode);
        response.put("headers", HEADERS);
        response.put("body", body);
        return response;
    }
    
    private String mapToJsonString(Map<String,Object> input) {
        return new JSONObject(input).toJSONString();
    }
}
