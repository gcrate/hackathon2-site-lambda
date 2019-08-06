package com.edo.hackathonsiteapi;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author grahamcrate
 */
public class ApiHandler extends AbstractHandler implements RequestStreamHandler {

    private static final String DYNAMO_TABLE = "hackathon2_registration";

    private static final String FIELD_TEAM = "team";
    private static final String FIELD_PIN = "pin";
    private static final String FIELD_M1 = "m1";
    private static final String FIELD_M2 = "m2";
    private static final String FIELD_M3 = "m3";
    private static final String FIELD_M4 = "m4";
    private static final String FIELD_M5 = "m5";
    private static final String FIELD_M1_EMAIL = "m1Email";
    private static final String FIELD_M2_EMAIL = "m2Email";
    private static final String FIELD_M3_EMAIL = "m3Email";
    private static final String FIELD_M4_EMAIL = "m4Email";
    private static final String FIELD_M5_EMAIL = "m5Email";

    private static final List<String> LIST_W_JUST_TEAM = new ArrayList<>();

    static {
        LIST_W_JUST_TEAM.add(FIELD_TEAM);
    }

    private final AmazonDynamoDB dynamoDbClient = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .build();

    private LambdaLogger logger;

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

        logger = context.getLogger();

        JSONObject jsonResponse = new JSONObject();
        
        try {
            InboundRequest iReq = getInboundRequest(inputStream);
            switch (iReq.getPath()) {
                case "/register":
                    String team = iReq.getBody().get(FIELD_TEAM) == null ? "" : iReq.getBody().get(FIELD_TEAM).toString();
                    String pin = iReq.getBody().get(FIELD_PIN) == null ? "" : iReq.getBody().get(FIELD_PIN).toString();
                    String m1 = iReq.getBody().get(FIELD_M1) == null ? "" : iReq.getBody().get(FIELD_M1).toString();
                    String m2 = iReq.getBody().get(FIELD_M2) == null ? "" : iReq.getBody().get(FIELD_M2).toString();
                    String m3 = iReq.getBody().get(FIELD_M3) == null ? "" : iReq.getBody().get(FIELD_M3).toString();
                    String m4 = iReq.getBody().get(FIELD_M4) == null ? "" : iReq.getBody().get(FIELD_M4).toString();
                    String m5 = iReq.getBody().get(FIELD_M5) == null ? "" : iReq.getBody().get(FIELD_M5).toString();
                    String m1Email = iReq.getBody().get(FIELD_M1_EMAIL) == null ? "" : iReq.getBody().get(FIELD_M1_EMAIL).toString();
                    String m2Email = iReq.getBody().get(FIELD_M2_EMAIL) == null ? "" : iReq.getBody().get(FIELD_M2_EMAIL).toString();
                    String m3Email = iReq.getBody().get(FIELD_M3_EMAIL) == null ? "" : iReq.getBody().get(FIELD_M3_EMAIL).toString();
                    String m4Email = iReq.getBody().get(FIELD_M4_EMAIL) == null ? "" : iReq.getBody().get(FIELD_M4_EMAIL).toString();
                    String m5Email = iReq.getBody().get(FIELD_M5_EMAIL) == null ? "" : iReq.getBody().get(FIELD_M5_EMAIL).toString();
                    jsonResponse = register(team, pin,
                            m1, m1Email,
                            m2, m2Email,
                            m3, m3Email,
                            m4, m4Email,
                            m5, m5Email);
                    break;
                case "/list-teams":
                    jsonResponse = createSuccessResponse(getAllTeamNames());
                    break;
                case "/load-team":
                    jsonResponse = loadTeam(iReq.getBody().get(FIELD_TEAM).toString(),
                            iReq.getBody().get(FIELD_PIN).toString());
                    break;
            }

        } catch (ParseException | IOException ex) {
            ex.printStackTrace();
            jsonResponse = createErrorResponse(ex.getMessage());
        }

        writeResponse(jsonResponse, outputStream);
    }
    
    private JSONObject loadTeam(String team, String pin) {
        Map<String, AttributeValue> existingTeam = getData(team);

        //if exists check pin
        if(existingTeam == null) {
            return createErrorResponse("Team Not Found", 404);
        } else {
            if(!existingTeam.get(FIELD_PIN).getS().equals(pin)) {
                return createErrorResponse("Invalid Pin", 401);
            }
        }
        
        //map and return
        JSONObject response = new JSONObject();
        
        existingTeam.keySet().stream()
                .forEach(x -> response.put(x, existingTeam.get(x).getS()));
        
        return createSuccessResponse(response.toJSONString());
    }

    private JSONObject register(
            String team, String pin,
            String m1, String m1Email,
            String m2, String m2Email,
            String m3, String m3Email,
            String m4, String m4Email,
            String m5, String m5Email) {
        //TODO: Validate input

        //Get
        Map<String, AttributeValue> existingTeam = getData(team);

        //if exists check pin
        if(existingTeam != null) {
            if(!existingTeam.get(FIELD_PIN).getS().equals(pin)) {
                return createErrorResponse("Invalid Pin", 401);
            }
        }
        
        HashMap<String, AttributeValue> itemVals
                = new HashMap<>();
        
        //Build item
        itemVals.put(FIELD_TEAM, new AttributeValue(team));
        itemVals.put(FIELD_PIN, new AttributeValue(pin));
        itemVals.put(FIELD_M1, new AttributeValue(m1));
        itemVals.put(FIELD_M2, new AttributeValue(m2));
        itemVals.put(FIELD_M3, new AttributeValue(m3));
        if(m4 != null && !m4.equals("")) {itemVals.put(FIELD_M4, new AttributeValue(m4)); };
        if(m5 != null && !m5.equals("")) { itemVals.put(FIELD_M5, new AttributeValue(m5)); };
        itemVals.put(FIELD_M1_EMAIL, new AttributeValue(m1Email));
        itemVals.put(FIELD_M2_EMAIL, new AttributeValue(m2Email));
        itemVals.put(FIELD_M3_EMAIL, new AttributeValue(m3Email));
        if(m4Email != null && !m4Email.equals("")) { itemVals.put(FIELD_M4_EMAIL, new AttributeValue(m4Email)); };
        if(m5Email != null && !m5Email.equals("")) { itemVals.put(FIELD_M5_EMAIL, new AttributeValue(m5Email)); };

        //save
        saveData(itemVals);
        return createEmptySuccessResponse();
    }

    private Map<String, AttributeValue> getData(String team) {
        HashMap<String, AttributeValue> keysAttr
                = new HashMap<>();
        keysAttr.put(FIELD_TEAM, new AttributeValue(team));

        return dynamoDbClient
                .getItem(DYNAMO_TABLE, keysAttr)
                .getItem();
    }

    private String getAllTeamNames() {
        JSONObject obj = new JSONObject();
        JSONArray names = new JSONArray();
        ScanResult sr = dynamoDbClient.scan(DYNAMO_TABLE, LIST_W_JUST_TEAM);
        sr.getItems().stream()
                .map(x -> x.get(FIELD_TEAM).getS())
                .sorted()
                .forEachOrdered(x -> names.add(x));
        return names.toJSONString();
    }

    private void saveData(Map<String, AttributeValue> data) {
        dynamoDbClient.putItem(DYNAMO_TABLE, data);
    }
}
