package com.edo.hackathonsiteapi;

import org.json.simple.JSONObject;

/**
 *
 * @author grahamcrate
 */
public class InboundRequest {
    private String path;
    private JSONObject body;

    public InboundRequest(String path, JSONObject body) {
        this.path = path;
        this.body = body;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public JSONObject getBody() {
        return body;
    }

    public void setBody(JSONObject body) {
        this.body = body;
    }
    
    
}
