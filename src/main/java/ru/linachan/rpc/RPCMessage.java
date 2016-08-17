package ru.linachan.rpc;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;

public class RPCMessage {

    private JSONObject messageData;

    @SuppressWarnings("unchecked")
    public RPCMessage() {
        this.messageData = new JSONObject();
        this.messageData.put("status", "ok");
    }

    public RPCMessage(String messageData) throws ParseException {
        this.messageData = (JSONObject) new JSONParser().parse(messageData);
    }

    public RPCMessage(JSONObject messageData) {
        this.messageData = messageData;
    }

    public RPCMessage(Map<?,?> messageData) {
        this.messageData = new JSONObject(messageData);
    }

    @SuppressWarnings("unchecked")
    public RPCMessage(Throwable exception) {
        this.messageData = new JSONObject();
        this.messageData.put("status", "error");
        this.messageData.put("errorType", exception.getClass().getSimpleName());
        this.messageData.put("errorMsg", exception.getMessage());
    }

    public JSONObject getData() {
        return messageData;
    }

    @SuppressWarnings("unchecked")
    public Object getData(String key, String defaultValue) {
        return messageData.getOrDefault(key, defaultValue);
    }

    public void setData(JSONObject messageData) {
        this.messageData = messageData;
    }

    @SuppressWarnings("unchecked")
    public void setData(String key, Object value) {
        this.messageData.put(key, value);
    }

    public String toJSON() {
        return messageData.toJSONString();
    }
}
