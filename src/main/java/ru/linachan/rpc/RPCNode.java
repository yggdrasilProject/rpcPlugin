package ru.linachan.rpc;

import org.json.simple.JSONObject;

import java.util.UUID;

public class RPCNode {

    private final UUID nodeUUID;
    private final String nodeType;
    private Long lastSeen;

    private final JSONObject nodeInfo;

    private final Long EXPIRATION_TIME = 60000L;

    public RPCNode(String nodeUUID, String nodeType, JSONObject nodeInfo) {
        this.nodeUUID = UUID.fromString(nodeUUID);
        this.nodeType = nodeType;
        this.nodeInfo = nodeInfo;
        this.lastSeen = System.currentTimeMillis();
    }

    public RPCNode(UUID nodeUUID, String nodeType, JSONObject nodeInfo) {
        this.nodeUUID = nodeUUID;
        this.nodeType = nodeType;
        this.nodeInfo = nodeInfo;
        this.lastSeen = System.currentTimeMillis();
    }

    public void update() {
        lastSeen = System.currentTimeMillis();
    }

    public boolean isExpired() {
        return System.currentTimeMillis() - lastSeen > EXPIRATION_TIME;
    }

    public UUID getNodeUUID() {
        return nodeUUID;
    }

    public String getNodeType() {
        return nodeType;
    }

    public Long getLastSeen() {
        return System.currentTimeMillis() - lastSeen;
    }

    @SuppressWarnings("unchecked")
    public Object getNodeInfo(String key, Object defaultValue) {
        if (nodeInfo != null)
            return nodeInfo.getOrDefault(key, defaultValue);

        return defaultValue;
    }
}
