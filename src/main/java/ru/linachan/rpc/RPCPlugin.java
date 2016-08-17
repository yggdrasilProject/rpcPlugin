package ru.linachan.rpc;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownNotifier;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.linachan.yggdrasil.plugin.YggdrasilPlugin;
import ru.linachan.yggdrasil.plugin.YggdrasilPluginManager;
import ru.linachan.yggdrasil.plugin.helpers.Plugin;
import ru.linachan.yggdrasil.scheduler.YggdrasilRunnable;
import ru.linachan.yggdrasil.scheduler.YggdrasilTask;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Plugin(name = "RPC", description = "Provides ability to launch RPC services")
public class RPCPlugin implements RPCCallback, YggdrasilPlugin {

    private ConnectionFactory rpcConnectionFactory;
    private List<Connection> rpcConnectionList;

    private RPCServer rpcServer;
    private RPCClient rpcClient;

    private final List<RPCNode> nodes = new ArrayList<>();

    private final Logger logger = LoggerFactory.getLogger(RPCPlugin.class);

    private final UUID NODE_UUID = UUID.randomUUID();
    public static final String RPC_EXCHANGE = "yggdrasil";
    public static final String NODE_TYPE = "controller";

    @Override
    @SuppressWarnings("unchecked")
    public void onInit() {
        rpcConnectionFactory = new ConnectionFactory();
        rpcConnectionList = new ArrayList<>();

        rpcConnectionFactory.setHost(core.getConfig().getString("rabbitmq.host", "localhost"));
        rpcConnectionFactory.setPort(core.getConfig().getInt("rabbitmq.port", 5672));
        rpcConnectionFactory.setUsername(core.getConfig().getString("rabbitmq.user", "guest"));
        rpcConnectionFactory.setPassword(core.getConfig().getString("rabbitmq.password", "guest"));
        rpcConnectionFactory.setVirtualHost(core.getConfig().getString("rabbitmq.virtual_host", "/"));

        try {
            rpcServer = getRPCServer();
            rpcClient = getRPCClient();

            rpcServer.bind(RPC_EXCHANGE, "discover");
            rpcServer.bind(RPC_EXCHANGE, "command");
            rpcServer.bind(RPC_EXCHANGE, NODE_UUID.toString());

            rpcServer.setDefaultHandler((request, response) -> {
                response.setData("status", "error");
                response.setData("errorType", "UnknownAction");
                response.setData("errorMsg", String.format(
                    "Unknown action: '%s'", request.getData().getOrDefault("action", ""))
                );
            });

            rpcServer.setHandler("discover", (request, response) -> {
                response.setData("nodeUUID", NODE_UUID.toString());
                response.setData("nodeType", NODE_TYPE);

                JSONObject nodeInfo = new JSONObject();

                nodeInfo.put("osName", System.getProperty("os.name"));
                nodeInfo.put("osArch", System.getProperty("os.arch"));
                nodeInfo.put("osVersion", System.getProperty("os.version"));

                nodeInfo.put("userName", System.getProperty("user.name"));
                nodeInfo.put("userHome", System.getProperty("user.home"));
                nodeInfo.put("workingDir", System.getProperty("user.dir"));

                response.setData("nodeInfo", nodeInfo);
            });

            rpcServer.setHandler("shutdown", (request, response) -> core.shutdown());
        } catch (IOException | TimeoutException e) {
            logger.error("Unable to initialize cluster: {}", e.getMessage());
        }

        rpcServer.start();
        rpcClient.start();

        core.getScheduler().scheduleTask(new YggdrasilTask("nodeHeartBeat", new YggdrasilRunnable() {
            @Override
            public void run() {
                try {
                    RPCMessage discoveryRequest = new RPCMessage();
                    discoveryRequest.setData("action", "discover");
                    discoveryRequest.setData("nodeUUID", NODE_UUID.toString());
                    rpcClient.call(RPC_EXCHANGE, "discover", discoveryRequest.toJSON(), core.getManager(
                        YggdrasilPluginManager.class
                    ).get(RPCPlugin.class));
                } catch (IOException e) {
                    logger.error("Unable to perform RPC rpcCall: {}", e.getMessage());
                }
            }

            @Override
            public void onCancel() {}
        }, 1, 15, TimeUnit.SECONDS));

        core.getScheduler().scheduleTask(new YggdrasilTask("nodeMonitor", new YggdrasilRunnable() {
            @Override
            public void run() {
                nodes.stream()
                    .filter(RPCNode::isExpired)
                    .collect(Collectors.toList())
                    .forEach(node -> {
                        nodes.remove(node);
                        logger.info("Node disconnected: {}", node.getNodeUUID().toString());
                    });
            }

            @Override
            public void onCancel() {}
        }, 20, 10, TimeUnit.SECONDS));
    }

    @Override
    public void onShutdown() {
        rpcConnectionList.stream()
            .collect(Collectors.toList()).stream()
            .filter(ShutdownNotifier::isOpen)
            .forEach(connection -> {
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.warn("Unable to close RabbitMQ connection: {}", e.getMessage());
                }
                rpcConnectionList.remove(connection);
            });

        try {
            rpcClient.shutdown();
            rpcServer.shutdown();
        } catch (IOException e) {
            logger.error("Unable to shutdown cluster: {}", e.getMessage());
        }
    }

    public Connection getConnection() throws IOException, TimeoutException {
        Connection rpcConnection = rpcConnectionFactory.newConnection();
        rpcConnectionList.add(rpcConnection);
        return rpcConnection;
    }

    public RPCServer getRPCServer() throws IOException, TimeoutException {
        return new RPCServer(getConnection());
    }

    public RPCClient getRPCClient() throws IOException, TimeoutException {
        return new RPCClient(getConnection());
    }

    @Override
    public void callback(RPCMessage message) {
        String nodeUUID = (String) message.getData("nodeUUID", null);
        String nodeType = (String) message.getData("nodeType", null);
        JSONObject nodeInfo = (JSONObject) message.getData("nodeInfo", null);

        if (nodeUUID != null) {
            Optional<RPCNode> nodeOptional = nodes.stream()
                .filter(node -> node.getNodeUUID().equals(UUID.fromString(nodeUUID)))
                .findFirst();

            if (nodeOptional.isPresent()) {
                nodeOptional.get().update();
            } else {
                logger.info("New node discovered: {}", nodeUUID);
                nodes.add(new RPCNode(nodeUUID, nodeType, nodeInfo));
            }
        }
    }

    public List<RPCNode> listNodes() {
        return nodes;
    }

    public void setHandler(String action, RPCHandler handler) {
        rpcServer.setHandler(action, handler);
    }
}
