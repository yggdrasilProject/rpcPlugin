package ru.linachan.rpc;

import com.rabbitmq.client.*;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class RPCClient implements Runnable {

    private final Connection connection;
    private Channel channel;
    private String replyQueueName;
    private QueueingConsumer consumer;
    private Boolean isRunning = true;

    private final Map<String, RPCCallback> callbackMap = new HashMap<>();
    private final Map<String, Long> callbackTimeOut = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(RPCClient.class);

    public RPCClient(Connection rpcConnection) throws IOException {
        connection = rpcConnection;
        channel = connection.createChannel();

        replyQueueName = channel.queueDeclare().getQueue();

        consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);
    }

    public void call(String exchange, String key, String message, RPCCallback callback) throws IOException {
        String corrId = UUID.randomUUID().toString();

        AMQP.BasicProperties props = new AMQP.BasicProperties
            .Builder()
            .correlationId(corrId)
            .replyTo(replyQueueName)
            .build();

        channel.basicPublish(exchange, key, props, message.getBytes());

        callbackMap.put(corrId, callback);
        callbackTimeOut.put(corrId, System.currentTimeMillis());
    }

    public void start() {
        Thread clientThread = new Thread(this);
        clientThread.start();
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                if (callbackMap.containsKey(delivery.getProperties().getCorrelationId())) {
                    callbackMap.get(delivery.getProperties().getCorrelationId())
                        .callback(new RPCMessage(new String(delivery.getBody())));
                }
            } catch (ShutdownSignalException | ConsumerCancelledException e) {
                isRunning = false;
                break;
            } catch (ParseException | InterruptedException e) {
                logger.error("Unable to handle RPC message: {}", e.getMessage());
            }

            callbackTimeOut.keySet().stream()
                    .filter(corrId -> System.currentTimeMillis() - callbackTimeOut.get(corrId) > 60000)
                    .collect(Collectors.toList()).forEach(corrId -> {
                callbackMap.remove(corrId);
                callbackTimeOut.remove(corrId);
            });
        }
    }

    public void shutdown() throws IOException {
        isRunning = false;
        try {
            channel.queueDelete(replyQueueName);
            connection.close();
        } catch(AlreadyClosedException ignored) {}
    }
}
