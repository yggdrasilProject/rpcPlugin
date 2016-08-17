package ru.linachan.rpc;

import com.rabbitmq.client.*;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class RPCServer implements Runnable {

    private final Connection connection;
    private Channel channel;
    private QueueingConsumer consumer;

    private UUID queue;

    private final List<String[]> bounds = new ArrayList<>();

    private final Map<String, RPCHandler> handlers = new HashMap<>();
    private RPCHandler handler;

    private Thread serverThread;
    private boolean isRunning = true;

    private static final Logger logger = LoggerFactory.getLogger(RPCServer.class);

    public RPCServer(Connection rpcConnection) throws IOException {
        connection = rpcConnection;
        channel = connection.createChannel();

        queue = UUID.randomUUID();

        channel.queueDeclare(queue.toString(), false, false, true, null);
        channel.basicQos(1);

        consumer = new QueueingConsumer(channel);
        channel.basicConsume(queue.toString(), false, consumer);
    }

    public void bind(String rpcExchange, String rpcKey) throws IOException {
        channel.exchangeDeclare(rpcExchange, "fanout");
        channel.queueBind(queue.toString(), rpcExchange, rpcKey);

        bounds.add(new String[] {rpcExchange, rpcKey});
    }

    public void start() {
        serverThread = new Thread(this);
        serverThread.start();
    }

    public void setHandler(String action, RPCHandler handler) {
        this.handlers.put(action, handler);
    }

    public void setDefaultHandler(RPCHandler handler) {
        this.handler = handler;
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();

                AMQP.BasicProperties props = delivery.getProperties();
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(props.getCorrelationId())
                    .build();

                RPCMessage message = new RPCMessage(new String(delivery.getBody()));

                RPCMessage response = dispatch(message);

                if (response != null) {
                    channel.basicPublish("", props.getReplyTo(), replyProps, response.toJSON().getBytes());
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            } catch (IOException | ParseException e) {
                logger.error("Unable to process RPC call: {}", e.getMessage());
            } catch (ShutdownSignalException | InterruptedException ignored) {}
        }
    }

    @SuppressWarnings("unchecked")
    private RPCMessage dispatch(RPCMessage request) {
        String action = (String) request.getData().getOrDefault("action", "");
        RPCHandler handler = handlers.getOrDefault(action, this.handler);
        RPCMessage response = new RPCMessage();

        if (handler != null) {
            handler.handle(request, response);
        } else {
            response.setData("status", "error");
            response.setData("errorType", "UnknownAction");
            response.setData("errorMsg", String.format("Unknown action: '%s'", action));
        }

        return response;
    }

    public void shutdown() throws IOException {
        isRunning = false;
        try {
            for (String[] bound: bounds) {
                channel.queueUnbind(queue.toString(), bound[0], bound[1]);
            }

            serverThread.interrupt();
            connection.close();
        } catch(AlreadyClosedException ignored) {}
    }
}
