package ru.linachan.rpc;

@FunctionalInterface
public interface RPCHandler {

    void handle(RPCMessage request, RPCMessage response);
}
