package ru.linachan.rpc;

import ru.linachan.yggdrasil.common.console.tables.Table;
import ru.linachan.yggdrasil.plugin.YggdrasilPluginManager;
import ru.linachan.yggdrasil.shell.YggdrasilShellCommand;
import ru.linachan.yggdrasil.shell.helpers.CommandAction;
import ru.linachan.yggdrasil.shell.helpers.ShellCommand;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@ShellCommand(command = "rpc", description = "RPC cluster management")
public class RPCCommand extends YggdrasilShellCommand implements RPCCallback {

    @Override
    protected void init() throws IOException {}

    private void rpcCall(RPCMessage message) throws IOException {
        try {
            RPCClient rpcClient = core.getManager(YggdrasilPluginManager.class).get(RPCPlugin.class).getRPCClient();
            rpcClient.call(RPCPlugin.RPC_EXCHANGE, "command", message.toJSON(), this);

            Thread.sleep(2000);

            rpcClient.shutdown();
        } catch (TimeoutException | InterruptedException e) {
            console.error("Unable to perform RPC rpcCall: {}", e.getMessage());
        }
    }

    @CommandAction("Shutdown entire cluster")
    public void shutdown() throws IOException {
        RPCMessage shutdownRequest = new RPCMessage();
        shutdownRequest.setData("action", "shutdown");
        rpcCall(shutdownRequest);
    }

    @CommandAction("Perform custom RPC call")
    public void call() throws IOException {
        RPCMessage callRequest = new RPCMessage();

        for (String key : kwargs.keySet()) {
            callRequest.setData(key, kwargs.get(key));
        }

        rpcCall(callRequest);
    }

    @CommandAction("Show cluster status")
    public void status() throws IOException {
        Table hosts = new Table("uuid", "type", "last_seen", "os_name", "os_arch", "os_version");

        for (RPCNode node : core.getManager(YggdrasilPluginManager.class).get(RPCPlugin.class).listNodes()) {
            hosts.addRow(
                node.getNodeUUID().toString(),
                node.getNodeType(),
                String.format("%0,3f", node.getLastSeen() / 1000.0),
                (String) node.getNodeInfo("osName", ""),
                (String) node.getNodeInfo("osArch", ""),
                (String) node.getNodeInfo("osVersion", "")
            );
        }

        console.writeTable(hosts);
    }

    @Override
    protected void onInterrupt() {

    }

    @Override
    public void callback(RPCMessage message) {
        try {
            console.writeLine(message.toJSON());
        } catch (IOException e) {
            logger.error("Unable to handle response response: {}", e.getMessage());
        }
    }
}
