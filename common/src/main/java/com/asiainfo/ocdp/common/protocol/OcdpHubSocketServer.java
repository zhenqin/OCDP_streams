package com.asiainfo.ocdp.common.protocol;

import com.asiainfo.ocdp.common.router.BanlanceRouter;
import com.asiainfo.ocdp.common.util.NetworkUtils;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/1/6
 * Time: 18:24
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class OcdpHubSocketServer extends SpecificResponder {




    protected String host = NetworkUtils.getLocalhostName();


    protected int port = 8440;


    protected Server server = null;



    protected boolean daemon = false;



    protected final OcdpHubProtocol ocdpHubProtocol;



    private static Logger LOG = LoggerFactory.getLogger(OcdpHubSocketServer.class);


    public OcdpHubSocketServer(Class<? extends OcdpHubProtocol> iface, Object impl) {
        super(iface, impl);
        this.ocdpHubProtocol = (OcdpHubProtocol)impl;
    }


    public OcdpHubSocketServer(String iface, Object impl) throws ClassNotFoundException {
        this((Class<? extends OcdpHubProtocol>) Class.forName(iface), impl);
    }




    public void serve() throws Exception {
        try {
            InetAddress inetAddress = InetAddress.getByName(host);
            server = new NettyServer(OcdpHubSocketServer.this, new InetSocketAddress(inetAddress, port));
            server.start();
            LOG.info("start avro nio socket at: " + inetAddress + ":" + port);

            if(!daemon) {
                server.join();
            }
        } catch (Exception e) {
            shutdown();
            throw e;
        }
    }



    public void shutdown() {
        if(server != null) {
            server.close();
        }
    }



    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }


    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }


    public static void main(String[] args) throws Exception {
        OcdpHubSocketServer server = new OcdpHubSocketServer(OcdpHubProtocol.class,
                new OcdpHubServerProtocolImpl(new BanlanceRouter("s1:6379,s2:6379,s3:6379,s4:6379,s5:6379")));
        server.setDaemon(false);
        server.setHost(NetworkUtils.getLocalhostName());
        server.setPort(3245);
        server.serve();
    }

}
