package com.asiainfo.ocdp.common.protocol;


import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;


/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 19:26
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class OcdpHubClientProtocol<T> implements OcdpHubProtocol {


    private String host = "localhost";

    private int port = 8440;


    private final OcdpHubProtocol ocdpHubProtocol;


    private final Transceiver t;


    /**
     * LOG
     */
    private final static Logger LOG = LoggerFactory.getLogger(OcdpHubClientProtocol.class);


    public OcdpHubClientProtocol() {
        this("localhost", 8440);
    }


    public OcdpHubClientProtocol(String host, int port) {
        this.host = host;
        this.port = port;
        try {
            this.t = new NettyTransceiver(new InetSocketAddress(host, port));
            this.ocdpHubProtocol = SpecificRequestor.getClient(OcdpHubProtocol.class,
                    new SpecificRequestor(OcdpHubProtocol.class, t));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }



    @Override
    public CharSequence getProxyHost(CharSequence host) throws AvroRemoteException {
        return ocdpHubProtocol.getProxyHost(host);
    }

    @Override
    public Map<CharSequence, CharSequence> getSysConf(CharSequence cond) throws AvroRemoteException {
        return ocdpHubProtocol.getSysConf(cond);
    }


    public void close() throws IOException {
        t.close();
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


    public static void main(String[] args) throws IOException {
        OcdpHubClientProtocol clientProtocol = new OcdpHubClientProtocol("zhenqin-pro102", 3245);

        for (int i = 0; i < 100; i++) {
            System.out.println(clientProtocol.getProxyHost("hello-" + i));
        }

        clientProtocol.close();
    }

}
