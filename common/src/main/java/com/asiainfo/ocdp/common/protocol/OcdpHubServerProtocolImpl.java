package com.asiainfo.ocdp.common.protocol;

import com.asiainfo.ocdp.common.router.Router;
import org.apache.avro.AvroRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/7/18
 * Time: 13:05
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class OcdpHubServerProtocolImpl implements OcdpHubProtocol {


    private final Router router;


    public OcdpHubServerProtocolImpl(Router router) {
        this.router = router;
    }

    /**
     * LOG
     */
    protected static Logger LOG = LoggerFactory.getLogger(OcdpHubServerProtocolImpl.class);

    @Override
    public CharSequence getProxyHost(CharSequence host) throws AvroRemoteException {
        return router.getProxyHost(host.toString());
    }

    @Override
    public Map<CharSequence, CharSequence> getSysConf(CharSequence cond) throws AvroRemoteException {
        return null;
    }


    public Router getRouter() {
        return router;
    }
}
