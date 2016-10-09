package com.asiainfo.ocdp.common.protocol;

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



    /**
     * LOG
     */
    protected static Logger LOG = LoggerFactory.getLogger(OcdpHubServerProtocolImpl.class);

    @Override
    public CharSequence getProxyHost(CharSequence host) throws AvroRemoteException {
        return null;
    }

    @Override
    public Map<CharSequence, CharSequence> getSysConf(CharSequence cond) throws AvroRemoteException {
        return null;
    }
}
