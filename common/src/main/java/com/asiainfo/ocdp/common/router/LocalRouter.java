package com.asiainfo.ocdp.common.router;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/10/9
 * Time: 12:02
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class LocalRouter extends Router {



    public LocalRouter(String cacheManagers) {
        super(cacheManagers);
    }

    @Override
    public String getProxyHost(String host) {
        return this.PROXY_HOST_MAP.get(host);
    }
}
