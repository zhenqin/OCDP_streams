package com.asiainfo.ocdp.common.router;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/10/9
 * Time: 12:46
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class LocalOrBanlanceRouter extends BanlanceRouter {


    public LocalOrBanlanceRouter(String cacheManagers) {
        super(cacheManagers);
    }


    @Override
    public String getProxyHost(String host) {
        String codisHost = this.PROXY_HOST_MAP.get(host);
        if (codisHost == null) {
            return super.getProxyHost(host);
        }
        return codisHost;
    }
}
