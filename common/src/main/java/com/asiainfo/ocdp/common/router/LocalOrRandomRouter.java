package com.asiainfo.ocdp.common.router;

import java.util.Random;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/10/9
 * Time: 12:05
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class LocalOrRandomRouter  extends  LocalRouter {



    public LocalOrRandomRouter(String cacheManagers) {
        super(cacheManagers);
    }


    @Override
    public String getProxyHost(String host) {
        String codisHost = super.getProxyHost(host);
        if (codisHost == null) {
            String[] managers = this.cacheManagers.split(",");
            int i = new Random().nextInt(managers.length);
            return managers[i];
        }
        return codisHost;
    }
}
