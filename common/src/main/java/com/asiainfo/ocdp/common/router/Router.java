package com.asiainfo.ocdp.common.router;

import java.util.HashMap;
import java.util.Map;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/10/9
 * Time: 11:50
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class Router {

    protected final String cacheManagers;



    protected final Map<String, String> PROXY_HOST_MAP = new HashMap<String, String>();


    public Router(String cacheManagers) {
        this.cacheManagers = cacheManagers;
        String[] managers = this.cacheManagers.split(",");
        for (String manager : managers) {
            String[] split = manager.split(":");
            this.PROXY_HOST_MAP.put(split[0], manager);
        }
    }

    public abstract String getProxyHost(String host);
}
