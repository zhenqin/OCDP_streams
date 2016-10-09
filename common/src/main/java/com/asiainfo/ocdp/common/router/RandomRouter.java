package com.asiainfo.ocdp.common.router;

import java.util.Random;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/10/9
 * Time: 12:03
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class RandomRouter extends Router {




    public RandomRouter(String cacheManagers) {
        super(cacheManagers);
    }

    @Override
    public String getProxyHost(String host) {
        String[] splits = this.cacheManagers.split(",");
        int i = new Random().nextInt(splits.length);
        return splits[i];
    }
}
