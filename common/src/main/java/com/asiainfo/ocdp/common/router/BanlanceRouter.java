package com.asiainfo.ocdp.common.router;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/10/9
 * Time: 12:37
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class BanlanceRouter extends Router {


    class HostAndCouter {
        private String host;
        private AtomicInteger counter = new AtomicInteger(0);


        public HostAndCouter(String host) {
            this.host = host;
        }


        public int getCounter() {
            return counter.get();
        }

        public int incrementAndGet() {
            return counter.incrementAndGet();
        }

        public int decrementAndGet() {
            return counter.decrementAndGet();
        }

        public int intValue() {
            return counter.intValue();
        }

        public String getHost() {
            return host;
        }
    }
    protected final LinkedList<HostAndCouter> list = new LinkedList<HostAndCouter>();

    public BanlanceRouter(String cacheManagers){
        super(cacheManagers);
        for (Map.Entry<String, String> entry : PROXY_HOST_MAP.entrySet()) {
            list.add(new HostAndCouter(entry.getValue()));
        }
    }

    @Override
    public String getProxyHost(String host) {
        Collections.sort(this.list, new Comparator<HostAndCouter>() {
            @Override
            public int compare(HostAndCouter o1, HostAndCouter o2) {
                return o1.getCounter() - o2.getCounter();
            }
        });
        HostAndCouter first = this.list.getFirst();
        first.incrementAndGet();
        return first.host;
    }
}
