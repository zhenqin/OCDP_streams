-- 1 STREAM_SYSTEMPROP
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"startTimeOutSeconds",45,"",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"delaySeconds",5,"",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"periodSeconds",30,"",0);

INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"cacheManager","CodisCacheManager","设置使用的cache类,该类提供cache操作的api",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"cacheServers","spark1:19000,spark2:19000,spark3:19000,spark4:19000","cacheServers(codis)的访问地址",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"jedisPoolMaxTotal",1000,"设置jedisPool的最大连接数",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"jedisPoolMaxIdle",300,"设置jedisPool的最大idle连接数",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"jedisPoolMinIdle",100,"设置jedisPool的最小idle连接数",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"jedisPoolMEM",600000,"设置jedisPool的？",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"jedisTimeOut",10000,"设置 jedis 访问超时，单位毫秒",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"cacheQryThreadPoolSize",50,"设置每个executor线程执行task操作cache时使用的线程池的线程数大小",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"cacheQryBatchSizeLimit",100,"设置批量操作cache时的每批次最大处理数量",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"cacheQryTaskSizeLimit",200,"设置批量操作cache任务中时批量提交到cacheServer(codis)的每批次最大数量",0);

INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"SPARK_HOME","/home/spark/app/spark-1.4.0-cdh5.0.2","",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"master","spark://spark1:4050","",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"supervise","false","",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"queue","","",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"appJars","streaming-core-1.0-SNAPSHOT.jar","",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"jars","streaming-event-mc-1.0-SNAPSHOT.jar","",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"interface_class","com.asiainfo.ocdp.streaming.manager.App_Interface","",0);
INSERT INTO STREAM_SYSTEMPROP (id,name,value,description,issys) VALUES (uuid(),"subject_class","","",0);


-- 2 STREAM_DATASOURCETYPE
INSERT INTO STREAM_DATASOURCETYPE(id,name,propname) VALUES(uuid(),"kafka","zookeeper.connect,metadata.broker.list,serializer.class");
INSERT INTO STREAM_DATASOURCETYPE(id,name,propname) VALUES(uuid(),"jdbc","jdbcurl,user,password");