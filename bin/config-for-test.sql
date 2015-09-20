DROP TABLE IF EXISTS STREAM_SYSTEMPROP;
CREATE TABLE STREAM_SYSTEMPROP (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  pvalue varchar(200) NOT NULL,
  description varchar(500),
  PRIMARY KEY (id),
  UNIQUE KEY (name)
);


--
INSERT INTO `STREAM_SYSTEMPROP` (`ID`, `NAME`, `PVALUE`) VALUES
        ('1', 'DefaultCacheManager', 'CodisCacheManager'),
        ('2', 'CodisProxy', 'codis1:29001,codis2:29002'),
        ('3', 'JedisMaxTotal', '1000'),
        ('4', 'JedisMaxIdle', '300'),
        ('5', 'JedisMEM', '600000'),
        ('6', 'JedisTimeOut', '10000'),
        ('7', 'checkpoint_dir', 'streaming/checkpoint'),
        ('8', 'codisQryThreadNum', '500'),
        ('9', 'pipeLineBatch', '200'),
        ('10', 'batchSize', '100'),
        ('11',"SPARK_HOME","/home/leo/app/spark-1.4.0-cdh5.0.2"),
        ('12',"master","spark://leo:4040"),
        ('13',"supervise","false"),
        ('14',"queue","false"),
        ('15',"interface_class","com.asiainfo.ocdp.streaming.App_Interface"),
        ('16',"subject_class",""),
        ('17',"delaySeconds","5"),
        ('18',"periodSeconds","30"),
        ('19',"appJars","streaming-core-1.0-SNAPSHOT.jar"),
        ('20',"jars","streaming-core-1.0-SNAPSHOT.jar,streaming-event-mc-1.0-SNAPSHOT.jar")
;

DROP TABLE IF EXISTS STREAM_DATASOURCE;
CREATE TABLE STREAM_DATASOURCE (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  type varchar(20) NOT NULL,
  description varchar(500),
  properties text NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (name)
);

INSERT INTO `STREAM_DATASOURCE`
(`ID`, `DESCRIPTION`,
`NAME`,
`PROPERTIES`,
`TYPE`) VALUES
        ('1', NULL,
        'ds1_kafka',
        '[
        	{"pname":"zookeeper.connect","pvalue":"localhost:2181"},
        	{"pname":"metadata.broker.list","pvalue":"localhost:9092"}
        ]',
        'kafka');

DROP TABLE IF EXISTS STREAM_DATAINTERFACE;
CREATE TABLE STREAM_DATAINTERFACE (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  type int NOT NULL,
  dsid varchar(16) NOT NULL,
  delim varchar(10) NOT NULL,
  description text,
  properties text NOT NULL,
  status int NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE (`dsid`,`name`),
  FOREIGN KEY (dsid) REFERENCES STREAM_DATASOURCE(id)
);


INSERT INTO `STREAM_DATAINTERFACE`
(`ID`, `DELIM`, `TYPE`,
`DSID`,
`NAME`,
`PROPERTIES`,
`STATUS`) VALUES
        ('1', ',', '0',
        '1',
        'di1_input_kafka_topic_stream1',
        '{"props":[
    		{"pname":"topic", "pvalue":"topic_stream1"},
    		{"pname":"direct_kafka_api_flag", "pvalue":"true"},
    		{"pname":"batch.duration.seconds", "pvalue":"15"},
    		{"pname":"field.numbers","pvalue":"17"},
    		{"pname":"uniqKeys", "pvalue":"imsi"},
    		{"pname":"UKSeparator", "pvalue":"#"},
    		{"pname":"filter_expr", "pvalue":"str.len == 16 and (str(7) != \\\"\\\" or str(8) != \\\"\\\")"},
    		{"pname":"delim", "pvalue":","},
        	{"pname":"num.consumer.fetchers","pvalue":"4"},
        	{"pname":"batchSize","pvalue":"100"}
    		],
    	"fields":[
    		{"pname":"EventID","ptype":"Int","pvalue":""},
    		{"pname":"time","ptype":"string","pvalue":""},
    		{"pname":"lac","ptype":"string","pvalue":""},
    		{"pname":"ci","ptype":"string","pvalue":""},
    		{"pname":"callingimei","ptype":"string","pvalue":""},
    		{"pname":"calledimei","ptype":"string","pvalue":""},
    		{"pname":"callingimsi","ptype":"string","pvalue":""},
    		{"pname":"calledimsi","ptype":"string","pvalue":""},
    		{"pname":"callingphone","ptype":"string","pvalue":""},
    		{"pname":"calledphone","ptype":"string","pvalue":""},
    		{"pname":"eventresult","ptype":"int","pvalue":""},
    		{"pname":"alertstatus","ptype":"int","pvalue":""},
    		{"pname":"assstatus","ptype":"int","pvalue":""},
    		{"pname":"clearstatus","ptype":"int","pvalue":""},
    		{"pname":"relstatus","ptype":"int","pvalue":""},
    		{"pname":"xdrtype","ptype":"int","pvalue":""},
    		{"pname":"issmsalone","ptype":"int","pvalue":""},
    		{"pname":"imsi","ptype":"String","pvalue":""},
    		{"pname":"imei","ptype":"String","pvalue":""}
    		]
    	}',
    	'1');

INSERT INTO `STREAM_DATAINTERFACE`
(`ID`, `DELIM`, `TYPE`,
`DSID`,
`NAME`,
`PROPERTIES`,
`STATUS`) VALUES
        ('2', ',', '1',
        '1',
        'di2_output_kafka_zhujiayu',
        '{"props":[
    		{"pname":"topic", "pvalue":"topic_stream1"},
    		{"pname":"delim", "pvalue":","},
    		{"pname":"userKeyIdx", "pvalue":"2"},
    		{"pname":"kafkakeycol", "pvalue":"2"},
    		{"pname":"batchsize", "pvalue":"100"}
    		]
    	}',
    	'1');

INSERT INTO `STREAM_DATAINTERFACE`
(`ID`, `DELIM`, `TYPE`,
`DSID`,
`NAME`,
`PROPERTIES`,
`STATUS`) VALUES
        ('3', ',', '0',
        '2',
        'di1_input_kafka_topic_stream1',
        '{"props":[
    		{"pname":"topic", "pvalue":"topic_stream1"},
    		{"pname":"direct_kafka_api_flag", "pvalue":"false"},
    		{"pname":"group.id", "pvalue":"cg1"},
    		{"pname":"num.consumer.fetchers", "pvalue":"3"},
    		{"pname":"batch.duration.seconds", "pvalue":"15"},
    		{"pname":"field.numbers","pvalue":"17"},
    		{"pname":"uniqKeys", "pvalue":"imsi"},
    		{"pname":"UKSeparator", "pvalue":"#"},
    		{"pname":"filter_expr", "pvalue":"str.len == 16 and (str(7) != \\\"\\\" or str(8) != \\\"\\\")"}
    		],
    	"fields":[
    		{"pname":"EventID","ptype":"Int","pvalue":""},
    		{"pname":"time","ptype":"string","pvalue":""},
    		{"pname":"lac","ptype":"string","pvalue":""},
    		{"pname":"ci","ptype":"string","pvalue":""},
    		{"pname":"callingimei","ptype":"string","pvalue":""},
    		{"pname":"calledimei","ptype":"string","pvalue":""},
    		{"pname":"callingimsi","ptype":"string","pvalue":""},
    		{"pname":"calledimsi","ptype":"string","pvalue":""},
    		{"pname":"callingphone","ptype":"string","pvalue":""},
    		{"pname":"calledphone","ptype":"string","pvalue":""},
    		{"pname":"eventresult","ptype":"int","pvalue":""},
    		{"pname":"alertstatus","ptype":"int","pvalue":""},
    		{"pname":"assstatus","ptype":"int","pvalue":""},
    		{"pname":"clearstatus","ptype":"int","pvalue":""},
    		{"pname":"relstatus","ptype":"int","pvalue":""},
    		{"pname":"xdrtype","ptype":"int","pvalue":""},
    		{"pname":"issmsalone","ptype":"int","pvalue":""},
    		{"pname":"imsi","ptype":"String","pvalue":""},
    		{"pname":"imei","ptype":"String","pvalue":""}
    		]
    	}',
    	'1');


DROP TABLE IF EXISTS STREAM_LABEL;
CREATE TABLE STREAM_LABEL (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  class_name varchar(100) NOT NULL,
  diid varchar(16) NOT NULL,
  p_label_id varchar(16),
  status int default '0',
  description varchar(500),
  properties text,
  PRIMARY KEY (id),
  UNIQUE KEY (diid,name),
  FOREIGN KEY (diid) REFERENCES DataInterface(id)
);

INSERT INTO `STREAM_LABEL`
(`ID`, `CLASS_NAME`, `DESCRIPTION`,
`DIID`, `NAME`, `P_label_Id`,
`PROPERTIES`,
`STATUS`) VALUES
        ('1', 'com.asiainfo.ocdp.streaming.label.SiteRule', NULL,
        '1', 'SiteLabel', '',
        '{"props":[
        	{}
        	]
        }',
        1);


INSERT INTO `STREAM_LABEL`
(`ID`, `CLASS_NAME`, `DESCRIPTION`,
`DIID`, `NAME`, `P_label_Id`,
`PROPERTIES`,
`STATUS`) VALUES
        ('1', 'com.asiainfo.ocdp.streaming.label.SiteRule', NULL,
        '1', 'SiteLabel', '',
        '{"props":[
        	{"name":"b","value":"b","description":"b"}
        	]
        ,"params":[
        	{"name":"c","value":"c","description":"c"}
        	]
        }',
        1);



DROP TABLE IF EXISTS STREAM_EVENT;
CREATE TABLE STREAM_EVENT (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  diid varchar(16) NOT NULL,
  filter_expr text,
  p_event_id varchar(16),
  PROPERTIES text,
  status int NOT NULL DEFAULT 0,
  description varchar(500),
  PRIMARY KEY (id),
  UNIQUE KEY (diid,name),
  FOREIGN KEY (diid) REFERENCES DataInterface(id)
);

INSERT INTO `STREAM_EVENT` (`ID`, `DESCRIPTION`,
`DIID`, `FILTER_EXPR`,
`NAME`, `P_EVENT_ID`,
 `PROPERTIES`, `STATUS`) VALUES
        ('1', 'a',
        '1', 'labels[\'area_onsite\'][\'ZHUJIAYU\'] = \'true\'',
        'event1', NULL,
        NULL, 1);

-- properties
--
DROP TABLE IF EXISTS STREAM_SUBJECT;
CREATE TABLE STREAM_SUBJECT (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  select_expr varchar(200) NOT NULL,
  output_diid varchar(16) NOT NULL,
  properties text NOT NULL,
  status int NOT NULL DEFAULT 0,
  description varchar(500),
  PRIMARY KEY (id),
  UNIQUE KEY (name)
);


INSERT INTO `STREAM_SUBJECT` (`ID`, `NAME`,
`PROPERTIES`,
`STATUS`,
`DESCRIPTION`) VALUES
        ('1', 'ZhuJiaYu1',
        '{"props":[
        	{"pname":"userKeyIdx", "pvalue":"2"},
        	{"pname":"class_name", "pvalue":""},
        	{"pname":"delaytime", "pvalue":"1800000"}
        	],
          "events":[
            {"eventId":"1", "select_expr":"imsi,time,labels[\'user_info\'][\'product_no\'],labels[\'area_onsite\'][\'ZHUJIAYU\'],labels[\'area_info\'][\'zhujiayu_xiaoqu_name\']"}
          ],
          "output_dis":[
            {"pname":"diid", "pvalue":"2"}
          ]
        }',
        '1',
        NULL);


INSERT INTO `STREAM_TASK` (`ID`, `NAME`,
`PROPERTIES`,
`STATUS`,
`DESCRIPTION`) VALUES
        ('1', 'ZhuJiaYu1',
        '{"props":[
        	{"pname":"userKeyIdx", "pvalue":"2"},
        	{"pname":"class_name", "pvalue":""},
        	{"pname":"delaytime", "pvalue":"1800000"}
        	],
          "events":[
            {"eventId":"1", "select_expr":"imsi,time,labels[\'user_info\'][\'product_no\'],labels[\'area_onsite\'][\'ZHUJIAYU\'],labels[\'area_info\'][\'zhujiayu_xiaoqu_name\']"}
          ],
          "output_dis":[
            {"pname":"diid", "pvalue":"2"}
          ]
        }',
        '1',
        NULL);