-- 1 SystemProp
DROP TABLE IF EXISTS STREAM_SYSTEMPROP;
CREATE TABLE STREAM_SYSTEMPROP (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  pvalue varchar(200) NOT NULL,
  description varchar(500),
  PRIMARY KEY (id),
  UNIQUE KEY (name)
);

-- 2 DataSource
-- properties字段必须的属性:（界面上配置保存时指定的属性名可以先自己定义）
--  type=kafka: zookeeper.connect->"",metadata.broker.list->""
--  type=direct_kafka: metadata.broker.list->""
--  type=hdfs: fs.defaultFS->
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

-- 3 DataInterface
-- type="input|output"
-- properties字段必须的属性:（界面上配置保存时指定的属性名可以先自己定义）
--  DataSource.type=kafka and type=input: topic->"", group.id->"", num.consumer.fetchers->""
--  DataSource.type=kafka and type=output: topic->""
--  DataSource.type=direct_kafka and type=input: topic->""
--  DataSource.type=direct_kafka and type=output: topic->""
--  DataSource.type=hdfs: path->""
DROP TABLE IF EXISTS STREAM_DATAINTERFACE;
CREATE TABLE STREAM_DATAINTERFACE (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  type int NOT NULL,
  dsid varchar(16) NOT NULL,
  delim varchar(10) NOT NULL,
  filter_expr text,
  description text,
  properties text NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE (`dsid`,`name`),
  FOREIGN KEY (dsid) REFERENCES STREAM_DATASOURCE(id)
);


-- 4 LabelRule
DROP TABLE IF EXISTS STREAM_LABEL;
CREATE TABLE STREAM_LABEL (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  classname varchar(100) NOT NULL,
  diid varchar(16) NOT NULL,
  plabelid varchar(16),
  status int default '0',
  description varchar(500),
  properties text NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (diid,name),
  FOREIGN KEY (diid) REFERENCES STREAM_DATAINTERFACE(id)
);

-- 5 Event
DROP TABLE IF EXISTS STREAM_EVENT;
CREATE TABLE STREAM_EVENT (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  diid varchar(16) NOT NULL,
  eventexpr text,
  peventid varchar(16),
  status int NOT NULL DEFAULT 0,
  description varchar(500),
  PRIMARY KEY (id),
  UNIQUE KEY (diid,name),
  FOREIGN KEY (diid) REFERENCES STREAM_DATAINTERFACE(id)
);

-- 6 Subject
-- props:
--  outputType=kafka:
--  outputType=hdfs:
DROP TABLE IF EXISTS STREAM_SUBJECT;
CREATE TABLE STREAM_SUBJECT (
  id varchar(16) NOT NULL,
  name varchar(30) NOT NULL,
  delim varchar(10) NOT NULL,
  subinterval int NOT NULL DEFAULT 0,
  properties text NOT NULL,
  status int NOT NULL DEFAULT 0,
  description varchar(500),
  PRIMARY KEY (id),
  UNIQUE KEY (name)
);

-- 7 Task
DROP TABLE IF EXISTS STREAM_TASK;
CREATE TABLE STREAM_TASK (
  id varchar(16) NOT NULL,
  tid varchar(16) NOT NULL,
  type int NOT NULL,
  receive_interval int NOT NULL DEFAULT 5,
  num_executors int NOT NULL DEFAULT 2,
  driver_memory varchar(5) NOT NULL DEFAULT '2g',
  executor_memory varchar(5) NOT NULL DEFAULT '2g',
  total_executor_cores int NOT NULL DEFAULT 2,
  queue varchar(100),
  status int NOT NULL DEFAULT 0,
  description varchar(500),
  PRIMARY KEY (id),
  UNIQUE KEY (tid)
);
