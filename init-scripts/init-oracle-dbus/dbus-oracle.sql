create table DBUS.TABLE_META_HIS
(
  owner              VARCHAR2(32) not null,
  table_name         VARCHAR2(32) not null,
  column_name        VARCHAR2(32) not null,
  column_id          NUMBER,
  version            NUMBER(8) not null,
  data_type          VARCHAR2(128) not null,
  data_length        NUMBER,
  data_precision     NUMBER,
  data_scale         NUMBER,
  nullable           VARCHAR2(1),
  is_pk              VARCHAR2(1),
  pk_position        NUMBER(2),
  ddl_time           TIMESTAMP(6) not null,
  char_length        NUMBER(6),
  char_used          VARCHAR2(1),
  internal_column_id NUMBER,
  hidden_column      VARCHAR2(8),
  virtual_column     VARCHAR2(8),
  comments           VARCHAR2(4000)
)
;
comment on table DBUS.TABLE_META_HIS
  is '表结构元数据历时信息，包含版本';
comment on column DBUS.TABLE_META_HIS.owner
  is '表的所有者';
comment on column DBUS.TABLE_META_HIS.table_name
  is '表名';
comment on column DBUS.TABLE_META_HIS.column_name
  is '列名';
comment on column DBUS.TABLE_META_HIS.column_id
  is '列ID';
comment on column DBUS.TABLE_META_HIS.version
  is '版本号';
comment on column DBUS.TABLE_META_HIS.data_type
  is '数据类型';
comment on column DBUS.TABLE_META_HIS.data_length
  is '长度';
comment on column DBUS.TABLE_META_HIS.data_precision
  is '精度';
comment on column DBUS.TABLE_META_HIS.data_scale
  is '小数部分长度';
comment on column DBUS.TABLE_META_HIS.nullable
  is '是否可为空';
comment on column DBUS.TABLE_META_HIS.is_pk
  is '是否为主键';
comment on column DBUS.TABLE_META_HIS.pk_position
  is '主键列的位置（创建符合主键时列的顺序）';
comment on column DBUS.TABLE_META_HIS.ddl_time
  is 'ddl发生的时间';
comment on column DBUS.TABLE_META_HIS.comments
  is '列注释';
alter table DBUS.TABLE_META_HIS
  add constraint PK_TABLE_META_HIS primary key (OWNER, TABLE_NAME, VERSION, COLUMN_NAME);

create table DBUS.META_SYNC_EVENT
(
  serno       NUMBER not null,
  table_owner VARCHAR2(32) not null,
  table_name  VARCHAR2(32) not null,
  version     NUMBER(11),
  event_time  TIMESTAMP(6),
  ddl_type    VARCHAR2(64) not null,
  ddl         VARCHAR2(3000)
)
;
comment on table DBUS.META_SYNC_EVENT
  is '元数据同步事件表';
comment on column DBUS.META_SYNC_EVENT.table_owner
  is '表的所有者';
comment on column DBUS.META_SYNC_EVENT.table_name
  is '表名';
comment on column DBUS.META_SYNC_EVENT.version
  is '版本号';
comment on column DBUS.META_SYNC_EVENT.event_time
  is '时间戳';
comment on column DBUS.META_SYNC_EVENT.ddl_type
  is 'ddl类型';
comment on column DBUS.META_SYNC_EVENT.ddl
  is 'ddl语句';
alter table DBUS.META_SYNC_EVENT
  add constraint PK_META_SYNC_EVENT primary key (SERNO);

create table DBUS.DB_HEARTBEAT_MONITOR
(
  id          NUMBER(19) not null,
  ds_name     VARCHAR2(32) not null,
  schema_name VARCHAR2(32) not null,
  table_name  VARCHAR2(64) not null,
  packet      VARCHAR2(256) not null,
  create_time VARCHAR2(32) not null,
  update_time VARCHAR2(32)
)
;
alter table DBUS.DB_HEARTBEAT_MONITOR
  add constraint PK_HEART_MONITOR_ID primary key (ID);

create table DBUS.DB_FULL_PULL_REQUESTS
(
  seqno                NUMBER not null,
  schema_name          VARCHAR2(32),
  table_name           VARCHAR2(50) not null,
  scn_no               NUMBER,
  split_col            VARCHAR2(50),
  split_bounding_query VARCHAR2(512),
  pull_target_cols     VARCHAR2(512),
  pull_req_create_time TIMESTAMP(6) not null,
  pull_start_time      TIMESTAMP(6),
  pull_end_time        TIMESTAMP(6),
  pull_status          VARCHAR2(16),
  pull_remark          VARCHAR2(1024)
)
;
comment on column DBUS.DB_FULL_PULL_REQUESTS.seqno
  is '主键';
comment on column DBUS.DB_FULL_PULL_REQUESTS.schema_name
  is '待拉取的schema名称';
comment on column DBUS.DB_FULL_PULL_REQUESTS.table_name
  is '表名';
comment on column DBUS.DB_FULL_PULL_REQUESTS.scn_no
  is 'scn号';
comment on column DBUS.DB_FULL_PULL_REQUESTS.split_col
  is '分片列名称';
comment on column DBUS.DB_FULL_PULL_REQUESTS.split_bounding_query
  is '分片查询';
comment on column DBUS.DB_FULL_PULL_REQUESTS.pull_target_cols
  is '目标列';
comment on column DBUS.DB_FULL_PULL_REQUESTS.pull_req_create_time
  is '创建时间';
comment on column DBUS.DB_FULL_PULL_REQUESTS.pull_start_time
  is '开始时间';
comment on column DBUS.DB_FULL_PULL_REQUESTS.pull_end_time
  is '结束时间';
comment on column DBUS.DB_FULL_PULL_REQUESTS.pull_status
  is '状态';
comment on column DBUS.DB_FULL_PULL_REQUESTS.pull_remark
  is '备注';
alter table DBUS.DB_FULL_PULL_REQUESTS
  add primary key (SEQNO);

create table DBUS.DBUS_TABLES
(
  owner       VARCHAR2(32) not null,
  table_name  VARCHAR2(32) not null,
  create_time TIMESTAMP(6)
)
;
alter table DBUS.DBUS_TABLES
  add constraint PK_DBUS_TABLES primary key (OWNER, TABLE_NAME);

create table DBUS.TEST_TABLE
(
  id             NUMBER not null,
  char_type      CHAR(20),
  varchar_type   VARCHAR2(20),
  nchar_type     NCHAR(20),
  nvarchar_type  NVARCHAR2(20),
  int_type       INTEGER,
  smallint_type  INTEGER,
  float_type     FLOAT,
  double_type    FLOAT,
  numuer2_type   NUMBER(10),
  number3_type   NUMBER(20,6),
  date_type      DATE,
  timestamp_type TIMESTAMP(6),
  lob_type       CLOB,
  bin_float      BINARY_FLOAT default 1.12 not null
)
;
alter table DBUS.TEST_TABLE
  add constraint PK_ID primary key (ID);

create sequence DBUS.SEQ_DB_FULL_PULL_REQUESTS
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
cache 20;

create sequence DBUS.SEQ_TEST_TABLE
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
cache 20;

create sequence DBUS.SEQ_DDL_VERSION
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
cache 20;

create sequence DBUS.SEQ_HEARTBEAT_MONITOR
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 1
cache 20;