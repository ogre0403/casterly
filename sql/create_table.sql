CREATE DATABASE casterly;
create user 'casterly'@'172.16.1.12' identified by 'casterly@hcgwc112';
grant all on casterly.* to 'casterly'@'%';
USE casterly;
CREATE TABLE APP_SUMMARY (
  EPOCH BIGINT,
  SEQ SMALLINT,
  USER CHAR(16),
  JOBNAME CHAR(255),
  QUEUE CHAR(32),
  START BIGINT,
  FINISH BIGINT,
  CPUHOUR BIGINT,
  PRIMARY KEY (EPOCH, SEQ)
);
CREATE TABLE TASK_DETAIL (
  EPOCH BIGINT,
  SEQ SMALLINT,
  TYPE CHAR(1),
  TASKID BIGINT,
  ATTEMPTID SMALLINT,
  START BIGINT,
  FINISH BIGINT,
  PRIMARY KEY (EPOCH, SEQ, TYPE, TASKID, ATTEMPTID)
);
CREATE TABLE EXECUTOR_DETAIL (
  EPOCH BIGINT,
  SEQ SMALLINT,
  ID SMALLINT,
  START BIGINT,
  PRIMARY KEY (EPOCH, SEQ, ID)
);

CREATE TABLE  LAST_PROCESSED (
  ID CHAR(10),
  LAST BIGINT,
  PRIMARY KEY (ID)
);
INSERT INTO LAST_PROCESSED (ID, LAST) VALUES ('MR',0);
INSERT INTO LAST_PROCESSED (ID, LAST) VALUES ('SPARK',0);
