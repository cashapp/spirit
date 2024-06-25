use mysql;

create role if not exists R_MIGRATOR;
grant alter, create, delete, drop, index, insert, lock tables, select, trigger, update on *.* to R_MIGRATOR;
create role if not exists R_REPLICATION;
grant REPLICATION SLAVE, REPLICATION CLIENT on *.* to R_REPLICATION;
create role if not exists R_THROTTLER;
grant SELECT on performance_schema.replication_applier_status_by_worker to R_THROTTLER;
grant SELECT on performance_schema.replication_connection_status to R_THROTTLER;

create user if not exists msandbox@'%' identified with caching_sha2_password by 'msandbox';
grant R_MIGRATOR, R_REPLICATION to msandbox@'%' ;
set default role R_MIGRATOR, R_REPLICATION to msandbox@'%';

create user if not exists rsandbox@'%' identified with caching_sha2_password by 'rsandbox';
grant R_REPLICATION, R_THROTTLER to rsandbox@'%';
set default role R_REPLICATION, R_THROTTLER to rsandbox@'%';

-- unfortunately the password for tsandbox must be the same as the password for root because in tests we switch to root
-- using the same password.
create user if not exists tsandbox@'%' identified with caching_sha2_password by 'msandbox';
grant R_MIGRATOR, R_REPLICATION to tsandbox@'%' ;
grant references, super, process on *.* to tsandbox@'%'; -- used in tests
set default role R_MIGRATOR, R_REPLICATION to tsandbox@'%';

flush privileges;

create database if not exists test;

use test;

create table t1
(
    id int not null primary key auto_increment,
    b  int not null,
    c  int not null
);
