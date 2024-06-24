use mysql;

create role if not exists R_DO_IT_ALL;
grant all on *.* to R_DO_IT_ALL;
create user if not exists msandbox@'%' identified with caching_sha2_password by 'msandbox';

grant R_DO_IT_ALL to msandbox@'%' ;
set default role R_DO_IT_ALL to msandbox@'%';


create role if not exists R_REPLICATION;
grant REPLICATION SLAVE, REPLICATION CLIENT on *.* to R_REPLICATION;
create role if not exists R_THROTTLER;
grant SELECT on performance_schema.replication_applier_status_by_worker to R_THROTTLER;
grant SELECT on performance_schema.replication_connection_status to R_THROTTLER;
create user if not exists rsandbox@'%' identified with caching_sha2_password by 'rsandbox';
grant R_REPLICATION, R_THROTTLER to rsandbox@'%';
set default role R_REPLICATION, R_THROTTLER to rsandbox@'%';

flush privileges;

create database if not exists test;

use test;

create table t1
(
    id int not null primary key auto_increment,
    b  int not null,
    c  int not null
);
