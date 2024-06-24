use mysql;
set password='msandbox';

create role if not exists R_DO_IT_ALL;
grant all on *.* to R_DO_IT_ALL;
create user if not exists msandbox@'%' identified by 'msandbox';

grant R_DO_IT_ALL to msandbox@'%' ;
set default role R_DO_IT_ALL to msandbox@'%';

create schema if not exists test;

create table if not exists test.t1
(
    id int not null primary key auto_increment,
    b  int not null,
    c  int not null
)