use default;
create table if not exists test_azkaban(id int,name string,address string) row format delimited fields terminated by ',';
load data local inpath '/home/hadoop/azkaban/test.txt' into table test_azkaban;
create table if not exists countaddress as select address,count(*) as num from test_azkaban group by address ;

insert overwrite local directory '/home/hadoop/azkaban/out' select * from countaddress; 