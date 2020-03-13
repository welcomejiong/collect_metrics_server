#20150316
INSERT INTO t_bi_user (login_name, PASSWORD, 
 sex, department, email, 
 telepone, create_date, last_login_time, 
 update_date,real_name,user_bi_id)
SELECT username, MD5(PASSWORD) PASSWORD,1 sex,1 department,email,'' telepone, CURRENT_DATE, CURRENT_DATE, CURRENT_DATE,realname,userid
FROM bi_report_old.user

insert into user_role(user_id,role_id) 
select a.userid,  
substring_index(substring_index(substring(a.role_id,2,length(a.role_id)-2),',',b.help_topic_id+1),',',-1) as role_id
from 
user a
join
mysql.help_topic b
on b.help_topic_id < (length(a.role_id) - length(replace(a.role_id,',','')))-1

insert into user_role_sn_game(user_id,role_id,sn_id,game_id)
select distinct ur.user_id,ur.role_id,sb.sn_id,gb.game_id from user_role ur,tbl_group_game_bind gb ,tbl_group_sn_bind  sb
where ur.role_id=gb.group_id
and ur.role_id=sb.group_id

select sg.* ,bgs.id sys_game_id from bi_report_db.games bgs,bi_report_old.user_role_sn_game sg 
where sg.sn_id=bgs.snid
and sg.game_id=bgs.gameid

select distinct tu.id,t_1.sys_game_id game_id from 
(select sg.* ,bgs.id sys_game_id from bi_report_db.games bgs,bi_report_old.user_role_sn_game sg 
where sg.sn_id=bgs.snid
and sg.game_id=bgs.gameid)t_1,t_bi_user tu where t_1.user_id=tu.user_bi_id

insert into user_game(user_id,game_id,user_bi_id)
select distinct tu.id,t_1.sys_game_id game_id ,tu.user_bi_id   from 
(select sg.* ,bgs.id sys_game_id from bi_report_db.games bgs,bi_report_old.user_role_sn_game sg 
where sg.sn_id=bgs.snid
and sg.game_id=bgs.gameid)t_1,t_bi_user tu where t_1.user_id=tu.user_bi_id

insert into bi_report_db.group_users(user_id,group_id)
select distinct a.userId,
  (case 
     when a.role_id=99 then 2  
	  when a.role_id=98 then 4 
	  else 3 end) roleId
 from
(select distinct ur.role_id as role_id ,bu.id as userId
from bi_report_old.user_role ur,bi_report_db.t_bi_user bu
where bu.user_bi_id = ur.user_id)a


#20150321
ALTER TABLE `games`
	ADD COLUMN `is_monitor` INT NULL DEFAULT '0' AFTER `rate`;
<<<<<<< .working
=======

	
#20150330
ALTER TABLE `games`
	ADD COLUMN `stats` VARCHAR(2) NOT NULL DEFAULT '0' AFTER `rate`;
	
update games
set stats = '1'
where
(snid =11 and gameid=120    )
 or(snid =11 and gameid=109 )
 or(snid =11 and gameid=126 )
 or(snid =15 and gameid=119 )
 or(snid =1 and gameid=130  )
 or(snid =1 and gameid=136  )
 or(snid =45 and gameid=71  )
 or(snid =11 and gameid=94  )
 or(snid =11 and gameid=104 )
 or(snid =32 and gameid=103 )
 or(snid =1 and gameid=118  )
 or(snid =1 and gameid=71   )
 or(snid =2 and gameid=20   )
 or(snid =36 and gameid=129 )
 or(snid =1 and gameid=26   )
 or(snid =11 and gameid=73  )
 or(snid =34 and gameid=128 )
 or(snid =34 and gameid=121 )
 or(snid =15 and gameid=133 )
 or(snid =11 and gameid=132 )
 or(snid =34 and gameid=73  )
 or(snid =1 and gameid=106  )
 or(snid =1 and gameid=123  )
 or(snid =44 and gameid=83  )
 or(snid =43 and gameid=125 )
 or(snid =15 and gameid=83  )
 or(snid =1 and gameid=63   )
 or(snid =34 and gameid=39   )
 or(snid =31 and gameid=124  )
 or(snid =32 and gameid=117  )
 or(snid =1 and gameid=65    )
 or(snid =34 and gameid=83   )
 or(snid =32 and gameid=20   )
 
 
 #20140409
 ALTER TABLE `daily_report`
	ADD COLUMN `role_choice` INT NOT NULL DEFAULT '0' AFTER `create_time`;

ALTER TABLE `source_daily_report`
	ADD COLUMN `role_choice` INT NOT NULL DEFAULT '0' AFTER `create_time`;
 
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic game-etl-finished-topic


2015/4/21 
insert into bi_report_db.group_users(user_id,group_id)
select distinct a.userId,
  (case 
     when a.role_id=99 then 2  
	  when a.role_id=98 then 4 
	  else 3 end) roleId
 from
(select distinct ur.role_id as role_id ,bu.id as userId
from bi_report_old.user_role ur,bi_report_db.t_bi_user bu
where bu.user_bi_id = ur.user_id)a

2015/4/27
ALTER TABLE `games`
	ADD COLUMN `time_zone` VARCHAR(50) NULL DEFAULT 'GMT+8' AFTER `stats`,
	ADD COLUMN `etl_trigger_time` DATETIME NULL DEFAULT NULL AFTER `time_zone`,
	ADD COLUMN `etl_trigger_id` VARCHAR(50) NULL DEFAULT NULL AFTER `etl_trigger_time`;
	
update games set time_zone='GMT+2' where snid=11 and gameid=108;
update games set time_zone='GMT+2' where snid=11 and gameid=113;
update games set time_zone='GMT-5' where snid=43 and gameid=125;
update games set time_zone='GMT-4' where snid=34 and gameid=135;
update games set time_zone='GMT-11' where snid=11 and gameid=91;