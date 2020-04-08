

CREATE KEYSPACE rawdatas WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE dau (
   snId text,
   gameId text,
   ds text,
   clientId text,
   userId text,
   source text,
   affiliate text,
   creative text,
   family text,
   genus text,
   ip text,
   dauDate text,
   dauTime text,
   fromUid text,
   extra text,
   version int,
   udid text,
   roleid text,
  PRIMARY KEY ((snId,gameId,ds),clientId,userId)
)WITH CLUSTERING ORDER BY (clientId ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;


CREATE TABLE install (
   snId text,
   gameId text,
   ds text,
   clientId text,
   userId text,
   source text,
   affiliate text,
   creative text,
   family text,
   genus text,
   fromUid text,
   installDate text,
   installTime text,
   extra text,
   version int,
   udid text,
   roleid text,
  PRIMARY KEY ((snId,gameId,ds),clientId,userId)
)WITH CLUSTERING ORDER BY (clientId ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;

CREATE TABLE counter (
    snId text,
	gameId text,
	ds text,
	clientId text,
	userId text,
	userLevel text ,
	counter text ,
	value text ,
	kingdom text ,
	phylum text ,
	classfield text ,
	family text ,
	genus text ,
	counterDate text ,
	counterTime text ,
 	extra  text  ,
 	version int,
    udid text,
    roleid text,
  PRIMARY KEY ((snId,gameId,ds,clientId,counter),kingdom,phylum,classfield,userId,counterTime)
)WITH CLUSTERING ORDER BY (kingdom ASC,phylum ASC,classfield ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;


CREATE TABLE economy (
    snId text,
	gameId text,
	ds text,
	clientId text,
	userId text,
	currency  text ,
	amount  text , 
	value  text ,
	kingdom  text ,
	phylum  text ,
	classfield  text ,
	family  text ,
	genus  text ,
	economyDate  text ,
	economyTime  text ,
 	extra  text  ,
 	version int,
    udid text,
    roleid text,
  PRIMARY KEY ((snId,gameId,ds,clientId,currency),kingdom,phylum,classfield,userId,economyTime)
)WITH CLUSTERING ORDER BY (kingdom ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;

CREATE TABLE gameinfo (
    snId text,
	gameId text,
	ds text,
	clientId text,
	userId text,
	userLevel  text ,
	gameinfo  text ,
	value  text ,
	kingdom  text ,
	phylum  text ,
	classfield  text ,
	family  text ,
	genus  text ,
	gameinfoDate  text ,
	gameinfoTime  text ,
 	extra  text  ,
 	version int,
    udid text,
    roleid text,
  PRIMARY KEY ((snId,gameId,ds),gameinfo,kingdom,phylum,classfield,clientId,userId,gameinfoTime)
)WITH CLUSTERING ORDER BY (gameinfo ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;



CREATE TABLE milestone (
    snId text,
	gameId text,
	ds text,
	clientId text,
	userId text,
	milestone  text ,
	value  text ,
	milestoneDate  text ,
	milestoneTime  text ,
 	extra  text  ,
 	version int,
    udid text,
    roleid text,
  PRIMARY KEY ((snId,gameId,ds),milestone,clientId,userId,milestoneTime)
)WITH CLUSTERING ORDER BY (milestone ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;

CREATE TABLE payment (
    snId text,
	gameId text,
	ds text,
	clientId text,
	userId text,
	amount  text ,
	currency  text ,
	provider  text ,
	ip  text ,
	transactionid  text ,
	status  text ,
	kingdom  text ,
	phylum  text ,
	value2  text ,
	paymentDate  text ,
	paymentTime  text ,
 	extra  text  ,
 	version int,
    udid text,
    roleid text,
  PRIMARY KEY ((snId,gameId,ds),currency,provider,clientId,userId,transactionid,paymentTime)
)WITH CLUSTERING ORDER BY (currency ASC,provider ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;


CREATE TABLE adtracking (
    snId text,
	gameId text,
	ds text,
	clientId text,
	userId text,
	appkey  text ,
    mac  text ,
    macMd5  text ,
    ifa  text ,
    ifaMd5  text ,
    uuid  text ,
    actType  int ,
    trackingDs  text ,
    actTime  text ,
    pf  text ,
    ip  text ,
    userAgent  text ,
 	extra  text  ,
 	version int,
    udid text,
    roleid text,
  PRIMARY KEY ((snId,gameId,ds,appkey),actType,actTime,pf,ip,uuid)
)WITH CLUSTERING ORDER BY (actType ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;


CREATE TABLE custombinarybodymetric (
    snId text,
	gameId text,
	ds text,
	clientId text,
	userId text,
	customMetircName  text ,
	metricDate  text ,
	metricTime  text ,
	body  blob ,
 	extra  text  ,
 	version int,
   udid text,
   roleid text,
  PRIMARY KEY ((snId,gameId,ds),customMetircName,clientId,userId,metricTime)
)WITH CLUSTERING ORDER BY (customMetircName ASC,clientId ASC,userId ASC)
AND compaction = {'class': 'SizeTieredCompactionStrategy'}
;



