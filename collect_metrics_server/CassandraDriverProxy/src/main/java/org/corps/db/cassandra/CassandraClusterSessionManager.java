package org.corps.db.cassandra;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.corps.db.cassandra.core.CassandraCluster;
import org.corps.db.cassandra.core.CassandraConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraClusterSessionManager {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(CassandraClusterSessionManager.class);
	
	private final CassandraConfig config;
	
	private final Cluster cluster;
	
	private final ConcurrentMap<String, Session> sessionsMap=new ConcurrentHashMap<String, Session>();
	
	// 默认配置的第一个keyspace链接的session
	private Session defaultSession;

	public CassandraClusterSessionManager(CassandraConfig config) {
		super();
		this.config=config;
		this.cluster=this.instanceCluster();
		this.initSession();
	}
	
	private Cluster instanceCluster(){
		Cluster.Builder builder=Cluster.builder();
		CassandraCluster cassandraCluster=this.config.getCassandraCluster();
		builder.addContactPoints(cassandraCluster.getContactPoints());
		builder.withPort(cassandraCluster.getPort());
		builder.withCredentials(cassandraCluster.getUserName(), cassandraCluster.getPassword());
		return builder.build();
	}
	
	private void initSession(){
		CassandraCluster cassandraCluster=this.config.getCassandraCluster();
		String[] keyspaces=cassandraCluster.getKeyspaces();
		if(keyspaces==null||keyspaces.length<1){
			throw new RuntimeException("the config is not config keyspaces!");
		}
		for (String keyspace : keyspaces) {
			Session session=this.instanceSession(keyspace);
			if(this.defaultSession==null){
				this.defaultSession=session;
			}
			this.sessionsMap.put(keyspace, session);
		}
	}
	
	private Session instanceSession(String keyspace){
		Session session=this.cluster.connect(keyspace);
		return session;
	}
	
	public Session getKeyspaceSession(String keyspace){
		if(!this.sessionsMap.containsKey(keyspace)){
			throw new RuntimeException("keyspace:"+keyspace +" is not init session!");
		}
		return this.sessionsMap.get(keyspace);
	}
	
	
	public Session getDefaultSession(){
		if(this.defaultSession==null){
			throw new RuntimeException("the config is not config keyspace! for getSession Method!");
		}
		return this.defaultSession;
	}
	
	
	public void close(){
		for (Entry<String,Session> entry : this.sessionsMap.entrySet()) {
			this.closeSession(entry.getValue());
		}
	}
	
	private void closeSession(Session session){
		try {
			if(session == null || session.isClosed()){
				return ;
			}
			session.close();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		}
	}
	

}
