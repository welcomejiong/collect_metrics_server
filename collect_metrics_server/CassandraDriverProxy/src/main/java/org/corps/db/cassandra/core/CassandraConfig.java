package org.corps.db.cassandra.core;


public class CassandraConfig {
	
	private ServerInfo serverInfo;
	
	private CassandraCluster cassandraCluster;
	
	public CassandraConfig() {
		super();
	}

	public CassandraConfig(ServerInfo server, CassandraCluster cassandraCluster) {
		super();
		this.serverInfo = server;
		this.cassandraCluster = cassandraCluster;
	}

	public ServerInfo getServerInfo() {
		return serverInfo;
	}

	public void setServerInfo(ServerInfo server) {
		this.serverInfo = server;
	}

	public CassandraCluster getCassandraCluster() {
		return cassandraCluster;
	}

	public void setCassandraCluster(CassandraCluster cassandraCluster) {
		this.cassandraCluster = cassandraCluster;
	}

}
