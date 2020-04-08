package org.corps.db.cassandra.core;

public class CassandraCluster{
	
	private String[] contactPoints;
	
	private int port;
	
	private String userName;
	
	private String password;
	
	private String[] keyspaces;
	

	public CassandraCluster() {
		super();
	}

	public CassandraCluster(String[] contactPoints, int port, String userName, String password,
			String[] keyspaces) {
		super();
		this.contactPoints = contactPoints;
		this.port = port;
		this.userName = userName;
		this.password = password;
		this.keyspaces = keyspaces;
	}

	public String[] getContactPoints() {
		return contactPoints;
	}

	public void setContactPoints(String[] contactPoints) {
		this.contactPoints = contactPoints;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String[] getKeyspaces() {
		return keyspaces;
	}

	public void setKeyspaces(String[] keyspaces) {
		this.keyspaces = keyspaces;
	}
	
	
	
}