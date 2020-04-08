import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.corps.db.cassandra.CassandraClusterSessionManager;
import org.corps.db.cassandra.core.CassandraConfig;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.kenai.constantine.Constant;

public class CassandraTest {
	
	private CassandraClusterSessionManager cassandraClusterSessionManager;
	
	private Session session;
	
	public CassandraTest() {
		super();
		//Constant.init();
		CassandraConfig config=new CassandraConfig();
		this.cassandraClusterSessionManager=new CassandraClusterSessionManager(config);
		this.session=this.cassandraClusterSessionManager.getDefaultSession();
	}
	@Test
	public void testInsert(){
		String sql="insert into back_main_data(snid,gameid ,serverid ,userid ,ds ,artifactlevel ,back ,bossequips ,chariot ,ding ,equipgem ,extra ,flagofficialdata ,gold ,honor ,itembag ,jinxing ,junliang ,officers ,petdata ,refiningattacklevel ,reputation ,secondarycurrenynum ,skillcounts ,soldiermatrixlevel ,soldiernum ,soldierskill ,soldierweapons ,supplysoldiers ,talent ,unicorns ,updatedate ,usehuanglongnum ,zhugenu ,zuoqis)";
		sql+=" values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement paPreparedStatement=this.session.prepare(sql);
		Random random=new Random();
		for(int i=0;i<10;i++){
			BatchStatement batchStatement=new BatchStatement();
			for(int j=0;j<10;j++){
				BoundStatement boundStatement=paPreparedStatement.bind();
				String[] cols=new String[]{
						"snid","gameid","serverid","userid","ds","artifactlevel","back","bossequips","chariot","ding","equipgem","extra","flagofficialdata","gold","honor","itembag","jinxing","junliang","officers","petdata","refiningattacklevel","reputation","secondarycurrenynum","skillcounts","soldiermatrixlevel","soldiernum","soldierskill","soldierweapons","supplysoldiers","talent","unicorns","updatedate","usehuanglongnum","zhugenu","zuoqis"
				};
				
				for (String col : cols) {
					if("snid".equals(col)||"gameid".equals(col)){
						boundStatement.setInt(col, i);
					}else if("jinxing".equals(col)||
							"refiningattacklevel".equals(col)||"reputation".equals(col)||"secondarycurrenynum".equals(col)||
							"soldiermatrixlevel".equals(col)){
						boundStatement.setInt(col, random.nextInt(10000000));
					}
					else if("gold".equals(col)||"honor".equals(col)||"supplysoldiers".equals(col)){
						boundStatement.setLong(col, random.nextLong());
					}else{
						boundStatement.setString(col, random.nextInt(990000000)+"");
					}
					
				}
				batchStatement.add(boundStatement);
			}
			this.session.execute(batchStatement);
			System.out.println("snid:"+i+" gameid:"+i);
		}
		
		
	}
	
	@Test
	public void testQuery(){
		String sql="select * from back_main_data where snid=? and gameid=? and serverid=? and userid=? and ds=?";
		PreparedStatement preparedStatement=this.session.prepare(sql);
		BoundStatement boundStatement=preparedStatement.bind();
		boundStatement.setInt("snid", 8664615);
		boundStatement.setInt("gameid", 8765548);
		boundStatement.setString("serverid", "102539477");
		boundStatement.setString("userid", "231812879");
		boundStatement.setString("ds", "595076626");
		
		ResultSet res=this.session.execute(boundStatement);
		for (Row row : res.all()) {
			System.out.println(row);
		}
	}
	
	@Test
	public void testQuery2(){
		String sql="select * from back_main_data where snid=:snid and gameid=:gameid and serverid=:serverid and userid=:userid and ds=:ds";
		
		Map<String,Object> params=new HashMap<String,Object>();
		params.put("snid", 8664615);
		params.put("gameid", 8765548);
		params.put("serverid", "102539477");
		params.put("userid", "231812879");
		params.put("ds", "595076626");
		
		
		ResultSet res=this.session.execute(sql,params);
		for (Row row : res.all()) {
			System.out.println(row);
		}
	}

}
