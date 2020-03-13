package org.corps.bi.datacenter.connect.hive;

import java.util.Map;
import java.util.Map.Entry;

import org.corps.bi.tools.util.JSONUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MetricMessDaoImpl implements MetricMessDao{
	
	private static final Logger LOGGER=LoggerFactory.getLogger(MetricMessDaoImpl.class);

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Override
	public void addPartition(String table,Map<String, String> paritionsMap) {
		
		StringBuilder sb=new StringBuilder("ALTER TABLE ");
		sb.append(table).append(" ADD PARTITION (");
		int i=0;
		int t=paritionsMap.size();
		for (Entry<String,String> entry : paritionsMap.entrySet()) {
			sb.append(entry.getKey()).append("=:"+entry.getKey());
			if(i<t-1) {
				sb.append(",");
			}
			i++;
		}
		sb.append(")");
		
		String sql=sb.toString();
		
		try {
			
			int ret=namedParameterJdbcTemplate.update(sql, paritionsMap);
			
			if(ret>=0) {
				LOGGER.info("sql:{} params:{} add partition succ.",sql,JSONUtils.toJSON(paritionsMap));
			}
		} catch (DataAccessException e) {
			
			if(e.getMessage().contains("org.apache.hadoop.hive.ql.exec.DDLTask. AlreadyExistsException")) {
				LOGGER.info("sql:{} params:{} partition already exists.",sql,JSONUtils.toJSON(paritionsMap));
			}else {
				LOGGER.error(e.getMessage(),e);
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
		} 
		
	}

}
