package com.shinhan.bdu.sandbox.step;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.annotations.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.util.MetaReadUtil;
import com.shinhan.bdu.sandbox.util.QueryConverter;
import com.shinhan.bdu.sandbox.util.StaticValues;
import com.shinhan.bdu.sandbox.pipe.PipeProducer;
import com.shinhan.bdu.sandbox.util.DBCPConnectionMgr;
/**
 *  
 * @desc query base로 table의 usage를 얻어내 step
 * @dependency Origin Query + Dynamic Query, ImpalaConnection + DBCP
 *
 */
public class GetTableUsageStep implements Step<List<Map>, List<Map>> {
	
	private final Logger logger = LoggerFactory.getLogger(PipeProducer.class);
	
	private String getQuery(List<Map> input) throws NullPointerException{
		String qry = (String) input.get(0).get("query");
		String originQuery = MetaReadUtil.getInstance().readSql(qry);
		String reltime = (String) input.get(0).get("reltime");
		String relTimeQuery = QueryConverter.getInstance().convertReltime(originQuery, reltime);
		Map<String, String> cvtmap = (Map<String, String>) input.get(0).get("convert");
		String cvtQuery = QueryConverter.getInstance().convert(relTimeQuery, cvtmap);
		return cvtQuery;
	}
	
	@Override
	public List<Map> process(List<Map> input) throws StepException {
		
		List<Map<String, Map<String, String>>> usageMaps = new ArrayList<Map<String, Map<String, String>>>();
		
		String query = this.getQuery(input);
		Connection connection = DBCPConnectionMgr.getInstance("impala").getConnection();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd;
		try {
			logger.info(" *** excuted query : {} ",  query);
			pstmt = connection.prepareStatement(query);
		    rs = pstmt.executeQuery();

            while (rs.next()) {
        		Map<String, Map<String, String>> rowMap = new HashMap<String, Map<String, String>>();
				String key = rs.getObject("sb_nm").toString() + StaticValues.KEY_OFFSET + rs.getObject("tbl_nm").toString();
				Map<String, String> rowContents = new HashMap<String, String>();
				rowContents.put("useCount", rs.getObject("cnt").toString());
				rowContents.put("lastDataUseTime", rs.getObject("last_dttm").toString());
				rowMap.put(key, rowContents);
				usageMaps.add(rowMap);
				
            }
            
        } catch (SQLException ex) {
            logger.error("{}", ex.getMessage());
        } finally {
            // 매우 중요! Connection을 사용하고 반납을 해야 Pooling이 된다.
        	DBCPConnectionMgr.getInstance("impala").freeConnection(connection, pstmt, rs);
        }
		
		
		logger.info("*** table usage : get " + usageMaps.size() + " item's data");
		return post(input, usageMaps) ;
	}

	public List<Map> post(List<Map> input, Object data) throws StepException {
		input.get(0).put("output", data); 
		input.get(0).put("status", "finish"); 
		input.add(input.remove(0));
		return input;
	}

}
