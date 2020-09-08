package com.shinhan.bdu.sandbox.step;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
public class InsertDbSandboxQuotaStep implements Step<List<Map>, List<Map>> {
	private final Logger logger = LoggerFactory.getLogger(InsertDbSandboxQuotaStep.class);
	private String getInsertQuery(List<Map> input) {
		String qry = (String) input.get(0).get("query");
		return MetaReadUtil.getInstance().readSql(qry);
	}
	
	private List<ArrayList<String>> getInsertData(List<Map> input) {
		Map<String, Map<String, String>> preData = (Map<String, Map<String, String>>) input.get(input.size()-1).get("output");
		List<ArrayList<String>> insertData = new ArrayList<ArrayList<String>>();
		for(String key : preData.keySet()) {
			ArrayList<String> rowData = new ArrayList<String>();
			rowData.add(key); // sandbox name
			rowData.add(preData.get(key).get("quota"));
			rowData.add(preData.get(key).get("spaceConsumed"));
			rowData.add(preData.get(key).get("spaceQuota"));
			insertData.add(rowData);
		}
		return insertData;
	}
	 
	@Override
	public List<Map> process(List<Map> input) throws StepException {
		
		String query = this.getInsertQuery(input);
		List<ArrayList<String>> insertData = this.getInsertData(input);
		Connection connection = DBCPConnectionMgr.getInstance("maria").getConnection();
		logger.info("* connection : " + connection);
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ResultSetMetaData rsmd;
		try {
			logger.info(" ** excuted query : " + query);
			pstmt = connection.prepareStatement(query);
			
			for(int i = 0; i < insertData.size(); i++){
				ArrayList<String> row = insertData.get(i);
				pstmt.setString(1, row.get(0));
				for(int index = 1; index < row.size(); index++){
					pstmt.setLong(index+1, Long.parseLong(row.get(index)));
				}
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				Date today = new Date ();
				Date myDate = null;
				try {
					myDate = formatter.parse(formatter.format(today));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				java.sql.Date sqlDate = new java.sql.Date(myDate.getTime());
				pstmt.setDate(row.size()+1, sqlDate);
				
                pstmt.addBatch();
                pstmt.clearParameters() ;
                 
                if( (i % Integer.parseInt((String) input.get(0).get("batch"))) == 0){
                    pstmt.executeBatch() ;
                    pstmt.clearBatch();
                    connection.commit() ;
                }
            }
            // 커밋되지 못한 나머지 구문에 대하여 커밋
            pstmt.executeBatch() ;
            connection.commit() ;

        } catch (SQLException ex) {
            logger.error("{}", ex.getMessage());
        } finally {
            // 매우 중요! Connection을 사용하고 반납을 해야 Pooling이 된다.
        	DBCPConnectionMgr.getInstance("maria").freeConnection(connection, pstmt, rs);
        }
		logger.info("*** mariadb insert end");
		return post(input, null) ;
	}

	public List<Map> post(List<Map> input, Object data) throws StepException {
		if (data == null) {
			data = input.get(input.size()-1).get("output");
		}
		input.get(0).put("output", data); 
		input.get(0).put("status", "finish"); 
		input.add(input.remove(0));
		return input;
	}

}
