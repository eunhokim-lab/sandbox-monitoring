package com.shinhan.bdu.sandbox.step.service;
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
import com.shinhan.bdu.sandbox.db.DbHandler;
import com.shinhan.bdu.sandbox.exception.ExceptionHandler;
import com.shinhan.bdu.sandbox.pipe.PipeProducer;
import com.shinhan.bdu.sandbox.step.abstact.DbAccessStepImpl;
import com.shinhan.bdu.sandbox.util.DBCPConnectionMgr;
/**
 *  
 * @desc query base로 table의 usage를 얻어내 step
 * @dependency Origin Query + Dynamic Query, ImpalaConnection + DBCP
 *
 */
public class GetTableUsageStep extends DbAccessStepImpl {
	
	private final Logger logger = LoggerFactory.getLogger(PipeProducer.class);
	
	@Override
	protected String getQuery(List<Map> input) throws NullPointerException{
		String qry = (String) input.get(0).get("query");
		String originQuery = MetaReadUtil.getInstance().readSql(qry);
		String reltime = (String) input.get(0).get("reltime");
		String relTimeQuery = QueryConverter.getInstance().convertReltime(originQuery, reltime);
		Map<String, String> cvtmap = (Map<String, String>) input.get(0).get("convert");
		String cvtQuery = QueryConverter.getInstance().convert(relTimeQuery, cvtmap);
		return cvtQuery;
	}
	
	@Override
	public Object logic(List<Map> input) throws StepException {
		
		List<Map<String, Map<String, String>>> usageMaps = new ArrayList<Map<String, Map<String, String>>>();
		DbHandler dbh = new DbHandler("impala");
		String query = this.getQuery(input);
        try {
        	ResultSet rs = dbh.readData(query);
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
			logger.error("{}", new ExceptionHandler().getPrintStackTrace(ex));
		} finally {
			 dbh.freeDbIns();
	    }
		
		logger.info("*** table usage : get " + usageMaps.size() + " item's data");
		return usageMaps ;
	}

}
