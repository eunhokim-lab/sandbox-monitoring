package com.shinhan.bdu.sandbox.step.persistence;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.shinhan.bdu.sandbox.db.DbHandler;
import com.shinhan.bdu.sandbox.pipe.PipeProducer;
import com.shinhan.bdu.sandbox.step.prd.DbAccessStepImpl;
import com.shinhan.bdu.sandbox.step.prd.InsertDbAccessStepImpl;
import com.shinhan.bdu.sandbox.step.prd.Step;
import com.shinhan.bdu.sandbox.util.DBCPConnectionMgr;
/**
 *  
 * @desc query base로 table의 usage를 얻어내 step
 * @dependency Origin Query + Dynamic Query, ImpalaConnection + DBCP
 *
 */
public class InsertDbBizAndFileStep extends InsertDbAccessStepImpl {
	private final Logger logger = LoggerFactory.getLogger(InsertDbBizAndFileStep.class);
	
	@Override
	protected List<ArrayList<String>> getInsertDataFromPreSteps(List<Map> input) {
		Map<String, Map<String, String>> preData = (Map<String, Map<String, String>>) input.get(input.size()-1).get("output");
		List<ArrayList<String>> insertData = new ArrayList<ArrayList<String>>();
		for(String key : preData.keySet()) {
			ArrayList<String> rowData = new ArrayList<String>();
			rowData.add(key); // area name
			if(key.toLowerCase().contains("filearea")) rowData.add("file");
			else rowData.add("db");
			rowData.add(preData.get(key).get("sizeUsed"));
			rowData.add(preData.get(key).get("spaceConsumed"));
			insertData.add(rowData);
		}
		return insertData;
	}
	
	@Override
	public List<Map> logic(List<Map> input) throws StepException {
		
		String query = this.getQuery(input);
		List<ArrayList<String>> insertData = this.getInsertDataFromPreSteps(input);
		DbHandler dbh = new DbHandler("maria");
		ArrayList<String> insertMeta = (ArrayList<String>) input.get(0).get("metas");
		try {
			dbh.InsertData(insertData, insertMeta, query
					     , Integer.parseInt((String) input.get(0).get("batch")));
        } catch (SQLException | NumberFormatException | ParseException ex) {
            logger.error("{}", ex.getMessage());
        } finally {
        	dbh.freeDbIns();
        }
		logger.info("*** mariadb insert end");
		return null;
	}
	

}
