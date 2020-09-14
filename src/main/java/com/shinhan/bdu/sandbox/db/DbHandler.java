package com.shinhan.bdu.sandbox.db;

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
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.exception.ExceptionHandler;
import com.shinhan.bdu.sandbox.util.DBCPConnectionMgr;
import com.sun.javadoc.ThrowsTag;

public class DbHandler {
	
	private final Logger logger = LoggerFactory.getLogger(DbHandler.class);
	
	private Connection connection = null;
	private PreparedStatement pstmt = null;
	private ResultSet rs = null;
	private ResultSetMetaData rsmd;
	private String dbType = null;
	
	public DbHandler(String dbType) {
		this.dbType = dbType;
		this.connection = DBCPConnectionMgr.getInstance(dbType).getConnection();
		logger.info("* connection : " + connection);
	}
	
	private void assertInput(String query) {
		if (query == null) {
			logger.error("[ DbHandler::DB Acess ] : query is null ");
			throw new NullPointerException();
		}
		if (query.length() < 8) {
			logger.error("[ DbHandler::DB Acess ] : query size is too short : {}", query);
			throw new NullPointerException();
		}
	}
	
	private void assertInput(List<ArrayList<String>> insertData, List<String> insertMeta) {
		if (insertData == null) {
			logger.error("[ DbHandler::DB Acess ] : insertData is null ");
			throw new NullPointerException();
		}
		if (insertData.size() == 0) {
			logger.warn("[ DbHandler::DB Acess ] : insertData size is 0 : {}");
		}
		if (insertMeta == null) {
			logger.error("[ DbHandler::DB Acess ] : insertMeta is null ");
			throw new NullPointerException();
		}
		if (insertMeta.size() == 0) {
			logger.warn("[ DbHandler::DB Acess ] : insertMeta size is 0 : {}");
		}
		if (insertData.size() != insertMeta.size()) {
			logger.error("[ DbHandler::DB Acess ] : insertMeta and insertData's size are not matched");
			throw new NullPointerException();
		}
	}
	
	
	public ResultSet readData(String query) throws SQLException{
		assertInput(query);
		logger.debug("[ DbHandler : readQuery ]");
		logger.info(" *** excuted query : {} ",  query);
		pstmt = connection.prepareStatement(query);
		rs = pstmt.executeQuery();
	    return rs;
	}
	
	public void InsertData(List<ArrayList<String>> insertData, List<String> insertMeta
			                , String query, int batch) throws SQLException, ParseException {
		assertInput(query);
		boolean nowDt = isLastDataOperDate(insertData, (ArrayList<String>) insertMeta);
		if (!nowDt)
			assertInput(insertData, insertMeta);
		logger.debug("[ DbHandler : insertData ]");
		logger.info(" *** excuted query : {} ",  query);
		pstmt = connection.prepareStatement(query);
		for (int index = 0; index < insertData.size(); index++) {
			ArrayList<String> row = insertData.get(index);
			for (int i = 0; i < row.size(); i++) {
				setInsertPreStatement(pstmt, insertMeta.get(i), row.get(i), (i+1));
			}
			if(nowDt)
				setInsertPreStatement(pstmt, "date", null, row.size()+1);
			pstmt.addBatch();
	        pstmt.clearParameters() ;
	        if( (index % batch) == 0){
	            pstmt.executeBatch();
	            pstmt.clearBatch();
	            connection.commit();
	        }
		}
		pstmt.executeBatch() ;
        connection.commit() ;
	}
	
	private PreparedStatement setInsertPreStatement(PreparedStatement pstmt, String type
			                                      , String data, int index) throws SQLException, ParseException {
		if (type.toLowerCase().equals("string")) 
			setDbString(pstmt, data, index);
		else if (type.toLowerCase().equals("double")) 
			setDbDouble(pstmt, Double.parseDouble(data), index);
		else if (type.toLowerCase().equals("date")) 
			setDbDate(pstmt, getNowSqlData(), index);
		else if (type.toLowerCase().equals("long")) 
			setDbLong(pstmt, Long.parseLong(data), index);
		else if (type.toLowerCase().equals("time") || type.toLowerCase().equals("datetime")) 
			setTimestamp(pstmt, cvtStrigToTimeStamp(data), index);
		else
			throw new SQLException();
			
		return pstmt;
	}
	
	private void setDbString(PreparedStatement pstmt, String data, int index) throws SQLException {
		pstmt.setString(index, data);
	}
	private void setDbDouble(PreparedStatement pstmt, Double data, int index) throws SQLException {
		pstmt.setDouble(index, data);
	}
	private void setDbDate(PreparedStatement pstmt, java.sql.Date data, int index) throws SQLException {
		pstmt.setDate(index, data);
	}
	private void setDbLong(PreparedStatement pstmt, long data, int index) throws SQLException {
		pstmt.setLong(index, data);
	}
	private void setTimestamp(PreparedStatement pstmt, java.sql.Timestamp data, int index) throws SQLException {
		pstmt.setTimestamp(index, data);
	}
	
	private boolean isLastDataOperDate(List<ArrayList<String>> insertData, ArrayList<String> insertMeta) {
		return (insertData.get(0).size() !=  insertMeta.size()) &&
	           (insertMeta.get(insertMeta.size()-1).toLowerCase().equals("date"));
	}
	
	
	public void freeDbIns() {
		logger.debug("[ DbHandler : freeDbIns : {}] ", dbType);
		DBCPConnectionMgr.getInstance(dbType).freeConnection(connection, pstmt, rs);
	}
	
	private java.sql.Timestamp cvtStrigToTimeStamp(String time) throws ParseException{
		if (time == null || time.length() < 1 )
			return null;
		SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date datetime = isoFormat.parse(time);
		return new java.sql.Timestamp(datetime.getTime());
	}
	
	public java.sql.Date getNowSqlData() {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date ();
		Date myDate = null;
		try {
			myDate = formatter.parse(formatter.format(today));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new java.sql.Date(myDate.getTime());
		
	}
	
}
