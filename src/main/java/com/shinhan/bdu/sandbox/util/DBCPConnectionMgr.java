package com.shinhan.bdu.sandbox.util;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shinhan.bdu.sandbox.step.GetHdfsBizAndFileInfoStep;
import com.sun.org.apache.xpath.internal.operations.Or;

import java.sql.*;
import java.util.Enumeration;
import java.util.Map;

/*
 * [ 사용 예 ]
 * 
 * 			try {
                pstmt = connection.prepareStatement("SELECT * FROM SOME_TABLE WHERE 조건 등등");
                rs = pstmt.executeQuery();

                while (rs.next()) {
                    String gameCode = rs.getString("가져올 Column값(String)");
                    long period = rs.getInt("가져올 Column 값(long)");
                }

            } catch (SQLException e) {
                logger.error("{}", e.getMessage());
            } finally {
                // 매우 중요! Connection을 사용하고 반납을 해야 Pooling이 된다.
                DBManager.getInstance().freeConnection(connection, pstmt, rs);
            }
 * 
 * 
 */

public class DBCPConnectionMgr {

	private final static Logger logger = LoggerFactory.getLogger(DBCPConnectionMgr.class);

    private static DBCPConnectionMgr instance;
    private Map<String, String> config;
    private static String DBType = null;
    
    synchronized public static DBCPConnectionMgr getInstance(String type) {
    	
        try {
            if (instance == null || DBType == null || !(DBType.equals("type"))) {
            	DBType = type;
                instance = new DBCPConnectionMgr(type);
                logger.info("DBManager initialize: {}", instance);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return instance;
    }

    private DBCPConnectionMgr(String type) {
        // Connection을 초기화
        initFirstConnection(type);
        // TODO : 다른 DB 사용 시 해당 DB 초기화.
    }

    private void initFirstConnection(String type){
    	// Config Setting
    	if (type.toLowerCase().equals("impala"))
    		config = MetaReadUtil.getInstance().readImpalaConfig();
    	else if (type.toLowerCase().equals("maria"))
    		config = MetaReadUtil.getInstance().readMariaConfig();
    	else
    		config = MetaReadUtil.getInstance().readImpalaConfig();
        try {
        	setupFirstDriver();
        	logger.info("[ FirstConnection Created ]");
        } catch (Exception ex) {
            logger.error("DBException : {}", ex.getMessage());
        }
        
    }

    /**
     * Comeback Player Connection을 리턴함
     *
     * @return DB Connection
     */
    public Connection getConnection() {
        Connection con = null;
        try {
            con = DriverManager.getConnection(config.get("dbRemoteURL"),
							            	  config.get("username"),
							            	  config.get("password"));
        } catch (SQLException ex) {
            logger.error("SQLException : {}", ex.getMessage());
        }
        return con;
    }
    
    /**
     * 
     *  Connection Pool 옵션 세팅
     */
    private GenericObjectPool setConnectionConfig(GenericObjectPool pool) {
    	if (config.containsKey("MaxActive")) 
    		pool.setMaxActive(Integer.parseInt(config.get("MaxActive")));
        else 
        	pool.setMaxActive(45);
    	if (config.containsKey("MinIdle")) 
    		pool.setMinIdle(Integer.parseInt(config.get("MinIdle")));
        else 
        	pool.setMinIdle(4);
    	if (config.containsKey("MaxWait")) 
    		pool.setMaxWait(Integer.parseInt(config.get("MaxWait")));
        else 
        	pool.setMaxWait(15000);
    	if (config.containsKey("TimeBetweenEvictionRunsMillis")) 
    		pool.setTimeBetweenEvictionRunsMillis(Integer.parseInt(config.get("TimeBetweenEvictionRunsMillis")));
        else 
        	pool.setTimeBetweenEvictionRunsMillis(15000);
    	if (config.containsKey("MinEvictableIdleTimeMillis")) 
    		pool.setMinEvictableIdleTimeMillis(Integer.parseInt(config.get("MinEvictableIdleTimeMillis")));
        else 
        	pool.setMinEvictableIdleTimeMillis(15000);
    	if (config.containsKey("MaxIdle")) 
    		pool.setMaxIdle(Integer.parseInt(config.get("MaxIdle")));
        else 
        	pool.setMaxIdle(15000);
    	if (config.containsKey("TestOnBorrow")) 
    		pool.setTestOnBorrow(Boolean.parseBoolean(config.get("TestOnBorrow")));
        else 
        	pool.setTestOnBorrow(true);
    	
    	return pool;
    }
    
    private void freeExistDriver() {
    	Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            try {
                DriverManager.deregisterDriver(driver);
                System.out.println("*** deregistering jdbc driver: " + driver);
            } catch (SQLException e) {
            	e.printStackTrace();
            }
        }
    }
    
    /**
     *  Connection Pool 설정
     *
     * @throws Exception
     */
    private void setupFirstDriver() {
        // JDBC 드라이버 로딩(Impala 드라이버를 가져옴. 사용하는 jdbc 드라이버를 로드하면 됨
    	try {
    		Class.forName(config.get("jdbcDriver"));
    	} catch (ClassNotFoundException ex) {
            // TODO: handle exception
            logger.error("Fail to load JDBC Driver");
            logger.error("{}", ex.getMessage());
        }

        // Connection Pool 생성, 옵션세팅
        GenericObjectPool connectionPool = new GenericObjectPool(null);
        connectionPool = setConnectionConfig(connectionPool);
        
        // 실제 DB와의 커넥션을 연결해주는 팩토리 생성
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
                config.get("dbRemoteURL"), // JDBC URL
                config.get("username"), // 사용자
                config.get("password"));
        
        // Connection Pool이 PoolableConnection 객체를 생성할 때 사용할
        // PoolableConnectionFactory 생성
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
                connectionFactory,
                connectionPool,
                null, // statement pool
                "SELECT 1", // 커넥션 테스트 쿼리: 커넥션이 유효한지 테스트할 때 사용되는 쿼리.
                false, // read only 여부
                false); // auto commit 여부

        // Pooling을 위한 JDBC 드라이버 생성 및 등록
        PoolingDriver driver = new PoolingDriver();

        // JDBC 드라이버에 커넥션 풀 등록
        driver.registerPool("first_connection", connectionPool);
    }

    /*
        Connection Pool에 free 및 객체 소멸 함수들
     */
    public void freeConnection(Connection con, PreparedStatement pstmt, ResultSet rs) {
        try {
            if (rs != null) rs.close();
            if (pstmt != null) pstmt.close();
            freeConnection(con);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void freeConnection(Connection con, Statement stmt, ResultSet rs) {
        try {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            freeConnection(con);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void freeConnection(Connection con, PreparedStatement pstmt) {
        try {
            if (pstmt != null) pstmt.close();
            freeConnection(con);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void freeConnection(Connection con, Statement stmt) {
        try {
            if (stmt != null) stmt.close();
            freeConnection(con);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void freeConnection(Connection con) {
        try {
            if (con != null) con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void freeConnection(Statement stmt) {
        try {
            if (stmt != null) stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void freeConnection(PreparedStatement pstmt) {
        try {
            if (pstmt != null) pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void freeConnection(ResultSet rs) {
        try {
            if (rs != null) rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}