package scheduling.dynamicScheduler.repository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Repository;
import scheduling.dynamicScheduler.dto.ConnectionDTO;
import org.springframework.jdbc.support.JdbcUtils;
import java.sql.*;
import java.util.*;

@Slf4j
@Repository
public class TableRepository {
    @Autowired
    private Environment environment;

    public List<Map<String, Object>> tableData(String sql) throws SQLException {
        return tableData(sql, null);
    }

    public List<Map<String, Object>> tableData(String sql, ConnectionDTO connect) throws SQLException {

        List<Map<String, Object>> dataList = new ArrayList<>();

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection(connect);
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                Map<String, Object> tableMappingInfo = new HashMap<String, Object>();
                ResultSetMetaData rsmd = rs.getMetaData();

                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String colName = rsmd.getColumnName(i);
                    tableMappingInfo.put(colName, rs.getObject(colName));  //컬럼의 타입이 각각 들어오는것마다 다를수 있기 때문에 getObject형태로 가져오기
                }
                dataList.add(tableMappingInfo);
            }

        } catch (SQLException e) {
            log.error("db error", e);
            throw e;
        } finally {
            close(con, pstmt, rs);
        }
        return dataList;
    }

    public void targetDataInsert(Map<String, Object> source, String targetTableName, List<String> targetColumnName, List<String> sourceColumnName, ConnectionDTO connection) throws SQLException {
        String setInsertColumn = "";
        List setTargetColumnValue = new ArrayList();
        List setSourceColumnValue = new ArrayList();

        for(int i = 0; i < targetColumnName.size(); i++){
            if( i == targetColumnName.size()-1){
                setInsertColumn += targetColumnName.get(i);
            } else {
                setInsertColumn += targetColumnName.get(i) + ", ";
            }
        }
        String sql = "INSERT INTO " + targetTableName + "(" + setInsertColumn + ") VALUES (?,?,?)";
        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = getConnection(connection);
            pstmt = con.prepareStatement(sql);

            for(int i=0; i<targetColumnName.size(); i++) {
                setTargetColumnValue.add(targetColumnName.get(i));
            }
            for(int i=0; i<sourceColumnName.size(); i++) {
                setSourceColumnValue.add(sourceColumnName.get(i));
            }

            for(int i = 0; i < setTargetColumnValue.size(); i++){
                if(source.get(String.valueOf(setSourceColumnValue.get(i))) instanceof String){
                    pstmt.setString(i+1, String.valueOf(source.get(String.valueOf(setSourceColumnValue.get(i)))));
                } else if(source.get(String.valueOf(setSourceColumnValue.get(i))) instanceof Integer){
                    pstmt.setInt(i+1, (int)source.get(String.valueOf(setSourceColumnValue.get(i))));
                }

            }
            int resultSize = pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("db error", e);
            throw e;
        } finally {
            close(con, pstmt, null);
        }
    }

    public void targetDataUpdate(Map<String, Object> source, String targetTableName, List<String> targetColumnName, List<String> sourceColumnName, Map<String, Object> pkColumn, ConnectionDTO connection) throws SQLException {
        String setUpdateColumn = "";
        List setUpdateColumnValue = new ArrayList();
        String targetPrimaryKey = String.valueOf(pkColumn.get("TARGET_COLUMNNAME"));
        String sourcePrimaryKey = String.valueOf(pkColumn.get("SOURCE_COLUMNNAME"));
        String setPrimaryColumn = targetPrimaryKey + "=?";
        for(int i=0; i<targetColumnName.size(); i++){
            if(targetColumnName.get(i).equals(targetPrimaryKey)){
                continue;
            }
            if(i < targetColumnName.size()-2){
                setUpdateColumn += targetColumnName.get(i) + "=?, ";
            } else {
                setUpdateColumn += targetColumnName.get(i) + "=?";
            }

        }
        for(int i=0; i<sourceColumnName.size(); i++){
            if(!sourceColumnName.get(i).equals(sourcePrimaryKey)){
                setUpdateColumnValue.add(sourceColumnName.get(i));
            }
        }

        String sql = "UPDATE " + targetTableName + " SET " + setUpdateColumn + " WHERE " +  setPrimaryColumn;
        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = getConnection(connection);
            pstmt = con.prepareStatement(sql);
            for(int i = 0; i < setUpdateColumnValue.size(); i++){
                if(source.get(String.valueOf(setUpdateColumnValue.get(i))) instanceof String){
                    pstmt.setString(i+1, String.valueOf(source.get(String.valueOf(setUpdateColumnValue.get(i)))));
                } else if(source.get(String.valueOf(setUpdateColumnValue.get(i))) instanceof Integer){
                    pstmt.setInt(i+1, (int)source.get(String.valueOf(setUpdateColumnValue.get(i))));
                }
            }
            if(source.get(sourcePrimaryKey) instanceof String){
                pstmt.setString(setUpdateColumnValue.size()+1, String.valueOf(source.get(sourcePrimaryKey)));
            } else if(source.get(sourcePrimaryKey) instanceof Integer) {
                pstmt.setInt(setUpdateColumnValue.size()+1, (int)source.get(sourcePrimaryKey));
            }

            int resultSize = pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error("db error", e);
            throw e;
        } finally {
            close(con, pstmt, null);
        }
    }

    public Connection getConnection(ConnectionDTO connect) {

        try {
            String url = environment.getProperty("spring.datasource.url");
            String username = environment.getProperty("spring.datasource.username");
            String password = environment.getProperty("spring.datasource.password");
            if(connect != null){
                url = String.valueOf(connect.getConnectionUrl());
                username = String.valueOf(connect.getConnectionID());
                password = String.valueOf(connect.getConnectionPW());
            }

            Connection connection = DriverManager.getConnection(url, username, password);

            return connection;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void close(Connection con, Statement stmt, ResultSet rs) {
        JdbcUtils.closeResultSet(rs);
        JdbcUtils.closeStatement(stmt);
        JdbcUtils.closeConnection(con);
    }
}