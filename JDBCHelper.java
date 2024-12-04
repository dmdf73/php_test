import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class JDBCHelper {
    private static final int DEFAULT_BATCH_SIZE = 1500;
    private static final String DEFAULT_SQL_END = "";

    public static ResultSet getSql(Connection conn, String sql, String[] parameters) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 0; i < parameters.length; i++) {
            pstmt.setString(i + 1, parameters[i]);
        }
        return pstmt.executeQuery();
    }

    public static String getOneSql(Connection conn, String sql, String[] parameters) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                pstmt.setString(i + 1, parameters[i]);
            }
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            }
        }
    }

    public static void doSql(Connection conn, String sql, String[] parameters) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                pstmt.setString(i + 1, parameters[i]);
            }
            pstmt.execute();
        }
    }

    public static void executeBulkInsert(Connection conn, String sql, Iterator<String[]> values, int columnCount) throws SQLException {
        executeBulkInsert(conn, sql, values, columnCount, DEFAULT_BATCH_SIZE, DEFAULT_SQL_END);
    }

    public static void executeBulkInsert(Connection conn, String sql, Iterator<String[]> values, int columnCount, int batchSize) throws SQLException {
        executeBulkInsert(conn, sql, values, columnCount, batchSize, DEFAULT_SQL_END);
    }

    public static void executeBulkInsert(Connection conn, String sql, Iterator<String[]> values, int columnCount, String sqlEnd) throws SQLException {
        executeBulkInsert(conn, sql, values, columnCount, DEFAULT_BATCH_SIZE, sqlEnd);
    }

    public static void executeBulkInsert(Connection conn, String sql, Iterator<String[]> values, int columnCount, int batchSize, String sqlEnd) throws SQLException {
        while (values.hasNext()) {
            List<String[]> batch = new ArrayList<>();
            while (values.hasNext() && batch.size() < batchSize) {
                batch.add(values.next());
            }
            if (!batch.isEmpty()) {
                String batchSql = sql + " " + generateBulkInsertSQL(batch.size(), columnCount) + " " + sqlEnd;
                try (PreparedStatement pstmt = conn.prepareStatement(batchSql)) {
                    int parameterIndex = 1;
                    for (String[] row : batch) {
                        for (String value : row) {
                            pstmt.setString(parameterIndex++, value);
                        }
                    }
                    pstmt.executeUpdate();
                }
            }
        }
    }

    private static String generateBulkInsertSQL(int rowCount, int columnCount) {
        StringBuilder bulkSql = new StringBuilder();
        bulkSql.append("VALUES ");
        String questionMarks = generateQuestionMarks(columnCount);
        for (int i = 0; i < rowCount; i++) {
            bulkSql.append(questionMarks);
            if (i < rowCount - 1) {
                bulkSql.append(", ");
            }
        }
        return bulkSql.toString();
    }

    private static String generateQuestionMarks(int columnCount) {
        StringBuilder marks = new StringBuilder("(");
        for (int i = 0; i < columnCount; i++) {
            marks.append("?");
            if (i < columnCount - 1) {
                marks.append(", ");
            }
        }
        marks.append(")");
        return marks.toString();
    }

    public static HashMap<String, String> loadHashMap(Connection conn, String sql) throws SQLException {
        HashMap<String, String> resultMap = new HashMap<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.getMetaData().getColumnCount() != 2) {
                throw new IllegalArgumentException("SQL must return exactly 2 columns");
            }
            while (rs.next()) {
                String key = rs.getString(1);
                String value = rs.getString(2);
                resultMap.put(key, value);
            }
        }
        return resultMap;
    }
}
