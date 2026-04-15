import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class AviationDataLoader {

    // MySQL 连接配置
    private static final String URL = "jdbc:mysql://localhost:3306/project1?characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true";
    private static final String USER = "checker";
    private static final String PWD = "123456";
    private static Connection conn = null;

    // 批量提交的阈值
    private static final int BATCH_SIZE = 5000;

    public static void main(String[] args) {
        System.out.println("=========================================");
        System.out.println("Starting Aviation Data Load (MySQL)");
        System.out.println("=========================================\n");

        long totalStartTime = System.currentTimeMillis();

        if (!connectDB()) {
            System.out.println("Failed to connect to database. Exiting...");
            return;
        }

        try {
            // 开启事务，关闭自动提交
            conn.setAutoCommit(false);

            // 统一的流线型导入逻辑
            loadRegion("data/clean_region.csv");
            loadPassenger("data/clean_passenger.csv");
            loadAirline("data/clean_airline.csv");
            loadAirport("data/clean_airport.csv");
            loadRoute("data/clean_route.csv");
            loadFlight("data/clean_flight.csv");
            loadFlightCabin("data/clean_flight_cabin.csv");

            conn.commit();
            System.out.println("\nAll transactions committed successfully!");

        } catch (SQLException e) {
            System.out.println("\nDatabase Transaction Error: " + e.getMessage());
            try {
                if (conn != null) conn.rollback();
                System.out.println("Transaction rolled back to ensure data integrity.");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            closeDB();
        }

        long totalEndTime = System.currentTimeMillis();
        System.out.println("=========================================");
        System.out.printf("Total Time Elapsed: %.3f seconds.\n", (totalEndTime - totalStartTime) / 1000.0);
        System.out.println("=========================================");
    }

    // ---------------------------------------------------------
    // 各表的具体导入逻辑 (全部精简为一行 SQL + 数据绑定)
    // ---------------------------------------------------------

    private static void loadRegion(String filePath) throws SQLException {
        String sql = "INSERT IGNORE INTO region (region_code, region_name) VALUES (?, ?)";
        executeBatchLoad(filePath, sql, "Region", data -> new Object[]{ data[0], data[1] });
    }

    private static void loadPassenger(String filePath) throws SQLException {
        String sql = "INSERT IGNORE INTO passenger (passenger_id, passenger_name, age, gender, mobile_number) VALUES (?, ?, ?, ?, ?)";
        executeBatchLoad(filePath, sql, "Passenger", data -> {
            Integer age = data[2].trim().isEmpty() ? null : Integer.parseInt(data[2]);
            return new Object[]{ Integer.parseInt(data[0]), data[1], age, data[3], data[4] };
        });
    }

    private static void loadAirline(String filePath) throws SQLException {
        String sql = "INSERT IGNORE INTO airline (airline_id, airline_code, airline_name, region_code) VALUES (?, ?, ?, ?)";
        executeBatchLoad(filePath, sql, "Airline", data -> new Object[]{ Integer.parseInt(data[0]), data[1], data[2], data[3] });
    }

    // Airport：通过三元运算符处理海拔(altitude)的空值
    private static void loadAirport(String filePath) throws SQLException {
        String sql = "INSERT IGNORE INTO airport (airport_id, iata_code, airport_name, city, region_code, latitude, longitude, altitude, timezone_offset, timezone_dst, timezone_region) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        executeBatchLoad(filePath, sql, "Airport", data -> {
            Double altitude = data[7].trim().isEmpty() ? null : Double.parseDouble(data[7]);
            return new Object[]{
                    Integer.parseInt(data[0]), data[1], data[2], data[3], data[4],
                    Double.parseDouble(data[5]), Double.parseDouble(data[6]),
                    altitude, // 如果是空，这里就是 null，底层会自动转为 MySQL 的 NULL
                    Integer.parseInt(data[8]), data[9], data[10]
            };
        });
    }

    private static void loadRoute(String filePath) throws SQLException {
        String sql = "INSERT IGNORE INTO route (route_id, flight_number, airline_id, source_airport_id, destination_airport_id, scheduled_departure_time, scheduled_arrival_time, arrival_day_offset) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        executeBatchLoad(filePath, sql, "Route", data -> new Object[]{
                Long.parseLong(data[0]), data[1], Integer.parseInt(data[2]),
                Integer.parseInt(data[3]), Integer.parseInt(data[4]),
                java.sql.Time.valueOf(data[5]), java.sql.Time.valueOf(data[6]), Integer.parseInt(data[7])
        });
    }

    private static void loadFlight(String filePath) throws SQLException {
        String sql = "INSERT IGNORE INTO flight (flight_id, route_id, flight_date) VALUES (?, ?, ?)";
        executeBatchLoad(filePath, sql, "Flight", data -> new Object[]{
                Long.parseLong(data[0]), Long.parseLong(data[1]), java.sql.Date.valueOf(data[2])
        });
    }

    private static void loadFlightCabin(String filePath) throws SQLException {
        String sql = "INSERT IGNORE INTO flight_cabin (flight_id, cabin_class, price, remain_count) VALUES (?, ?, ?, ?)";
        executeBatchLoad(filePath, sql, "Flight Cabin", data -> new Object[]{
                Long.parseLong(data[0]), data[1], Double.parseDouble(data[2]), Integer.parseInt(data[3])
        });
    }

    // ---------------------------------------------------------
    // 核心底层引擎：通用批处理与计时监控
    // ---------------------------------------------------------
    private static void executeBatchLoad(String filePath, String sql, String tableName, DataBinder binder) throws SQLException {
        long startTime = System.currentTimeMillis();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            String line;
            br.readLine(); // 跳过表头
            int count = 0;

            while ((line = br.readLine()) != null) {
                String[] data = line.split(",", -1); // 保证切分出空字符串
                Object[] values = binder.bind(data);

                for (int i = 0; i < values.length; i++) {
                    // setObject 极其强大，如果 values[i] 是 null，它会自动设为数据库的 NULL
                    pstmt.setObject(i + 1, values[i]);
                }
                pstmt.addBatch();

                if (++count % BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch(); // 提交尾部数据

            long endTime = System.currentTimeMillis();

            System.out.printf("  [✔] Loaded %-15s : %7d records in %5.3f sec.\n", tableName, count, (endTime - startTime) / 1000.0);

        } catch (IOException e) {
            System.out.println("  [✖] File Error for " + tableName + ": " + e.getMessage());
        }
    }

    @FunctionalInterface
    interface DataBinder {
        Object[] bind(String[] data);
    }

    // ---------------------------------------------------------
    // 数据库生命周期管理
    // ---------------------------------------------------------
    private static boolean connectDB() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(URL, USER, PWD);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void closeDB() {
        try { if (conn != null) conn.close(); } catch (SQLException e) { e.printStackTrace(); }
    }
}