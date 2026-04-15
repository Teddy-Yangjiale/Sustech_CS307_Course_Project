import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class AviationDataLoader {

    private Connection con = null;
    private final String url = "jdbc:postgresql://localhost:5432/project1?characterEncoding=UTF-8";
    private final String user = "checker"; // 替换为你的用户名
    private final String pwd = "123456";    // 替换为你的密码

    public void openConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection(url, user, pwd);
            // ⚠️ 极其关键：关闭自动提交，开启极速批处理模式
            con.setAutoCommit(false);
            System.out.println("✅ 数据库连接成功，开启批处理模式...");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void closeConnection() {
        try {
            if (con != null) {
                con.setAutoCommit(true); // 恢复默认
                con.close();
                System.out.println("数据库连接已关闭。");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通用的核心导入方法 (利用高阶函数思维简化代码)
     */
    public void loadCsvData(String filePath, String sql, DataBinder binder) {
        System.out.print("正在导入 " + filePath + " ... ");
        long startTime = System.currentTimeMillis();

        try (PreparedStatement pstmt = con.prepareStatement(sql);
             BufferedReader br = new BufferedReader(new InputStreamReader(
                     new FileInputStream(filePath), StandardCharsets.UTF_8))) {

            String line;
            boolean isFirstLine = true;
            int count = 0;

            while ((line = br.readLine()) != null) {
                if (isFirstLine) { isFirstLine = false; continue; } // 跳过表头

                // CSV 按逗号分割 (注意：如果数据本身含有逗号，需用专业CSV库，这里假设清洗得很干净)
                // 加上 -1 参数，强制 Java 保留末尾的空列
                String[] data = line.split(",",-1);

                // 让外部传入的具体业务逻辑去绑定参数
                binder.bind(pstmt, data);

                pstmt.addBatch(); // 加入批处理队列
                count++;

                // 每 5000 条发送一次，防止内存溢出
                if (count % 5000 == 0) {
                    pstmt.executeBatch();
                    pstmt.clearBatch();
                }
            }
            pstmt.executeBatch(); // 执行剩余的
            con.commit();         // 正式提交写入磁盘

            long endTime = System.currentTimeMillis();
            System.out.println("完成! 共 " + count + " 条，耗时 " + (endTime - startTime) + " ms.");

        } catch (Exception e) {
            try { con.rollback(); } catch (SQLException ex) { ex.printStackTrace(); }
            System.err.println("\n❌ 导入失败！发生回滚。检查数据：" + filePath);
            e.printStackTrace();
        }
    }

    // 定义一个接口，用于处理不同表的数据绑定逻辑
    @FunctionalInterface
    public interface DataBinder {
        void bind(PreparedStatement pstmt, String[] data) throws SQLException;
    }

    public static void main(String[] args) {
        AviationDataLoader loader = new AviationDataLoader();
        loader.openConnection();

        // 1. 导入 Region
        loader.loadCsvData("data/clean_region.csv",
                "INSERT INTO region (region_code, region_name) VALUES (?, ?) ON CONFLICT DO NOTHING",
                (pstmt, data) -> {
                    pstmt.setString(1, data[0]); // region_code
                    pstmt.setString(2, data[1]); // region_name
                }
        );

        // 2. 导入 Passenger
        loader.loadCsvData("data/clean_passenger.csv",
                "INSERT INTO passenger (passenger_id, passenger_name, age, gender, mobile_number) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (pstmt, data) -> {
                    pstmt.setInt(1, Integer.parseInt(data[0]));
                    pstmt.setString(2, data[1]);
                    pstmt.setInt(3, Integer.parseInt(data[2]));
                    pstmt.setString(4, data[3]);
                    pstmt.setString(5, data[4]);
                }
        );

        // 3. 导入 Airline
        loader.loadCsvData("data/clean_airline.csv",
                "INSERT INTO airline (airline_id, airline_code, airline_name, region_code) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (pstmt, data) -> {
                    pstmt.setInt(1, Integer.parseInt(data[0]));
                    pstmt.setString(2, data[1]);
                    pstmt.setString(3, data[2]);
                    pstmt.setString(4, data[3]);
                }
        );

        // 4. 导入 Airport
        loader.loadCsvData("data/clean_airport.csv",
                "INSERT INTO airport (airport_id, iata_code, airport_name, city, region_code, latitude, longitude, altitude, timezone_offset, timezone_dst, timezone_region) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (pstmt, data) -> {
                    pstmt.setInt(1, Integer.parseInt(data[0]));
                    pstmt.setString(2, data[1]);
                    pstmt.setString(3, data[2]);
                    pstmt.setString(4, data[3]);
                    pstmt.setString(5, data[4]);
                    pstmt.setBigDecimal(6, new BigDecimal(data[5])); // latitude
                    pstmt.setBigDecimal(7, new BigDecimal(data[6])); // longitude
                    // 处理 altitude 可能为空的问题
                    if (data[7].isEmpty()) pstmt.setNull(8, Types.INTEGER);
                    else pstmt.setInt(8, Integer.parseInt(data[7].split("\\.")[0]));
                    pstmt.setInt(9, Integer.parseInt(data[8])); // timezone_offset
                    pstmt.setString(10, data[9]);
                    pstmt.setString(11, data[10]);
                }
        );

        // 5. 导入 Route
        loader.loadCsvData("data/clean_route.csv",
                "INSERT INTO route (route_id, flight_number, airline_id, source_airport_id, destination_airport_id, scheduled_departure_time, scheduled_arrival_time, arrival_day_offset) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (pstmt, data) -> {
                    pstmt.setLong(1, Long.parseLong(data[0]));
                    pstmt.setString(2, data[1]);
                    pstmt.setInt(3, Integer.parseInt(data[2]));
                    pstmt.setInt(4, Integer.parseInt(data[3]));
                    pstmt.setInt(5, Integer.parseInt(data[4]));
                    pstmt.setTime(6, Time.valueOf(data[5])); // scheduled_departure_time (HH:MM:SS)
                    pstmt.setTime(7, Time.valueOf(data[6])); // scheduled_arrival_time
                    pstmt.setInt(8, Integer.parseInt(data[7])); // arrival_day_offset
                }
        );

        // 6. 导入 Flight
        loader.loadCsvData("data/clean_flight.csv",
                "INSERT INTO flight (flight_id, route_id, flight_date) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                (pstmt, data) -> {
                    pstmt.setLong(1, Long.parseLong(data[0]));
                    pstmt.setLong(2, Long.parseLong(data[1]));
                    pstmt.setDate(3, Date.valueOf(data[2])); // flight_date (YYYY-MM-DD)
                }
        );

        // 7. 导入 Flight Cabin
        loader.loadCsvData("data/clean_flight_cabin.csv",
                "INSERT INTO flight_cabin (flight_id, cabin_class, price, remain_count) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (pstmt, data) -> {
                    pstmt.setLong(1, Long.parseLong(data[0]));
                    pstmt.setString(2, data[1]); // BUSINESS or ECONOMY
                    pstmt.setBigDecimal(3, new BigDecimal(data[2])); // price
                    pstmt.setInt(4, Integer.parseInt(data[3])); // remain_count
                }
        );

        loader.closeConnection();
    }
}