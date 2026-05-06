//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.InputStreamReader;
//import java.math.BigDecimal;
//import java.nio.charset.StandardCharsets;
//import java.sql.*;
//
//public class AviationDataLoader {
//
//    private Connection con = null;
//    private final String url = "jdbc:postgresql://localhost:5432/project1?characterEncoding=UTF-8";
//    private final String user = "checker"; // 替换为你的用户名
//    private final String pwd = "123456";    // 替换为你的密码
//
//    public void openConnection() {
//        try {
//            Class.forName("org.postgresql.Driver");
//            con = DriverManager.getConnection(url, user, pwd);
//            // ⚠️ 极其关键：关闭自动提交，开启极速批处理模式
//            con.setAutoCommit(false);
//            System.out.println("✅ 数据库连接成功，开启批处理模式...");
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(1);
//        }
//    }
//
//    public void closeConnection() {
//        try {
//            if (con != null) {
//                con.setAutoCommit(true); // 恢复默认
//                con.close();
//                System.out.println("数据库连接已关闭。");
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 通用的核心导入方法 (利用高阶函数思维简化代码)
//     */
//    public void loadCsvData(String filePath, String sql, DataBinder binder) {
//        System.out.print("正在导入 " + filePath + " ... ");
//        long startTime = System.currentTimeMillis();
//
//        try (PreparedStatement pstmt = con.prepareStatement(sql);
//             BufferedReader br = new BufferedReader(new InputStreamReader(
//                     new FileInputStream(filePath), StandardCharsets.UTF_8))) {
//
//            String line;
//            boolean isFirstLine = true;
//            int count = 0;
//
//            while ((line = br.readLine()) != null) {
//                if (isFirstLine) { isFirstLine = false; continue; } // 跳过表头
//
//                // CSV 按逗号分割 (注意：如果数据本身含有逗号，需用专业CSV库，这里假设清洗得很干净)
//                // 加上 -1 参数，强制 Java 保留末尾的空列
//                String[] data = line.split(",",-1);
//
//                // 让外部传入的具体业务逻辑去绑定参数
//                binder.bind(pstmt, data);
//
//                pstmt.addBatch(); // 加入批处理队列
//                count++;
//
//                // 每 5000 条发送一次，防止内存溢出
//                if (count % 5000 == 0) {
//                    pstmt.executeBatch();
//                    pstmt.clearBatch();
//                }
//            }
//            pstmt.executeBatch(); // 执行剩余的
//            con.commit();         // 正式提交写入磁盘
//
//            long endTime = System.currentTimeMillis();
//            System.out.println("完成! 共 " + count + " 条，耗时 " + (endTime - startTime) + " ms.");
//
//        } catch (Exception e) {
//            try { con.rollback(); } catch (SQLException ex) { ex.printStackTrace(); }
//            System.err.println("\n❌ 导入失败！发生回滚。检查数据：" + filePath);
//            e.printStackTrace();
//        }
//    }
//
//    // 定义一个接口，用于处理不同表的数据绑定逻辑
//    @FunctionalInterface
//    public interface DataBinder {
//        void bind(PreparedStatement pstmt, String[] data) throws SQLException;
//    }
//
//    public static void main(String[] args) {
//        AviationDataLoader loader = new AviationDataLoader();
//        loader.openConnection();
//
//        // 1. 导入 Region
//        loader.loadCsvData("data/clean_region.csv",
//                "INSERT INTO region (region_code, region_name) VALUES (?, ?) ON CONFLICT DO NOTHING",
//                (pstmt, data) -> {
//                    pstmt.setString(1, data[0]); // region_code
//                    pstmt.setString(2, data[1]); // region_name
//                }
//        );
//
//        // 2. 导入 Passenger
//        loader.loadCsvData("data/clean_passenger.csv",
//                "INSERT INTO passenger (passenger_id, passenger_name, age, gender, mobile_number) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
//                (pstmt, data) -> {
//                    pstmt.setInt(1, Integer.parseInt(data[0]));
//                    pstmt.setString(2, data[1]);
//                    pstmt.setInt(3, Integer.parseInt(data[2]));
//                    pstmt.setString(4, data[3]);
//                    pstmt.setString(5, data[4]);
//                }
//        );
//
//        // 3. 导入 Airline
//        loader.loadCsvData("data/clean_airline.csv",
//                "INSERT INTO airline (airline_id, airline_code, airline_name, region_code) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
//                (pstmt, data) -> {
//                    pstmt.setInt(1, Integer.parseInt(data[0]));
//                    pstmt.setString(2, data[1]);
//                    pstmt.setString(3, data[2]);
//                    pstmt.setString(4, data[3]);
//                }
//        );
//
//        // 4. 导入 Airport
//        loader.loadCsvData("data/clean_airport.csv",
//                "INSERT INTO airport (airport_id, iata_code, airport_name, city, region_code, latitude, longitude, altitude, timezone_offset, timezone_dst, timezone_region) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
//                (pstmt, data) -> {
//                    pstmt.setInt(1, Integer.parseInt(data[0]));
//                    pstmt.setString(2, data[1]);
//                    pstmt.setString(3, data[2]);
//                    pstmt.setString(4, data[3]);
//                    pstmt.setString(5, data[4]);
//                    pstmt.setBigDecimal(6, new BigDecimal(data[5])); // latitude
//                    pstmt.setBigDecimal(7, new BigDecimal(data[6])); // longitude
//                    // 处理 altitude 可能为空的问题
//                    if (data[7].isEmpty()) pstmt.setNull(8, Types.INTEGER);
//                    else pstmt.setInt(8, Integer.parseInt(data[7].split("\\.")[0]));
//                    pstmt.setInt(9, Integer.parseInt(data[8])); // timezone_offset
//                    pstmt.setString(10, data[9]);
//                    pstmt.setString(11, data[10]);
//                }
//        );
//
//        // 5. 导入 Route
//        loader.loadCsvData("data/clean_route.csv",
//                "INSERT INTO route (route_id, flight_number, airline_id, source_airport_id, destination_airport_id, scheduled_departure_time, scheduled_arrival_time, arrival_day_offset) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
//                (pstmt, data) -> {
//                    pstmt.setLong(1, Long.parseLong(data[0]));
//                    pstmt.setString(2, data[1]);
//                    pstmt.setInt(3, Integer.parseInt(data[2]));
//                    pstmt.setInt(4, Integer.parseInt(data[3]));
//                    pstmt.setInt(5, Integer.parseInt(data[4]));
//                    pstmt.setTime(6, Time.valueOf(data[5])); // scheduled_departure_time (HH:MM:SS)
//                    pstmt.setTime(7, Time.valueOf(data[6])); // scheduled_arrival_time
//                    pstmt.setInt(8, Integer.parseInt(data[7])); // arrival_day_offset
//                }
//        );
//
//        // 6. 导入 Flight
//        loader.loadCsvData("data/clean_flight.csv",
//                "INSERT INTO flight (flight_id, route_id, flight_date) VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
//                (pstmt, data) -> {
//                    pstmt.setLong(1, Long.parseLong(data[0]));
//                    pstmt.setLong(2, Long.parseLong(data[1]));
//                    pstmt.setDate(3, Date.valueOf(data[2])); // flight_date (YYYY-MM-DD)
//                }
//        );
//
//        // 7. 导入 Flight Cabin
//        loader.loadCsvData("data/clean_flight_cabin.csv",
//                "INSERT INTO flight_cabin (flight_id, cabin_class, price, remain_count) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
//                (pstmt, data) -> {
//                    pstmt.setLong(1, Long.parseLong(data[0]));
//                    pstmt.setString(2, data[1]); // BUSINESS or ECONOMY
//                    pstmt.setBigDecimal(3, new BigDecimal(data[2])); // price
//                    pstmt.setInt(4, Integer.parseInt(data[3])); // remain_count
//                }
//        );
//
//        loader.closeConnection();
//    }
//}
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class AviationDataLoader {

    private Connection con = null;
    private final String url = "jdbc:postgresql://localhost:5432/project1?characterEncoding=UTF-8";
    private final String user = "checker";
    private final String pwd = "123456";

    /**
     * 开启连接并设置为手动提交模式
     */
    public void openConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection(url, user, pwd);
            con.setAutoCommit(false);
            System.out.println("✅ 数据库连接成功，开启批处理模式...");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 关闭连接 (修复之前的报错点)
     */
    public void closeConnection() {
        try {
            if (con != null) {
                con.setAutoCommit(true); // 恢复自动提交
                con.close();
                System.out.println("🔌 数据库连接已关闭。");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 1. 彻底清空所有表
     */
    public void clearDatabase() throws SQLException {
        System.out.print("🗑️ 正在强制清空所有旧数据... ");
        String[] tables = {"ticket_order", "flight_cabin", "flight", "route", "airport", "airline", "passenger_contact", "passenger", "region"};
        try (Statement stmt = con.createStatement()) {
            for (String table : tables) {
                // 使用 RESTART IDENTITY 重置自增 ID
                stmt.execute("TRUNCATE TABLE " + table + " RESTART IDENTITY CASCADE");
            }
            con.commit();
            System.out.println("完成！");
        }
    }

    /**
     * 2. 插入专属测试账号和联系人 (确保 138 手机号可用)
     */
    public void insertTestUser() throws SQLException {
        System.out.print("👤 修正测试账号数据... ");
        try (Statement stmt = con.createStatement()) {
            // 手机号 11 位，身份证 18 位，密码 123456
            stmt.execute("INSERT INTO passenger (passenger_id, passenger_name, mobile_number, login_password_hash) " +
                    "VALUES (9999, '张三', '13800138000', '123456')");

            // 这里的 id_number 设为真实的 18 位格式
            stmt.execute("INSERT INTO passenger_contact (owner_passenger_id, contact_name, id_type, id_number) " +
                    "VALUES (9999, '张三', 'ID_CARD', '110101199001011234')");
            con.commit();
            System.out.println("完成！");
        }
    }

    /**
     * 3. 同步序列 (极其重要：防止 UI 新增数据时报主键冲突)
     */
    public void syncSequences() throws SQLException {
        System.out.print("🔄 正在同步自增序列计数器... ");
        String[] sqls = {
                "SELECT setval('passenger_passenger_id_seq', (SELECT MAX(passenger_id) FROM passenger))",
                "SELECT setval('passenger_contact_contact_id_seq', (SELECT MAX(contact_id) FROM passenger_contact))",
                "SELECT setval('ticket_order_order_id_seq', coalesce((SELECT MAX(order_id) FROM ticket_order), 1))"
        };
        try (Statement stmt = con.createStatement()) {
            for (String sql : sqls) { stmt.execute(sql); }
            con.commit();
            System.out.println("完成！");
        }
    }

    /**
     * 通用 CSV 导入方法
     */
    public void loadCsvData(String filePath, String sql, DataBinder binder) {
        System.out.print("正在导入 " + filePath + " ... ");
        try (PreparedStatement pstmt = con.prepareStatement(sql);
             BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            String line;
            br.readLine(); // 跳过表头
            int count = 0;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",", -1);
                binder.bind(pstmt, data);
                pstmt.addBatch();
                if (++count % 5000 == 0) pstmt.executeBatch();
            }
            pstmt.executeBatch();
            con.commit();
            System.out.println("成功 (" + count + " 条)");
        } catch (Exception e) {
            try { con.rollback(); } catch (SQLException ex) {}
            System.err.println("\n❌ 导入失败: " + filePath + " -> " + e.getMessage());
        }
    }

    @FunctionalInterface
    public interface DataBinder { void bind(PreparedStatement pstmt, String[] data) throws SQLException; }

    public static void main(String[] args) throws SQLException {
        AviationDataLoader loader = new AviationDataLoader();
        loader.openConnection();

        // 顺序 1: 清空
        loader.clearDatabase();

        // 顺序 2: 基础数据
        loader.loadCsvData("data/clean_region.csv", "INSERT INTO region VALUES (?,?)", (p,d)->{ p.setString(1,d[0]); p.setString(2,d[1]); });

        loader.loadCsvData("data/clean_passenger.csv",
                "INSERT INTO passenger(passenger_id,passenger_name,age,gender,mobile_number,login_password_hash) VALUES(?,?,?,?,?,'123456')", (p,d)->{
                    p.setInt(1,Integer.parseInt(d[0])); p.setString(2,d[1]); p.setInt(3,Integer.parseInt(d[2])); p.setString(4,d[3]); p.setString(5,d[4]);
                });

        // 关键：给每个乘客生成一个联系人，实现“默认值”
        loader.loadCsvData("data/clean_passenger.csv",
                "INSERT INTO passenger_contact(owner_passenger_id,contact_name,id_type,id_number) VALUES(?,?,'ID_CARD',?)", (p,d)->{
                    p.setInt(1,Integer.parseInt(d[0])); p.setString(2,d[1]); p.setString(3, "ID"+d[0]);
                });

        loader.loadCsvData("data/clean_airline.csv", "INSERT INTO airline VALUES(?,?,?,?)", (p,d)->{ p.setInt(1,Integer.parseInt(d[0])); p.setString(2,d[1]); p.setString(3,d[2]); p.setString(4,d[3]); });

        loader.loadCsvData("data/clean_airport.csv", "INSERT INTO airport VALUES(?,?,?,?,?,?,?,?,?,?,?)", (p,d)->{
            p.setInt(1,Integer.parseInt(d[0])); p.setString(2,d[1]); p.setString(3,d[2]); p.setString(4,d[3]); p.setString(5,d[4]);
            p.setBigDecimal(6,new BigDecimal(d[5])); p.setBigDecimal(7,new BigDecimal(d[6]));
            p.setInt(8, d[7].isEmpty() ? 0 : (int)Double.parseDouble(d[7]));
            p.setInt(9,Integer.parseInt(d[8])); p.setString(10,d[9]); p.setString(11,d[10]);
        });

        loader.loadCsvData("data/clean_route.csv", "INSERT INTO route VALUES(?,?,?,?,?,?,?,?)", (p,d)->{
            p.setLong(1,Long.parseLong(d[0])); p.setString(2,d[1]); p.setInt(3,Integer.parseInt(d[2])); p.setInt(4,Integer.parseInt(d[3]));
            p.setInt(5,Integer.parseInt(d[4])); p.setTime(6,Time.valueOf(d[5])); p.setTime(7,Time.valueOf(d[6])); p.setInt(8,Integer.parseInt(d[7]));
        });

        loader.loadCsvData("data/clean_flight.csv", "INSERT INTO flight VALUES(?,?,?)", (p,d)->{ p.setLong(1,Long.parseLong(d[0])); p.setLong(2,Long.parseLong(d[1])); p.setDate(3,Date.valueOf(d[2])); });

        loader.loadCsvData("data/clean_flight_cabin.csv", "INSERT INTO flight_cabin VALUES(?,?,?,?)", (p,d)->{ p.setLong(1,Long.parseLong(d[0])); p.setString(2,d[1]); p.setBigDecimal(3,new BigDecimal(d[2])); p.setInt(4,Integer.parseInt(d[3])); });

        // 顺序 3: 插入测试号并同步序列
        loader.insertTestUser();
        loader.syncSequences();

        loader.closeConnection();
    }
}