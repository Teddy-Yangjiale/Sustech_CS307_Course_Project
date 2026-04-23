//import java.sql.*;
//import java.util.Scanner;
//
//public class Client {
//
//    // 数据库连接配置,需要修改成自己的信息
//    private static final String URL = "jdbc:postgresql://localhost:5432/project1?characterEncoding=UTF-8";
//    private static final String USER = "checker";
//    private static final String PWD = "123456";
//    private static Connection conn = null;
//
//    public static void main(String[] args) {
//        Scanner scanner = new Scanner(System.in);
//
//        System.out.println("Connecting to database...");
//        if (!connectDB()) {
//            System.out.println("Failed to connect to database. Exiting...");
//            return;
//        }
//        System.out.println("Connected successfully!\n");
//
//        while (true) {
//            System.out.println("=====================================");
//            System.out.println("     Aviation Ticket System CLI      ");
//            System.out.println("=====================================");
//            System.out.println("1. Generate tickets");
//            System.out.println("2. Search Flights");
//            System.out.println("3. Manage Orders (CRUD)");
//            System.out.println("0. Exit");
//            System.out.print(">> Input: ");
//
//            String choice = scanner.nextLine();
//
//            switch (choice) {
//                case "1":
//                    generateTickets(scanner);
//                    break;
//                case "2":
//                    searchFlights(scanner);
//                    break;
//                case "3":
//                    manageOrdersMenu(scanner);
//                    break;
//                case "0":
//                    System.out.println("Goodbye!");
//                    closeDB();
//                    scanner.close();
//                    System.exit(0);
//                default:
//                    System.out.println("Invalid input, please try again.\n");
//            }
//        }
//    }
//    // 1. Generate tickets: 统计指定日期范围内的航班与余票信息
//    private static void generateTickets(Scanner scanner) {
//        System.out.println("\n--- Generate Tickets (Statistics) ---");
//        System.out.print("Enter Start Date (YYYY-MM-DD): ");
//        String startDate = scanner.nextLine();
//        System.out.print("Enter End Date (YYYY-MM-DD): ");
//        String endDate = scanner.nextLine();
//
//        // 核心 SQL: 联表查询并使用 SUM 和 CASE WHEN 进行分类汇总
//        String sql = "SELECT " +
//                "    COUNT(DISTINCT f.flight_id) AS total_flights, " +
//                "    SUM(CASE WHEN fc.cabin_class = 'BUSINESS' THEN fc.remain_count ELSE 0 END) AS business_tickets, " +
//                "    SUM(CASE WHEN fc.cabin_class = 'ECONOMY' THEN fc.remain_count ELSE 0 END) AS economy_tickets " +
//                "FROM flight f " +
//                "JOIN flight_cabin fc ON f.flight_id = fc.flight_id " +
//                "WHERE f.flight_date BETWEEN ? AND ?";
//
//        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            // 将输入的字符串转换为 SQL Date
//            pstmt.setDate(1, java.sql.Date.valueOf(startDate));
//            pstmt.setDate(2, java.sql.Date.valueOf(endDate));
//
//            ResultSet rs = pstmt.executeQuery();
//
//            if (rs.next()) {
//                long totalFlights = rs.getLong("total_flights");
//                if (totalFlights == 0) {
//                    System.out.println("No flights found in this date range.");
//                } else {
//                    System.out.println("\nStatistics for " + startDate + " to " + endDate + ":");
//                    System.out.println("--------------------------------------------------");
//                    System.out.printf("Total Flights Scheduled : %d\n", totalFlights);
//                    System.out.printf("Business Class Tickets  : %d available\n", rs.getLong("business_tickets"));
//                    System.out.printf("Economy Class Tickets   : %d available\n", rs.getLong("economy_tickets"));
//                    System.out.println("--------------------------------------------------\n");
//                }
//            }
//        } catch (IllegalArgumentException e) {
//            System.out.println("Invalid date format. Please use YYYY-MM-DD.");
//        } catch (SQLException e) {
//            System.out.println("Database Error: " + e.getMessage());
//        }
//    }
//    // 2. Search Flights: 复杂的 5 表联查找机票
//    private static void searchFlights(Scanner scanner) {
//        System.out.println("\n--- Search Flights ---");
//        System.out.print("Enter Source City or IATA Code (e.g., Beijing or PEK): ");
//        String source = scanner.nextLine();
//        System.out.print("Enter Destination City or IATA Code (e.g., Shanghai or SHA): ");
//        String dest = scanner.nextLine();
//        System.out.print("Enter Flight Date (YYYY-MM-DD): ");
//        String date = scanner.nextLine();
//
//        // 核心 SQL: 5表 JOIN，并且在 WHERE 条件中同时支持城市名和 IATA 码搜索
//        String sql = "SELECT " +
//                "    f.flight_id, r.flight_number, a.airline_name, " +
//                "    src.iata_code AS src_code, dst.iata_code AS dst_code, " +
//                "    r.scheduled_departure_time, r.scheduled_arrival_time, " +
//                "    fc.cabin_class, fc.price, fc.remain_count " +
//                "FROM flight f " +
//                "JOIN route r ON f.route_id = r.route_id " +
//                "JOIN airline a ON r.airline_id = a.airline_id " +
//                "JOIN airport src ON r.source_airport_id = src.airport_id " +
//                "JOIN airport dst ON r.destination_airport_id = dst.airport_id " +
//                "JOIN flight_cabin fc ON f.flight_id = fc.flight_id " +
//                "WHERE f.flight_date = ? " +
//                "  AND (src.city ILIKE ? OR src.iata_code ILIKE ?) " +
//                "  AND (dst.city ILIKE ? OR dst.iata_code ILIKE ?) " +
//                "  AND fc.remain_count > 0 " + // 只显示还有余票的
//                "ORDER BY r.scheduled_departure_time ASC";
//
//        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            pstmt.setDate(1, java.sql.Date.valueOf(date));
//            // ILIKE 在 PostgreSQL 中是不区分大小写的匹配
//            pstmt.setString(2, source);
//            pstmt.setString(3, source);
//            pstmt.setString(4, dest);
//            pstmt.setString(5, dest);
//
//            ResultSet rs = pstmt.executeQuery();
//
//            System.out.println("\n--- Flight Results for " + date + " ---");
//            System.out.printf("%-10s %-10s %-15s %-10s %-10s %-10s %-12s %-8s %-10s\n",
//                    "FlightID", "FlightNo", "Airline", "From", "To", "Departs", "Arrives", "Class", "Price");
//            System.out.println("--------------------------------------------------------------------------------------------------");
//
//            boolean hasData = false;
//            while (rs.next()) {
//                hasData = true;
//                // 截取时间字符串的前5位 (HH:mm) 让输出更简洁
//                String depTime = rs.getString("scheduled_departure_time").substring(0, 5);
//                String arrTime = rs.getString("scheduled_arrival_time").substring(0, 5);
//
//                System.out.printf("%-10d %-10s %-15s %-10s %-10s %-10s %-12s %-8.2s %-10.2f\n",
//                        rs.getLong("flight_id"),
//                        rs.getString("flight_number"),
//                        // 航司名字可能太长，截断显示保证排版不乱
//                        rs.getString("airline_name").length() > 13 ? rs.getString("airline_name").substring(0, 13) + ".." : rs.getString("airline_name"),
//                        rs.getString("src_code"),
//                        rs.getString("dst_code"),
//                        depTime,
//                        arrTime,
//                        rs.getString("cabin_class"), // BUSINESS 或 ECONOMY
//                        rs.getDouble("price"));
//            }
//            if (!hasData) {
//                System.out.println("No available flights found for this route and date.");
//            }
//            System.out.println("--------------------------------------------------------------------------------------------------\n");
//
//        } catch (IllegalArgumentException e) {
//            System.out.println("Invalid date format. Please use YYYY-MM-DD.");
//        } catch (SQLException e) {
//            System.out.println("Database Error: " + e.getMessage());
//        }
//    }
//    // ==========================================
//    // 3.订单管理子菜单 (CRUD 核心逻辑)
//    // ==========================================
//    private static void manageOrdersMenu(Scanner scanner) {
//        while (true) {
//            System.out.println("\n--- Manage Orders ---");
//            System.out.println("1. Book Ticket (增 - Create)");
//            System.out.println("2. View My Orders (查 - Read)");
//            System.out.println("3. Cancel Order (改 - Update)");
//            System.out.println("4. Delete Order (删 - Delete)");
//            System.out.println("0. Return to Main Menu");
//            System.out.print(">> Input: ");
//
//            String choice = scanner.nextLine();
//            if (choice.equals("0")) break;
//
//            try {
//                switch (choice) {
//                    case "1": bookTicket(scanner); break;
//                    case "2": viewOrders(scanner); break;
//                    case "3": cancelOrder(scanner); break;
//                    case "4": deleteOrder(scanner); break;
//                    default: System.out.println("Invalid input.");
//                }
//            } catch (SQLException e) {
//                System.out.println("Database Error: " + e.getMessage());
//                try {
//                    if (conn != null) conn.rollback(); // 发生错误时回滚事务
//                } catch (SQLException ex) { ex.printStackTrace(); }
//            }
//        }
//    }
//
//    // 1. 增 (Create)：购买机票 (涉及事务处理)
//    private static void bookTicket(Scanner scanner) throws SQLException {
//        System.out.print("Enter Passenger ID: ");
//        int passengerId = Integer.parseInt(scanner.nextLine());
//        System.out.print("Enter Flight ID: ");
//        long flightId = Long.parseLong(scanner.nextLine());
//        System.out.print("Enter Cabin Class (BUSINESS/ECONOMY): ");
//        String cabinClass = scanner.nextLine().toUpperCase();
//        System.out.print("Enter Ticket Price: ");
//        double price = Double.parseDouble(scanner.nextLine());
//
//        conn.setAutoCommit(false); //  开启事务：买票涉及两张表的改动
//
//        // 第一步：尝试扣减余票 (利用 WHERE 保证不超卖)
//        String updateCabinSql = "UPDATE flight_cabin SET remain_count = remain_count - 1 " +
//                "WHERE flight_id = ? AND cabin_class = ? AND remain_count > 0";
//        try (PreparedStatement pstmt1 = conn.prepareStatement(updateCabinSql)) {
//            pstmt1.setLong(1, flightId);
//            pstmt1.setString(2, cabinClass);
//            int updated = pstmt1.executeUpdate();
//
//            if (updated == 0) {
//                System.out.println("Booking failed: Not enough tickets or flight not found.");
//                conn.rollback();
//                return;
//            }
//        }
//
//        // 第二步：生成订单
//        String insertOrderSql = "INSERT INTO ticket_order (passenger_id, flight_id, cabin_class, order_status, ticket_price) " +
//                "VALUES (?, ?, ?, 'PAID', ?)";
//        try (PreparedStatement pstmt2 = conn.prepareStatement(insertOrderSql)) {
//            pstmt2.setInt(1, passengerId);
//            pstmt2.setLong(2, flightId);
//            pstmt2.setString(3, cabinClass);
//            pstmt2.setDouble(4, price);
//            pstmt2.executeUpdate();
//        }
//
//        conn.commit(); // 提交事务
//        System.out.println("Ticket booked successfully!");
//        conn.setAutoCommit(true); // 恢复默认
//    }
//
//    // 2. 查 (Read)：查看某乘客订单
//    private static void viewOrders(Scanner scanner) throws SQLException {
//        System.out.print("Enter Passenger ID to search: ");
//        int passengerId = Integer.parseInt(scanner.nextLine());
//
//        String sql = "SELECT order_id, flight_id, cabin_class, order_status, ticket_price, order_time " +
//                "FROM ticket_order WHERE passenger_id = ? ORDER BY order_time DESC";
//
//        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            pstmt.setInt(1, passengerId);
//            ResultSet rs = pstmt.executeQuery();
//
//            System.out.println("\n--- Order History ---");
//            System.out.printf("%-10s %-10s %-15s %-15s %-10s %-25s\n", "OrderID", "FlightID", "Cabin", "Status", "Price", "Time");
//            boolean hasData = false;
//            while (rs.next()) {
//                hasData = true;
//                System.out.printf("%-10d %-10d %-15s %-15s %-10.2f %-25s\n",
//                        rs.getLong("order_id"), rs.getLong("flight_id"),
//                        rs.getString("cabin_class"), rs.getString("order_status"),
//                        rs.getDouble("ticket_price"), rs.getTimestamp("order_time").toString());
//            }
//            if (!hasData) System.out.println("No orders found for this passenger.");
//            System.out.println("---------------------\n");
//        }
//    }
//
//    // 3. 改 (Update)：取消订单 (退票)
//    private static void cancelOrder(Scanner scanner) throws SQLException {
//        System.out.print("Enter Order ID to cancel: ");
//        long orderId = Long.parseLong(scanner.nextLine());
//
//        conn.setAutoCommit(false); // 事务开始
//
//        // 第一步：把状态 PAID 改为 CANCELLED
//        String updateOrderSql = "UPDATE ticket_order SET order_status = 'CANCELLED' " +
//                "WHERE order_id = ? AND order_status = 'PAID' RETURNING flight_id, cabin_class";
//        long flightId = -1;
//        String cabinClass = "";
//
//        try (PreparedStatement pstmt1 = conn.prepareStatement(updateOrderSql)) {
//            pstmt1.setLong(1, orderId);
//            ResultSet rs = pstmt1.executeQuery();
//            if (rs.next()) {
//                flightId = rs.getLong("flight_id");
//                cabinClass = rs.getString("cabin_class");
//            } else {
//                System.out.println("Cancellation failed: Order not found or already cancelled.");
//                conn.rollback();
//                return;
//            }
//        }
//
//        // 第二步：把退掉的票加回库存
//        String updateCabinSql = "UPDATE flight_cabin SET remain_count = remain_count + 1 " +
//                "WHERE flight_id = ? AND cabin_class = ?";
//        try (PreparedStatement pstmt2 = conn.prepareStatement(updateCabinSql)) {
//            pstmt2.setLong(1, flightId);
//            pstmt2.setString(2, cabinClass);
//            pstmt2.executeUpdate();
//        }
//
//        conn.commit();
//        System.out.println("Order cancelled successfully! Ticket returned to pool.");
//        conn.setAutoCommit(true);
//    }
//
//    // 4. 删 (Delete)：物理删除订单
//    private static void deleteOrder(Scanner scanner) throws SQLException {
//        System.out.print("Enter Order ID to PERMANENTLY delete: ");
//        long orderId = Long.parseLong(scanner.nextLine());
//
//        String sql = "DELETE FROM ticket_order WHERE order_id = ?";
//        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            pstmt.setLong(1, orderId);
//            int rows = pstmt.executeUpdate();
//            if (rows > 0) {
//                System.out.println("Order deleted permanently from database.");
//            } else {
//                System.out.println("Delete failed: Order not found.");
//            }
//        }
//    }
//
//    // ==========================================
//    // 数据库连接与关闭辅助方法
//    // ==========================================
//    private static boolean connectDB() {
//        try {
//            Class.forName("org.postgresql.Driver");
//            conn = DriverManager.getConnection(URL, USER, PWD);
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    private static void closeDB() {
//        try {
//            if (conn != null) conn.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//}
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Client {
    // --- 数据库配置 (请根据你的实际情况修改) ---
    private static final String URL = "jdbc:postgresql://localhost:5432/project1";
    private static final String USER = "checker";
    private static final String PWD = "123456";

    // --- 全局状态 ---
    private static Connection conn;
    private static final Scanner scanner = new Scanner(System.in);
    private static Integer currentUserId = null;
    private static String currentUserName = "";
    private static String currentRole = ""; // "PASSENGER" or "ADMIN"

    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(URL, USER, PWD);
            System.out.println("✅ 数据库连接成功！欢迎进入 SUSTech 航空系统。");

            while (true) {
                if (currentUserId == null && !currentRole.equals("ADMIN")) {
                    showLoginMenu();
                } else if (currentRole.equals("ADMIN")) {
                    showAdminMenu();
                } else {
                    showPassengerMenu();
                }
            }
        } catch (Exception e) {
            System.err.println("❌ 系统崩溃: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ==========================================
    // 1. 认证系统
    // ==========================================
    private static void showLoginMenu() throws SQLException {
        System.out.println("\n--- 主登录菜单 ---");
        System.out.println("1. 乘客登录 (手机号)");
        System.out.println("2. 管理员登录 (授权码)");
        System.out.println("0. 退出程序");
        System.out.print("请选择: ");
        String choice = scanner.nextLine();

        if (choice.equals("1")) {
            System.out.print("请输入手机号: ");
            String phone = scanner.nextLine();
            System.out.print("请输入密码: ");
            String pass = scanner.nextLine();

            String sql = "SELECT passenger_id, passenger_name FROM passenger WHERE mobile_number = ? AND login_password_hash = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, phone);
                pstmt.setString(2, pass);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    currentUserId = rs.getInt("passenger_id");
                    currentUserName = rs.getString("passenger_name");
                    currentRole = "PASSENGER";
                    System.out.println("🎉 欢迎回来, " + currentUserName + "!");
                } else {
                    System.out.println("❌ 账号或密码错误。");
                }
            }
        } else if (choice.equals("2")) {
            System.out.print("请输入管理员授权码: ");
            String code = scanner.nextLine();
            if (code.equals("admin")) {
                currentRole = "ADMIN";
                currentUserName = "System Administrator";
                System.out.println("🛠️ 已进入管理员模式。");
            } else {
                System.out.println("❌ 授权码无效。");
            }
        } else if (choice.equals("0")) {
            System.exit(0);
        }
    }

    // ==========================================
    // 2. 乘客业务 (搜索、订票、退票)
    // ==========================================
    private static void showPassengerMenu() throws SQLException {
        System.out.println("\n--- 乘客个人中心 (" + currentUserName + ") ---");
        System.out.println("1. 高级搜索机票 (Task 2)");
        System.out.println("2. 预订机票 (Task 3)");
        System.out.println("3. 我的订单/退票 (Task 4)");
        System.out.println("0. 注销登录");
        System.out.print("请选择: ");
        String choice = scanner.nextLine();

        switch (choice) {
            case "1": advancedSearch(); break;
            case "2": bookTicket(); break;
            case "3": manageMyOrders(); break;
            case "0": logout(); break;
        }
    }

    /**
     * 高级搜索实现 (Task 2)
     * 支持: 必填(始发、到达、日期), 选填(航司、起飞时间段)
     */
    private static void advancedSearch() throws SQLException {
        System.out.println("\n[机票高级搜索]");
        System.out.print("始发地 (城市或三位码, 必填): "); String src = scanner.nextLine();
        System.out.print("目的地 (城市或三位码, 必填): "); String dst = scanner.nextLine();
        System.out.print("日期 (YYYY-MM-DD, 必填): "); String date = scanner.nextLine();
        System.out.print("指定航司 (回车跳过): "); String airline = scanner.nextLine();
        System.out.print("起飞不早于 (HH:MM:SS, 回车跳过): "); String startTime = scanner.nextLine();
        System.out.print("起飞不晚于 (HH:MM:SS, 回车跳过): "); String endTime = scanner.nextLine();

        StringBuilder sql = new StringBuilder(
                "SELECT f.flight_id, r.flight_number, a.airline_name, " +
                        "src.iata_code AS src_code, dst.iata_code AS dst_code, " +
                        "r.scheduled_departure_time AS dep_time, r.scheduled_arrival_time AS arr_time, " +
                        "r.arrival_day_offset, fc.cabin_class, fc.price, fc.remain_count " +
                        "FROM flight f JOIN route r ON f.route_id = r.route_id " +
                        "JOIN airline a ON r.airline_id = a.airline_id " +
                        "JOIN airport src ON r.source_airport_id = src.airport_id " +
                        "JOIN airport dst ON r.destination_airport_id = dst.airport_id " +
                        "JOIN flight_cabin fc ON f.flight_id = fc.flight_id " +
                        "WHERE f.flight_date = ?::date " +
                        "AND (src.city ILIKE ? OR src.iata_code ILIKE ?) " +
                        "AND (dst.city ILIKE ? OR dst.iata_code ILIKE ?) " +
                        "AND fc.remain_count > 0 "
        );

        List<Object> params = new ArrayList<>(Arrays.asList(date, src, src, dst, dst));
        if (!airline.isEmpty()) { sql.append(" AND a.airline_name ILIKE ? "); params.add("%" + airline + "%"); }
        if (!startTime.isEmpty()) { sql.append(" AND r.scheduled_departure_time >= ?::time "); params.add(startTime); }
        if (!endTime.isEmpty()) { sql.append(" AND r.scheduled_departure_time <= ?::time "); params.add(endTime); }
        sql.append(" ORDER BY r.scheduled_departure_time ASC");

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) pstmt.setObject(i + 1, params.get(i));
            ResultSet rs = pstmt.executeQuery();
            System.out.println("\n--------------------------------------------------------------------------------------------------");
            System.out.printf("%-10s | %-10s | %-10s | %-8s -> %-8s | %-10s | %-8s | %-6s\n",
                    "ID", "航班号", "航空公司", "出发", "到达", "舱位", "价格", "余票");
            while (rs.next()) {
                String arrTimeStr = rs.getString("arr_time").substring(0,5);
                if(rs.getInt("arrival_day_offset") > 0) arrTimeStr += " (+"+rs.getInt("arrival_day_offset")+"天)";

                System.out.printf("%-10d | %-10s | %-10s | %-8s -> %-8s | %-10s | %-8.2f | %-6d\n",
                        rs.getLong("flight_id"), rs.getString("flight_number"), rs.getString("airline_name"),
                        rs.getString("dep_time").substring(0,5), arrTimeStr,
                        rs.getString("cabin_class"), rs.getBigDecimal("price"), rs.getInt("remain_count"));
            }
            System.out.println("--------------------------------------------------------------------------------------------------");
        }
    }

    /**
     * 订票业务 (Task 3)
     * 使用事务保证库存与订单的一致性
     */
    private static void bookTicket() throws SQLException {
        System.out.println("\n[航班预订流程]");
        System.out.print("请输入您想预订的 Flight ID: ");
        long fId = Long.parseLong(scanner.nextLine());
        System.out.print("请输入舱位 (ECONOMY/BUSINESS): ");
        String cabin = scanner.nextLine().toUpperCase();

        // 获取乘机人 (Task 3 涉及联系人选择)
        System.out.println("您的常用乘机人:");
        String contactSql = "SELECT contact_id, contact_name, id_number FROM passenger_contact WHERE owner_passenger_id = ?";
        List<Integer> validContacts = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(contactSql)) {
            pstmt.setInt(1, currentUserId);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                System.out.println("ID: " + rs.getInt("contact_id") + " | 姓名: " + rs.getString("contact_name") + " | 证件: " + rs.getString("id_number"));
                validContacts.add(rs.getInt("contact_id"));
            }
        }
        System.out.print("选择乘机人 ID: ");
        int cId = Integer.parseInt(scanner.nextLine());

        // 开启事务
        conn.setAutoCommit(false);
        try {
            // 1. 尝试扣减库存
            String updateSql = "UPDATE flight_cabin SET remain_count = remain_count - 1 WHERE flight_id = ? AND cabin_class = ? AND remain_count > 0";
            try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                pstmt.setLong(1, fId); pstmt.setString(2, cabin);
                if (pstmt.executeUpdate() == 0) throw new Exception("库存不足或航班信息有误。");
            }

            // 2. 插入订单 (记录当前时间)
            String orderSql = "INSERT INTO ticket_order (passenger_id, contact_id, flight_id, cabin_class, ticket_price, order_time, order_status) " +
                    "SELECT ?, ?, ?, ?, price, CURRENT_TIMESTAMP, 'PAID' FROM flight_cabin WHERE flight_id = ? AND cabin_class = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(orderSql)) {
                pstmt.setInt(1, currentUserId); pstmt.setInt(2, cId);
                pstmt.setLong(3, fId); pstmt.setString(4, cabin);
                pstmt.setLong(5, fId); pstmt.setString(6, cabin);
                pstmt.executeUpdate();
            }

            conn.commit();
            System.out.println("✅ 订票成功！订单已生成。");
        } catch (Exception e) {
            conn.rollback();
            System.err.println("❌ 订票失败: " + e.getMessage());
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /**
     * 订单管理与退票 (Task 4)
     */
    private static void manageMyOrders() throws SQLException {
        System.out.println("\n[我的订单记录]");
        String sql = "SELECT t.order_id, f.flight_date, c.contact_name, t.cabin_class, t.ticket_price, t.order_status " +
                "FROM ticket_order t JOIN flight f ON t.flight_id = f.flight_id " +
                "JOIN passenger_contact c ON t.contact_id = c.contact_id " +
                "WHERE t.passenger_id = ? ORDER BY t.order_time DESC";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, currentUserId);
            ResultSet rs = pstmt.executeQuery();
            System.out.printf("%-8s | %-12s | %-10s | %-10s | %-8s | %-10s\n", "订单ID", "日期", "乘机人", "舱位", "价格", "状态");
            while (rs.next()) {
                System.out.printf("%-8d | %-12s | %-10s | %-10s | %-8.2f | %-10s\n",
                        rs.getInt("order_id"), rs.getDate("flight_date"), rs.getString("contact_name"),
                        rs.getString("cabin_class"), rs.getBigDecimal("ticket_price"), rs.getString("order_status"));
            }
        }

        System.out.print("\n是否需要办理退票? (输入订单ID或按回车跳过): ");
        String orderIdStr = scanner.nextLine();
        if (!orderIdStr.isEmpty()) {
            int oId = Integer.parseInt(orderIdStr);
            conn.setAutoCommit(false);
            try {
                // 1. 获取订单信息并锁定
                String getOrder = "SELECT flight_id, cabin_class FROM ticket_order WHERE order_id = ? AND passenger_id = ? FOR UPDATE";
                long flightId = -1; String cabin = "";
                try (PreparedStatement pstmt = conn.prepareStatement(getOrder)) {
                    pstmt.setInt(1, oId); pstmt.setInt(2, currentUserId);
                    ResultSet rs = pstmt.executeQuery();
                    if(rs.next()){ flightId = rs.getLong("flight_id"); cabin = rs.getString("cabin_class"); }
                    else throw new Exception("订单未找到。");
                }

                // 2. 释放库存
                String restore = "UPDATE flight_cabin SET remain_count = remain_count + 1 WHERE flight_id = ? AND cabin_class = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(restore)) {
                    pstmt.setLong(1, flightId); pstmt.setString(2, cabin);
                    pstmt.executeUpdate();
                }

                // 3. 删除订单 (Task 4 要求支持删除)
                String del = "DELETE FROM ticket_order WHERE order_id = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(del)) {
                    pstmt.setInt(1, oId);
                    pstmt.executeUpdate();
                }

                conn.commit();
                System.out.println("✅ 退票成功，订单已物理删除。");
            } catch (Exception e) {
                conn.rollback();
                System.err.println("❌ 操作失败: " + e.getMessage());
            } finally { conn.setAutoCommit(true); }
        }
    }

    // ==========================================
    // 3. 管理员业务 (极速排班)
    // ==========================================
    private static void showAdminMenu() throws SQLException {
        System.out.println("\n--- 管理控制台 ---");
        System.out.println("1. 极速批量排班 (Generate series)");
        System.out.println("2. 全系统订单审计");
        System.out.println("0. 注销登录");
        System.out.print("请选择: ");
        String choice = scanner.nextLine();

        if (choice.equals("1")) {
            System.out.print("开始日期 (YYYY-MM-DD): "); String start = scanner.nextLine();
            System.out.print("结束日期 (YYYY-MM-DD): "); String end = scanner.nextLine();

            System.out.print("正在执行大规模 SQL 排班任务... ");
            String sqlFlight = "INSERT INTO flight (route_id, flight_date) SELECT route_id, d::date FROM route CROSS JOIN generate_series(?::date, ?::date, '1 day'::interval) d ON CONFLICT DO NOTHING";
            String sqlCabin = "INSERT INTO flight_cabin (flight_id, cabin_class, price, remain_count) " +
                    "SELECT f.flight_id, c.cls, 1000, 100 FROM flight f CROSS JOIN (SELECT unnest(ARRAY['ECONOMY', 'BUSINESS']) as cls) c " +
                    "WHERE f.flight_date BETWEEN ?::date AND ?::date ON CONFLICT DO NOTHING";

            try (PreparedStatement p1 = conn.prepareStatement(sqlFlight);
                 PreparedStatement p2 = conn.prepareStatement(sqlCabin)) {
                p1.setString(1, start); p1.setString(2, end);
                p2.setString(1, start); p2.setString(2, end);
                p1.executeUpdate(); p2.executeUpdate();
                System.out.println("完成！");
            }
        } else if (choice.equals("2")) {
            String sql = "SELECT t.order_id, p.passenger_name, t.ticket_price, t.order_time FROM ticket_order t JOIN passenger p ON t.passenger_id = p.passenger_id ORDER BY t.order_time DESC LIMIT 20";
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                System.out.printf("%-8s | %-15s | %-8s | %-20s\n", "ID", "账户", "金额", "成交时间");
                while (rs.next()) {
                    System.out.printf("%-8d | %-15s | %-8.2f | %-20s\n", rs.getInt("order_id"), rs.getString("passenger_name"), rs.getBigDecimal("ticket_price"), rs.getTimestamp("order_time"));
                }
            }
        } else if (choice.equals("0")) { logout(); }
    }

    private static void logout() {
        currentUserId = null;
        currentRole = "";
        currentUserName = "";
        System.out.println("🔒 已安全登出。");
    }
}