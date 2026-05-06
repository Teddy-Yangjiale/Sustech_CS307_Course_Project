import java.sql.*;
import java.util.Scanner;

public class Client {

    //MySQL 专属连接配置 (注意端口 3306 和 serverTimezone)
    private static final String URL = "jdbc:mysql://localhost:3306/project1?characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    private static final String USER = "checker";
    private static final String PWD = "123456";
    private static Connection conn = null;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Connecting to MySQL database...");
        if (!connectDB()) {
            System.out.println("Failed to connect to database. Exiting...");
            return;
        }
        System.out.println("Connected to MySQL successfully!\n");

        while (true) {
            System.out.println("=====================================");
            System.out.println("  Aviation Ticket System (MySQL)     ");
            System.out.println("=====================================");
            System.out.println("1. Generate tickets");
            System.out.println("2. Search Flights");
            System.out.println("3. Manage Orders (CRUD)");
            System.out.println("0. Exit");
            System.out.print(">> Input: ");

            String choice = scanner.nextLine();

            switch (choice) {
                case "1": generateTickets(scanner); break;
                case "2": searchFlights(scanner); break;
                case "3": manageOrdersMenu(scanner); break;
                case "0":
                    System.out.println("Goodbye!");
                    closeDB();
                    scanner.close();
                    System.exit(0);
                default: System.out.println("Invalid input, please try again.\n");
            }
        }
    }

    // 1. Generate tickets
    private static void generateTickets(Scanner scanner) {
        System.out.println("\n--- Generate Tickets (Statistics) ---");
        System.out.print("Enter Start Date (YYYY-MM-DD): ");
        String startDate = scanner.nextLine();
        System.out.print("Enter End Date (YYYY-MM-DD): ");
        String endDate = scanner.nextLine();

        String sql = "SELECT " +
                "    COUNT(DISTINCT f.flight_id) AS total_flights, " +
                "    SUM(CASE WHEN fc.cabin_class = 'BUSINESS' THEN fc.remain_count ELSE 0 END) AS business_tickets, " +
                "    SUM(CASE WHEN fc.cabin_class = 'ECONOMY' THEN fc.remain_count ELSE 0 END) AS economy_tickets " +
                "FROM flight f " +
                "JOIN flight_cabin fc ON f.flight_id = fc.flight_id " +
                "WHERE f.flight_date BETWEEN ? AND ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDate(1, java.sql.Date.valueOf(startDate));
            pstmt.setDate(2, java.sql.Date.valueOf(endDate));
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                long totalFlights = rs.getLong("total_flights");
                if (totalFlights == 0) {
                    System.out.println("No flights found in this date range.");
                } else {
                    System.out.println("\nStatistics for " + startDate + " to " + endDate + ":");
                    System.out.println("--------------------------------------------------");
                    System.out.printf("Total Flights Scheduled : %d\n", totalFlights);
                    System.out.printf("Business Class Tickets  : %d available\n", rs.getLong("business_tickets"));
                    System.out.printf("Economy Class Tickets   : %d available\n", rs.getLong("economy_tickets"));
                    System.out.println("--------------------------------------------------\n");
                }
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid date format. Please use YYYY-MM-DD.");
        } catch (SQLException e) {
            System.out.println("Database Error: " + e.getMessage());
        }
    }

    // 2. Search Flights (MySQL 使用 LIKE)
    private static void searchFlights(Scanner scanner) {
        System.out.println("\n--- Search Flights ---");
        System.out.print("Enter Source City or IATA Code (e.g., Beijing or PEK): ");
        String source = scanner.nextLine();
        System.out.print("Enter Destination City or IATA Code (e.g., Shanghai or SHA): ");
        String dest = scanner.nextLine();
        System.out.print("Enter Flight Date (YYYY-MM-DD): ");
        String date = scanner.nextLine();

        String sql = "SELECT " +
                "    f.flight_id, r.flight_number, a.airline_name, " +
                "    src.iata_code AS src_code, dst.iata_code AS dst_code, " +
                "    r.scheduled_departure_time, r.scheduled_arrival_time, " +
                "    fc.cabin_class, fc.price, fc.remain_count " +
                "FROM flight f " +
                "JOIN route r ON f.route_id = r.route_id " +
                "JOIN airline a ON r.airline_id = a.airline_id " +
                "JOIN airport src ON r.source_airport_id = src.airport_id " +
                "JOIN airport dst ON r.destination_airport_id = dst.airport_id " +
                "JOIN flight_cabin fc ON f.flight_id = fc.flight_id " +
                "WHERE f.flight_date = ? " +
                "  AND (src.city LIKE ? OR src.iata_code LIKE ?) " +
                "  AND (dst.city LIKE ? OR dst.iata_code LIKE ?) " +
                "  AND fc.remain_count > 0 " +
                "ORDER BY r.scheduled_departure_time ASC";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDate(1, java.sql.Date.valueOf(date));
            pstmt.setString(2, source);
            pstmt.setString(3, source);
            pstmt.setString(4, dest);
            pstmt.setString(5, dest);
            ResultSet rs = pstmt.executeQuery();

            System.out.println("\n--- Flight Results for " + date + " ---");
            System.out.printf("%-10s %-10s %-15s %-10s %-10s %-10s %-12s %-8s %-10s\n",
                    "FlightID", "FlightNo", "Airline", "From", "To", "Departs", "Arrives", "Class", "Price");
            System.out.println("--------------------------------------------------------------------------------------------------");

            boolean hasData = false;
            while (rs.next()) {
                hasData = true;
                String depTime = rs.getString("scheduled_departure_time").substring(0, 5);
                String arrTime = rs.getString("scheduled_arrival_time").substring(0, 5);
                System.out.printf("%-10d %-10s %-15s %-10s %-10s %-10s %-12s %-8.2s %-10.2f\n",
                        rs.getLong("flight_id"), rs.getString("flight_number"),
                        rs.getString("airline_name").length() > 13 ? rs.getString("airline_name").substring(0, 13) + ".." : rs.getString("airline_name"),
                        rs.getString("src_code"), rs.getString("dst_code"), depTime, arrTime,
                        rs.getString("cabin_class"), rs.getDouble("price"));
            }
            if (!hasData) System.out.println("No available flights found for this route and date.");
            System.out.println("--------------------------------------------------------------------------------------------------\n");
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid date format. Please use YYYY-MM-DD.");
        } catch (SQLException e) {
            System.out.println("Database Error: " + e.getMessage());
        }
    }

    private static void manageOrdersMenu(Scanner scanner) {
        while (true) {
            System.out.println("\n--- Manage Orders ---");
            System.out.println("1. Book Ticket (增 - Create)");
            System.out.println("2. View My Orders (查 - Read)");
            System.out.println("3. Cancel Order (改 - Update)");
            System.out.println("4. Delete Order (删 - Delete)");
            System.out.println("0. Return to Main Menu");
            System.out.print(">> Input: ");
            String choice = scanner.nextLine();
            if (choice.equals("0")) break;

            try {
                switch (choice) {
                    case "1": bookTicket(scanner); break;
                    case "2": viewOrders(scanner); break;
                    case "3": cancelOrder(scanner); break;
                    case "4": deleteOrder(scanner); break;
                    default: System.out.println("Invalid input.");
                }
            } catch (SQLException e) {
                System.out.println("Database Error: " + e.getMessage());
                try { if (conn != null) conn.rollback(); } catch (SQLException ex) { ex.printStackTrace(); }
            }
        }
    }

    // 1. 增：买票 (MySQL 行级锁机制与PG一致，代码无需修改)
    private static void bookTicket(Scanner scanner) throws SQLException {
        System.out.print("Enter Passenger ID: ");
        int passengerId = Integer.parseInt(scanner.nextLine());
        System.out.print("Enter Flight ID: ");
        long flightId = Long.parseLong(scanner.nextLine());
        System.out.print("Enter Cabin Class (BUSINESS/ECONOMY): ");
        String cabinClass = scanner.nextLine().toUpperCase();
        System.out.print("Enter Ticket Price: ");
        double price = Double.parseDouble(scanner.nextLine());

        conn.setAutoCommit(false);

        String updateCabinSql = "UPDATE flight_cabin SET remain_count = remain_count - 1 WHERE flight_id = ? AND cabin_class = ? AND remain_count > 0";
        try (PreparedStatement pstmt1 = conn.prepareStatement(updateCabinSql)) {
            pstmt1.setLong(1, flightId);
            pstmt1.setString(2, cabinClass);
            if (pstmt1.executeUpdate() == 0) {
                System.out.println("Booking failed: Not enough tickets or flight not found.");
                conn.rollback();
                return;
            }
        }

        String insertOrderSql = "INSERT INTO ticket_order (passenger_id, flight_id, cabin_class, order_status, ticket_price) VALUES (?, ?, ?, 'PAID', ?)";
        try (PreparedStatement pstmt2 = conn.prepareStatement(insertOrderSql)) {
            pstmt2.setInt(1, passengerId);
            pstmt2.setLong(2, flightId);
            pstmt2.setString(3, cabinClass);
            pstmt2.setDouble(4, price);
            pstmt2.executeUpdate();
        }

        conn.commit();
        System.out.println("Ticket booked successfully!");
        conn.setAutoCommit(true);
    }

    // 2. 查：查订单
    private static void viewOrders(Scanner scanner) throws SQLException {
        System.out.print("Enter Passenger ID to search: ");
        int passengerId = Integer.parseInt(scanner.nextLine());

        String sql = "SELECT order_id, flight_id, cabin_class, order_status, ticket_price, order_time FROM ticket_order WHERE passenger_id = ? ORDER BY order_time DESC";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, passengerId);
            ResultSet rs = pstmt.executeQuery();
            System.out.println("\n--- Order History ---");
            System.out.printf("%-10s %-10s %-15s %-15s %-10s %-25s\n", "OrderID", "FlightID", "Cabin", "Status", "Price", "Time");
            boolean hasData = false;
            while (rs.next()) {
                hasData = true;
                System.out.printf("%-10d %-10d %-15s %-15s %-10.2f %-25s\n",
                        rs.getLong("order_id"), rs.getLong("flight_id"), rs.getString("cabin_class"),
                        rs.getString("order_status"), rs.getDouble("ticket_price"), rs.getTimestamp("order_time").toString());
            }
            if (!hasData) System.out.println("No orders found for this passenger.");
            System.out.println("---------------------\n");
        }
    }

    // 3. 改：退票 (MySQL 重构版：取消 RETURNING，先查后改)
    private static void cancelOrder(Scanner scanner) throws SQLException {
        System.out.print("Enter Order ID to cancel: ");
        long orderId = Long.parseLong(scanner.nextLine());

        conn.setAutoCommit(false);
        long flightId = -1;
        String cabinClass = "";

        // 步骤 1：先查询订单的航班和舱位信息
        String selectOrderSql = "SELECT flight_id, cabin_class FROM ticket_order WHERE order_id = ? AND order_status = 'PAID'";
        try (PreparedStatement pstmtSelect = conn.prepareStatement(selectOrderSql)) {
            pstmtSelect.setLong(1, orderId);
            ResultSet rs = pstmtSelect.executeQuery();
            if (rs.next()) {
                flightId = rs.getLong("flight_id");
                cabinClass = rs.getString("cabin_class");
            } else {
                System.out.println("Cancellation failed: Order not found or already cancelled.");
                conn.rollback();
                return;
            }
        }

        // 步骤 2：更新订单状态为 CANCELLED
        String updateOrderSql = "UPDATE ticket_order SET order_status = 'CANCELLED' WHERE order_id = ?";
        try (PreparedStatement pstmtUpdate = conn.prepareStatement(updateOrderSql)) {
            pstmtUpdate.setLong(1, orderId);
            pstmtUpdate.executeUpdate();
        }

        // 步骤 3：加回余票库存
        String updateCabinSql = "UPDATE flight_cabin SET remain_count = remain_count + 1 WHERE flight_id = ? AND cabin_class = ?";
        try (PreparedStatement pstmtCabin = conn.prepareStatement(updateCabinSql)) {
            pstmtCabin.setLong(1, flightId);
            pstmtCabin.setString(2, cabinClass);
            pstmtCabin.executeUpdate();
        }

        conn.commit();
        System.out.println("Order cancelled successfully! Ticket returned to pool.");
        conn.setAutoCommit(true);
    }

    // 4. 删：删除订单
    private static void deleteOrder(Scanner scanner) throws SQLException {
        System.out.print("Enter Order ID to PERMANENTLY delete: ");
        long orderId = Long.parseLong(scanner.nextLine());
        String sql = "DELETE FROM ticket_order WHERE order_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, orderId);
            if (pstmt.executeUpdate() > 0) {
                System.out.println("Order deleted permanently.");
            } else {
                System.out.println("Delete failed: Order not found.");
            }
        }
    }

    // 数据库连接 ( MySQL 专属驱动)
    private static boolean connectDB() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(URL, USER, PWD);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private static void closeDB() {
        try { if (conn != null) conn.close(); } catch (SQLException e) { e.printStackTrace(); }
    }
}