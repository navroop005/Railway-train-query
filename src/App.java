import java.io.File;
import java.io.FileWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;

public class App {

    public static Connection dbConnection;

    public static void main(String[] args) {
        dbConnection = open_database();
        initialize_db();
        read_trains_file("./Inputs/trains.csv");
        read_input("./Inputs/client_input.txt", "./Outputs/client_output.txt");
    }

    public static void initialize_db() {
        File schemaFile = new File("./sql/schema.pgsql");
        try {
            Scanner sc = new Scanner(schemaFile);
            String query = "";
            while (sc.hasNextLine()) {
                query += sc.nextLine() + " ";
            }
            Statement stmt = dbConnection.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
            sc.close();

            try {
                query = "CREATE TYPE train_rec as (src_train_id INTEGER, src_depart_time TIME(0), dest_train_id INTEGER, dest_arrival_time TIME(0), intm_station_code VARCHAR, intm_arrival TIME(0), intm_depart TIME(0), src_days INTEGER, dest_days INTEGER)";
                stmt = dbConnection.createStatement();
                stmt.execute(query);
                stmt.close();
            } catch (Exception e) {
            }

            File functionFile = new File("./sql/functions.pgsql");
            sc = new Scanner(functionFile);
            query = "";
            while (sc.hasNextLine()) {
                query += sc.nextLine() + " ";
            }
            stmt = dbConnection.createStatement();
            stmt.executeUpdate(query);
            stmt.close();
            sc.close();
        } catch (Exception e) {
            print_exception(e);
        }
    }

    public static void read_trains_file(String inp_file) {
        File file = new File(inp_file);
        String train_query = "{CALL insert_train(?, ?)}";
        String station_query = "{CALL insert_station(?, ?)}";
        String route_query = "{CALL insert_route(?, ?, ?, ?, ?, ?)}";
        try {
            Scanner sc = new Scanner(file);
            LocalTime prev_depart = LocalTime.MIN;
            LocalTime prev_arrival = LocalTime.MIN;
            int prev_train_id = -1;
            int day = 1;
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                StringTokenizer st = new StringTokenizer(line, ",");
                String s = st.nextToken().trim().replace("'", "");
                int train_id = Integer.parseInt(s);
                String train_name = st.nextToken().trim();
                int station_order = Integer.parseInt(st.nextToken().trim());
                String station_code = st.nextToken().trim();
                String station_name = st.nextToken().trim();
                s = st.nextToken().trim().replace("'", "");
                LocalTime arival_time = LocalTime.parse(s);
                s = st.nextToken().trim().replace("'", "");
                LocalTime depart_time = LocalTime.parse(s);
                if (train_id != prev_train_id) {
                    day = 1;
                    prev_train_id = train_id;
                } else if (arival_time.isBefore(prev_depart) || arival_time.isBefore(prev_arrival)) {
                    day++;
                    // System.out.println("++");
                }
                prev_depart = depart_time;
                // System.out.println(train_id + ", " + train_name + ", " + station_order + ", "
                // + station_code + ", "
                // + station_name + ", " + arival_time + ", " + depart_time + ", " + day);

                CallableStatement stmt = dbConnection.prepareCall(train_query);
                stmt.setInt(1, train_id);
                stmt.setString(2, train_name);
                stmt.execute();
                stmt.close();
                stmt = dbConnection.prepareCall(station_query);
                stmt.setString(1, station_code);
                stmt.setString(2, station_name);
                stmt.execute();
                stmt.close();
                stmt = dbConnection.prepareCall(route_query);
                stmt.setInt(1, train_id);
                stmt.setInt(2, station_order);
                stmt.setString(3, station_code);
                stmt.setObject(4, arival_time);
                stmt.setObject(5, depart_time);
                stmt.setInt(6, day);
                // System.out.println(stmt);
                stmt.execute();
                stmt.close();
            }
            sc.close();
        } catch (Exception e) {
            print_exception(e);
        }
        System.out.println("Routes added");
    }

    public static Connection open_database() {
        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            Properties properties = new Properties();
            properties.setProperty("user", "postgres");
            properties.setProperty("password", "12345");
            properties.setProperty("escapeSyntaxCallMode", "callIfNoReturn");

            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/railway_train_query", properties);

        } catch (Exception e) {
            print_exception(e);
        }
        System.out.println("Opened database successfully");
        return c;
    }

    public static void read_input(String in_file, String out_file) {
        File inFile =new  File(in_file);
        File outFile =new  File(out_file);
        try {
            Scanner sc = new Scanner(inFile);
            FileWriter fw = new FileWriter(outFile);
            while (sc.hasNextLine()) {
                String s = sc.nextLine();
                String[] sl = s.split(" ");
                String response = get_trains(sl[0], sl[1]);
                fw.write(response);
                fw.write("\n");
            }
            sc.close();
            fw.close();
        } catch (Exception e) {
            print_exception(e);
        }
    }
    public static String get_trains(String source, String dest) {
        String response = "";
        try {
            String query = "SELECT * FROM find_trains(? ,?)";
            PreparedStatement stmt = dbConnection.prepareStatement(query);
            stmt.setString(1, source);
            stmt.setString(2, dest);

            ResultSet rs = stmt.executeQuery();

            int prev_train_id = 0;

            while (rs.next()) {
                int src_train_id = rs.getInt("src_train_id");
                int dest_train_id = rs.getInt("dest_train_id");
                LocalTime src_depart_time = rs.getTime("src_depart_time").toLocalTime();
                LocalTime dest_arrival_time = rs.getTime("dest_arrival_time").toLocalTime();
                String intm_station_code = rs.getString("intm_station_code");
                LocalTime intm_depart = rs.getTime("intm_depart").toLocalTime();
                LocalTime intm_arrival = rs.getTime("intm_arrival").toLocalTime();
                int src_days = rs.getInt("src_days");
                int dest_days = rs.getInt("dest_days");
                Duration total_time = Duration.between(src_depart_time, dest_arrival_time);
                total_time = total_time.plusDays(src_days + dest_days);

                if (src_train_id == dest_train_id && src_train_id != prev_train_id) {
                    response += "Train: " + src_train_id + ", Depart time: " + src_depart_time + ", Arrival time: "
                            + dest_arrival_time + ", Total Time: " + total_time.toDays()+ " days "
                             + total_time.toHoursPart()+ " hours " + total_time.toMinutesPart() + " mins\n";
                    
                } else if (src_train_id != dest_train_id) {
                    Duration intm_wait = Duration.between(intm_arrival, intm_depart);
                    if (intm_wait.isNegative()) {
                        intm_wait = intm_wait.plusHours(24);
                    }
                    total_time = total_time.plus(intm_wait);
                    response += "Train: " + src_train_id + ", Depart time: " + src_depart_time
                            + ", Intermediate Arrival time: " + intm_arrival + ", Intemediate Station: " +
                            intm_station_code + ", Intermediate wait time: "
                            + intm_wait.toHours() +":" +intm_wait.toMinutesPart()+", Intermediate Departure time: "
                            + intm_depart + ", Second Train: " + dest_train_id + ", Destination Arrival time: " +
                            dest_arrival_time + ", Total Time: " + total_time.toDays()+ " days "
                            + total_time.toHoursPart()+ " hours " + total_time.toMinutesPart() + " mins\n";
                }
                prev_train_id = src_train_id;
            }
        } catch (Exception e) {
            print_exception(e);
        }
        if (response.length() == 0) {
            response = "No train available\n";
        }
        return response;
    }

    public static void print_exception(Exception e) {
        e.printStackTrace();
        System.err.println(e.getClass().getName() + ": " + e.getMessage());
        System.exit(0);
    }
}