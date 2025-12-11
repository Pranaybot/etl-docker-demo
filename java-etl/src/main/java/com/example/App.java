
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class App {
    public static void main(String[] args) throws Exception {
        String url = System.getenv("JDBC_URL");
        String user = System.getenv("DB_USER");
        String password = System.getenv("DB_PASSWORD");

        System.out.println("Java ETL starting...");

        // Try a few times in case Postgres isn't ready yet
        int retries = 10;
        while (retries > 0) {
            try (Connection conn = DriverManager.getConnection(url, user, password)) {
                System.out.println("Connected to Postgres from Java ETL.");

                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS java_etl_data (" +
                        "  id SERIAL PRIMARY KEY," +
                        "  message TEXT NOT NULL" +
                        ")"
                    );
                }

                String sql = "INSERT INTO java_etl_data (message) VALUES (?)";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, "Hello from Java ETL");
                    ps.executeUpdate();
                }

                System.out.println("Java ETL inserted a row into java_etl_data.");
                break;
            } catch (Exception e) {
                retries--;
                System.out.println("Java ETL: waiting for Postgres... (" + retries + " retries left)");
                Thread.sleep(3000);
            }
        }

        System.out.println("Java ETL finished.");
    }
}
