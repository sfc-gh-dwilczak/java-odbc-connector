import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SnowflakeExample {

    public static void main(String[] args) {
        /* 
            Snowflake login information and object to be used. This would be
            given to you once the user runs the connector code.
        */ 
        String account_identifer = "GGB82720";
        String role              = "service_syndigo";
        String warehouse         = "service_syndigo";
        String user              = "service_syndigo";
        String password          = "Password12";
        String database          = "SYNDIGO";

        String url = "jdbc:snowflake://"+account_identifer+".snowflakecomputing.com/?warehouse="+warehouse+"&role="+role;
       
        // You can choose this. I'm just using the product as an example.
        String schema = "products";

        // JDBC connection object
        Connection connection = null;

        try {
            // Load the Snowflake JDBC driver
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

            // Establish the connection
            connection = DriverManager.getConnection(url, user, password);

            // Create a Statement object
            Statement statement = connection.createStatement();
            

            // Create the schema.
            statement.execute("CREATE schema IF NOT EXISTS " + database + "." + schema);
            // Create the table.
            statement.execute("CREATE TABLE IF NOT EXISTS " + database + "." + schema + ".custumer (" +
                "id INT, " +
                "name VARCHAR(255), " +
                "customer VARCHAR(255)" +
                ")"
            );
            // Insert some data.
            statement.execute("INSERT INTO " + database + "." + schema + ".custumer (id, name, customer) VALUES " +
                "(1, 'John Doe', 'Company A'), " +
                "(2, 'Jane Smith', 'Company B'), " +
                "(3, 'Bob Johnson', 'Company C'), " +
                "(4, 'Alice Brown', 'Company A'), " +
                "(5, 'Charlie White', 'Company B')"
            );

            System.out.println("Created successfully.");

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            // Close the connection in the finally block to ensure proper cleanup
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
