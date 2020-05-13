package sqlite.connect.net.sqlitetutorial;
import java.io.*;
import java.sql.*;

public class CSVToDatabaseInserter {

    public static void main(String[] args) {
        String jdbcURL = "jdbc:sqlite:C:/sqlite/CSVDatabase.db";
        String username = "user";
        String password = "password";

        String csvFilePath = "CSVData.csv";

        int batchSize = 20;

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO People (FirstName, LastName, Email, Gender, Image, PaymentMethod, Payment, Boolean, Boolean2, City) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;

            int count = 0;

            lineReader.readLine(); // skip header line

            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                String courseName = data[0];
                String studentName = data[1];
                String timestamp = data[2];
                String rating = data[3];
                String comment = data.length == 5 ? data[4] : "";

                statement.setString(1, courseName);
                statement.setString(2, studentName);

                Timestamp sqlTimestamp = Timestamp.valueOf(timestamp);
                statement.setTimestamp(3, sqlTimestamp);

                Float fRating = Float.parseFloat(rating);
                statement.setFloat(4, fRating);

                statement.setString(5, comment);

                statement.addBatch();

                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }

            lineReader.close();

            // execute the remaining queries
            statement.executeBatch();

            connection.commit();
            connection.close();

        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}