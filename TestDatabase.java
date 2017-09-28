package coreservlets;

import java.sql.*;

public class TestDatabase {
  private String driver;
  private String url;
  private String username;
  private String password;

  public TestDatabase(String driver, String url,
                      String username, String password) {
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
  }

  /** Test the JDBC connection to the database and report the
   *  product name and product version.
   */

  public void testConnection() {
    System.out.println();
    System.out.println("Testing database connection ...\n");
    Connection connection = getConnection();
    if (connection == null) {
      System.out.println("Test failed.");
      return;
    }
    try {
      DatabaseMetaData dbMetaData = connection.getMetaData();
      String productName =
        dbMetaData.getDatabaseProductName();
      String productVersion =
        dbMetaData.getDatabaseProductVersion();
      String driverName = dbMetaData.getDriverName();
      String driverVersion = dbMetaData.getDriverVersion();
      System.out.println("Driver: " + driver);
      System.out.println("URL: " + url);
      System.out.println("Username: " + username);
      System.out.println("Password: " + password);
      System.out.println("Product name: " + productName);
      System.out.println("Product version: " + productVersion);
      System.out.println("Driver Name: " + driverName);
      System.out.println("Driver Version: " + driverVersion);
    } catch(SQLException sqle) {
      System.err.println("Error connecting: " + sqle);
    } finally {
      closeConnection(connection);
    }
    System.out.println();
  }

  public void createTable() {
    System.out.print("Creating authors table ... ");
    Connection connection = getConnection();
    if (connection == null) {
      System.out.println("failure");
      return;
    }
    try {
      String format =
        "(id INTEGER, first_name VARCHAR(12), " +
        " last_name VARCHAR(12))";
      String[] rows = { "(1, 'Marty', 'Hall')",
                        "(2, 'Larry', 'Brown')" };
      Statement statement = connection.createStatement();
      // Drop previous table if it exists, but don't get
      // error if not. Thus, the separate try/catch here.
      try {
        statement.execute("DROP TABLE authors");
      } catch(SQLException sqle) {}
      String createCommand =
        "CREATE TABLE authors " + format;
      statement.execute(createCommand);
      String insertPrefix =
        "INSERT INTO authors VALUES";
      for(int i=0; i<rows.length; i++) {
        statement.execute(insertPrefix + rows[i]);
      }
      System.out.println("successful");
    } catch(SQLException sqle) {
      System.out.println("failure");
      System.err.println("Error creating table: " + sqle);
    } finally {
      closeConnection(connection);
    }
    System.out.println();
  }

  /** Query all rows in the "authors" table. */

  public void executeQuery() {
    System.out.println("Querying authors table ... ");
    Connection connection = getConnection();
    if (connection == null) {
      System.out.println("Query failed.");
      return;
    }
    try {
      Statement statement = connection.createStatement();
      String query = "SELECT * FROM authors";
      ResultSet resultSet = statement.executeQuery(query);
      ResultSetMetaData resultSetMetaData =
        resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      // Print out columns
      String[] columns = new String[columnCount];
      int[] widths = new int[columnCount];
      for(int i=1; i <= columnCount; i++) {
        columns[i-1] = resultSetMetaData.getColumnName(i);
        widths[i-1] = resultSetMetaData.getColumnDisplaySize(i);
      }
      System.out.println(makeSeparator(widths));
      System.out.println(makeRow(columns, widths));
      // Print out rows
      System.out.println(makeSeparator(widths));
      String[] rowData = new String[columnCount];
      while(resultSet.next()) {
        for(int i=1; i <= columnCount; i++) {
          rowData[i-1] = resultSet.getString(i);
        }
        System.out.println(makeRow(rowData, widths));
      }
      System.out.println(makeSeparator(widths));
    } catch(SQLException sqle) {
      System.err.println("Error executing query: " + sqle);
    } finally {
      closeConnection(connection);
    }
    System.out.println();
  }

  public void checkJDBCVersion() {
    System.out.println();
    System.out.println("Checking JDBC version ...\n");
    Connection connection = getConnection();
    if (connection == null) {
      System.out.println("Check failed.");
      return;
    }
    int majorVersion = 1;
    int minorVersion = 0;
    try {
      Statement statement = connection.createStatement(
                              ResultSet.TYPE_SCROLL_INSENSITIVE,
                              ResultSet.CONCUR_READ_ONLY);
      String query = "SELECT * FROM authors";
      ResultSet resultSet = statement.executeQuery(query);
      resultSet.last(); // JDBC 2.0
      majorVersion = 2;
    } catch(SQLException sqle) {
      // Ignore - last() not supported
    }
    try {
      DatabaseMetaData dbMetaData = connection.getMetaData();
      majorVersion = dbMetaData.getJDBCMajorVersion(); // JDBC 3.0
      minorVersion = dbMetaData.getJDBCMinorVersion(); // JDBC 3.0
    } catch(Throwable throwable) {
      // Ignore - methods not supported
    } finally {
      closeConnection(connection);
    }
    System.out.println("JDBC Version: " +
                       majorVersion + "." + minorVersion);
  }

  // A String of the form "|  xxx |  xxx |  xxx |"

  private String makeRow(String[] entries, int[] widths) {
    String row = "|";
    for(int i=0; i<entries.length; i++) {
      row = row + padString(entries[i], widths[i], " ");
      row = row + " |";
    }
    return(row);
  }

  // A String of the form "+------+------+------+"

  private String makeSeparator(int[] widths) {
    String separator = "+";
    for(int i=0; i<widths.length; i++) {
      separator += padString("", widths[i] + 1, "-") + "+";
    }
    return(separator);
  }

  private String padString(String orig, int size,
                           String padChar) {
    if (orig == null) {
      orig = "<null>";
    }
    // Use StringBuffer, not just repeated String concatenation
    // to avoid creating too many temporary Strings.
    StringBuffer buffer = new StringBuffer(padChar);
    int extraChars = size - orig.length();
    buffer.append(orig);
    for(int i=0; i<extraChars; i++) {
      buffer.append(padChar);
    }
    return(buffer.toString());
  }

  /** Obtain a new connection to the database or return
   *  null on failure.
   */

  public Connection getConnection() {
    try {
      Class.forName(driver);
      Connection connection =
        DriverManager.getConnection(url, username,
                                    password);
      return(connection);
    } catch(ClassNotFoundException cnfe) {
      System.err.println("Error loading driver: " + cnfe);
      return(null);
    } catch(SQLException sqle) {
      System.err.println("Error connecting: " + sqle);
      return(null);
    }
  }

  /** Close the database connection. */

  private void closeConnection(Connection connection) {
    try {
      connection.close();
    } catch(SQLException sqle) {
      System.err.println("Error closing connection: " + sqle);
      connection = null;
    }
  }

  public static void main(String[] args) {
    if (args.length < 5) {
      printUsage();
      return;
    }
    String vendor = args[4];
    // Change to DriverUtilities2.loadDrivers() to
    // load the drivers from an XML file.
    DriverUtilities.loadDrivers();
    if (!DriverUtilities.isValidVendor(vendor)) {
      printUsage();
      return;
    }
    String driver = DriverUtilities.getDriver(vendor);
    String host = args[0];
    String dbName = args[1];
    String url =
      DriverUtilities.makeURL(host, dbName, vendor);
    String username = args[2];
    String password = args[3];

    TestDatabase database =
      new TestDatabase(driver, url, username, password);
    database.testConnection();
    database.createTable();
    database.executeQuery();
    database.checkJDBCVersion();
  }
  private static void printUsage() {
    System.out.println("Usage: TestDatabase host dbName " +
                       "username password vendor.");
  }
}        