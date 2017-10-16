package nobleprog

import java.sql.{Connection, DriverManager}

/**
  * Created by allwefantasy on 11/9/2017.
  */
object JDBCExample {
  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "org.apache.hive.jdbc.HiveDriver"
    val url = "jdbc:hive2://localhost:10000/default"

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM zhl_table ")
      while ( resultSet.next() ) {
        println(" city = "+ resultSet.getString("city") )
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }
}
