import org.apache.spark.sql.SparkSession

object dwconnector {

  def main(args: Array[String]):Unit = {

    val spark = SparkSession.builder().appName("DWDataSource").master("local").getOrCreate()

    spark.conf.set("fs.azure.account.key.lazhudatalakev2.dfs.core.windows.net", "EwDuW5uAbKlBcYCPPL75Yn9sF6idr9IHpp8KS0ETFDtkuAoJ5aTkUoUl1awhCrHzDq4A0iArVjKRg/idD37Btg==")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.lazhudatalakev2.dfs.core.windows.net", "EwDuW5uAbKlBcYCPPL75Yn9sF6idr9IHpp8KS0ETFDtkuAoJ5aTkUoUl1awhCrHzDq4A0iArVjKRg/idD37Btg==")

    val df = spark.read.format("com.microsoft.lazhu")
      .option("url", "jdbc:sqlserver://lazhusynapse.sql.azuresynapse.net:1433;database=sqlpool;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30")
      .option("query", "select * from t14")
      .option("user", "lazhu@lazhusynapse")
      .option("password", "Password01!")
      .option("externalDataSource", "dwconnector_external_data_source")
      .option("externalFileFormat", "dwconnector_external_file_format")
      .load()

    df.write.format("com.microsoft.lazhu")
      .option("url", "jdbc:sqlserver://lazhusynapse.sql.azuresynapse.net:1433;database=sqlpool;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30")
      .option("user", "lazhu@lazhusynapse")
      .option("password", "Password01!")
      .option("stagingdir", "abfss://lazhusynapse@lazhudatalakev2.dfs.core.windows.net/temp")
      .option("dbTable", "t15")
      .option("tableOptions", "HEAP, DISTRIBUTION=HASH([c1])").mode("overwrite").save()
  }

}