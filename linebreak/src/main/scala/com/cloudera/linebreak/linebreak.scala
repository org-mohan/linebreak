import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import com.databricks.spark.csv

object linebreak {

	def convert(sqlContext: SQLContext, filename: String, schema: StructType, tablename: String) {
		  // import text-based table first into a data frame.
		  // make sure to use com.databricks:spark-csv version 1.3+ 
		  // which has consistent treatment of empty strings as nulls.
		  val df = sqlContext.read
			.format("com.databricks.spark.csv")
			.schema(schema)
			.option("delimiter",",")
			.option("nullValue","")
			.option("treatEmptyValuesAsNulls","true")
			.load(filename)
		  // now simply write to a parquet file

			df.registerTempTable("temperature")
		  val dfLinbreak = sqlContext.sql("select statecode, degrees,param from temperature")
			dfLinbreak.write.format("com.databricks.spark.csv")
				.option("delimiter",",")
				.option("nullValue","")
				.option("treatEmptyValuesAsNulls","true")
				.save("/user/cloudera/"+tablename)
	  }
	  
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("linebreak"))
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // get threshold
     val inputfilename = args(0)

		////  // usage exampe -- a tpc-ds table called catalog_page
	val schema= StructType(Array(
				StructField("statecode",StringType,false),
				StructField("countrycode",StringType,false),
				StructField("sitenum",StringType,false),
				StructField("paramcode",StringType,false),
				StructField("poc",StringType,false),
				StructField("latitude",StringType,false),
				StructField("longitude",StringType,false),
				StructField("datum",StringType,false),
				StructField("param",StringType,false),
				StructField("datelocal",StringType,false),
				StructField("timelocal",StringType,false),
				StructField("dategmt",StringType,false),
				StructField("timegmt",StringType,false),
				StructField("degrees",StringType,false),
				StructField("uom",StringType,false),
				StructField("mdl",StringType,false),
				StructField("uncert",StringType,false),
				StructField("qual",StringType,false),
				StructField("method",StringType,false),
				StructField("methodname",StringType,false),
				StructField("state",StringType,false),
				StructField("county",StringType,false),
				StructField("dateoflastchange",StringType,false),
				StructField("col_new",StringType,false)
		  ))

  convert(sqlContext,
          inputfilename,
          schema,
          "linebreak")
	
  } 
}
