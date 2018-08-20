val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//Read CSV data files
val dhol_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("Dataset_Holidays.txt")
val dtrpt_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("Dataset_Transport.txt")
val dusr_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("Dataset_User_details.txt")

//Join of Holidays, Transport, User_Details
val dholusr_df = dhol_df.join(dusr_df, dhol_df.col("userid") === dusr_df.col("userid"))
val dholusrtrpt_df = dholusr_df.join(dtrpt_df, dholusr_df.col("travelmode") === dtrpt_df.col("travelmode"))

//What is the distribution of the total number of air-travelers per year?
dholusrtrpt_df.groupBy("yearoftravel","name").count().orderBy("yearoftravel").show()
//What is the total air distance covered by each user per year?
dholusrtrpt_df.groupBy("yearoftravel","name").sum("distance").orderBy("yearoftravel").show()
//Which user has travelled the largest distance till date?
dholusrtrpt_df.groupBy("name").sum("distance").sort(desc("sum(distance)")).show()
//What is the most preferred destination for all users?
dholusrtrpt_df.groupBy("dest").count().sort(desc("count")).show()}
