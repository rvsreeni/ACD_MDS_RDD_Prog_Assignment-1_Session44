val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//Read CSV data file
val stdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("student_dataset.txt")
//What is the count of students per grade in the school?
stdf.groupBy("grade").count().show()
//Find the average of each student
stdf.groupBy("name","grade").avg("marks").show()
//What is the average score of students in each subject across all grades?
stdf.groupBy("subject").avg("marks").show()
//What is the average score of students in each subject per grade?
stdf.groupBy("subject","grade").avg("marks").show()
//For all students in grade-2, how many have average score greater than 50?
val gr2df = stdf.filter(stdf("grade") === "grade-2").groupBy("grade").avg("marks").show()
