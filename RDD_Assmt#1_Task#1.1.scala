import scala.collection.mutable.ListBuffer
// read the CSV file
val csv = sc.textFile("student_dataset.txt")
// split & clean data
val headerAndRows = csv.map(line => line.split(",").map(_.trim))
// get header
val header = headerAndRows.first
// get data (filter out header)
val data = headerAndRows.filter(_(0) != header(0))
// count of total number of rows present
data.count()
// distinct number of subjects present in the entire school
var nrows:Int = data.count().toInt - 1
var subjlist = new ListBuffer[String]()
for( a <- 0 to nrows) {
subjlist += data.collect()(a)(1)
}
var subjset = subjlist.toSet
println(subjset.size)
// splits to map (header/value pairs)
val maps = data.map(splits => header.zip(splits).toMap)
// count of the number of students in the school, whose name is Mathew and marks are 55
var nam_mrk = maps.filter(map => map("name") == "Mathew" && map("marks") == "55")
println(nam_mrk.count)
