<H1>Spark 2.0.x dump a csv file from a dataframe containing one array of type string</H1>

I have a dataframe df that contains one column of type array

df.show() looks like

|ID|ArrayOfString|Age|Gender|
+--+-------------+---+------+
|1 | [A,B,D]     |22 | F    |
|2 | [A,Y]       |42 | M    |
|3 | [X]         |60 | F    |

+--+-------------+---+------+
I try to dump that df in a csv file as follow:
val dumpCSV = df.write.csv(path="/home/me/saveDF")


It is not working because of the column ArrayOfString. I get the error:

CSV data source does not support array string data type

The code works if I remove the column ArrayOfString. But I need to keep ArrayOfString!

What would be the best way to dump the csv dataframe including column ArrayOfString

For more spark question check this website Spark Course<a href="https://intellipaat.com/apache-spark-scala-training/">Spark Course</a>

<H2>Answer</h2>
As the error says, The CSV file format doesn't support array type.

So, in order to dump the csv dataframe including column ArrayOfString, you need to express it as a string.

Try the following :

import org.apache.spark.sql.functions._

val stringify = udf((vs: Seq[String]) => vs match {

  case null => null

  case _    => s"""[${vs.mkString(",")}]"""

})

df.withColumn("ArrayOfString", stringify($"ArrayOfString")).write.csv(...)

or

import org.apache.spark.sql.Column

def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))

df.withColumn("ArrayOfString", stringify($"ArrayOfString")).write.csv(...)
