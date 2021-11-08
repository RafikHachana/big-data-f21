import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object MainClass{
    def main(args: Array[String]){

        val conf = new SparkConf().setAppName("appName")
        val sc = new SparkContext(conf)

        val inputPath = args(0)
        val outputPath = args(1)
        
        val textFile = sc.textFile(inputPath)
        val counts = textFile.flatMap(line => line.split(" ")).map(_.replaceAll("""[\p{Punct}]""","").trim).filter(!_.isEmpty)
                         .map(word => (word, 1))
                         .reduceByKey(_ + _)
                         .map(_.swap)
                         .sortByKey(false)
                         .map(_.productIterator.mkString("\t"))
        counts.saveAsTextFile(outputPath)
    }
}


