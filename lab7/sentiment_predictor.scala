import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


import org.apache.spark.ml.feature.{Tokenizer, RegexTokenizer}
import org.apache.spark.ml.feature.{HashingTF, IDF}


import org.apache.spark.ml.classification.{LogisticRegression}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator





object MainClass{
    def main(args: Array[String]){

        val conf = new SparkConf().setAppName("appName")
        val sc = new SparkContext(conf)

        val spark = SparkSession
                    .builder()
                    .appName("appName")
                    .getOrCreate()
        import spark.implicits._

        

        val inputPath = args(0)
        val outputPath = args(1)

        val schema = StructType("ItemID Sentiment SentimentText".split(" ")
                                .map(fieldName => {
                                    if(fieldName == "ItemID" || fieldName=="Sentiment")
                                        StructField(fieldName, IntegerType, nullable=false)
                                    else 
                                        StructField(fieldName, StringType, nullable=false)
                                }))

        val data = spark.read.format("csv").schema(schema)
                            .option("header", "true")
                            .load(inputPath)


        val removeRepetitive = udf{ str: String => str.replaceAll("((.))\\1+","$1").trim.toLowerCase()}

        val noRepetitiveData = data.withColumn("Collapsed", removeRepetitive('SentimentText))



        val tokenizer = new RegexTokenizer()
            .setInputCol("Collapsed")
            .setOutputCol("Tokens")
            .setPattern("\\s+")

        val hashingTF = new HashingTF().setInputCol("Tokens").setOutputCol("tf").setNumFeatures(2000)

        val idfModel = new IDF().setInputCol("tf").setOutputCol("tfidfFeatures")

        val lr = new LogisticRegression()
            .setFamily("multinomial")
            .setFeaturesCol("tfidfFeatures")
            .setLabelCol("Sentiment")


        // pipeline example
        val pipe = new Pipeline()
        .setStages(Array(
        tokenizer, // split original sentence on whitespaces
        hashingTF,
        idfModel,
        lr // add classification stage 
        ))


        val paramGrid = new ParamGridBuilder()
            .addGrid(lr.tol, Array(1e-10, 1e-5))
            .addGrid(lr.maxIter, Array(100, 200, 400))
            .build()

        val cv = new CrossValidator()
                    .setEstimator(pipe)
                    .setEvaluator(new BinaryClassificationEvaluator()
                    .setRawPredictionCol("prediction")
                    .setLabelCol("Sentiment"))
                    .setEstimatorParamMaps(paramGrid)
                    .setNumFolds(3)  // Use 3+ in practice
                    .setParallelism(2)
        
        val model = cv.fit(noRepetitiveData)

        sc.parallelize(model.bestModel.extractParamMap().toString).saveAsTextFile(outputPath)
        
    }
}



