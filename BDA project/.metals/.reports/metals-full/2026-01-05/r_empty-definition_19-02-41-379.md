error id: file:///C:/BDA%20project/src/main/scala/FCMPreprocessingWithMongo.scala:`<error>`#`<error>`.
file:///C:/BDA%20project/src/main/scala/FCMPreprocessingWithMongo.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/ImageSchema.
	 -org/apache/spark/ml/image/ImageSchema.
	 -org/apache/spark/sql/types/ImageSchema.
	 -spark/implicits/ImageSchema.
	 -ImageSchema.
	 -scala/Predef.ImageSchema.
offset: 852
uri: file:///C:/BDA%20project/src/main/scala/FCMPreprocessingWithMongo.scala
text:
```scala
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.types._

object FCMPreprocessingWithMongo {

  def main(args: Array[String]): Unit = {

    // ---------------------------------------
    // 1. Spark Session (FIXED master URL)
    // ---------------------------------------
    val spark = SparkSession.builder()
      .appName("HatefulMemes-FCM-Preprocessing-Mongo")
      .master("spark://spark-master:7077")
      .config("spark.mongodb.write.connection.uri",
              "mongodb://mongodb:27017/hateful_memes")
      .getOrCreate()

    import spark.implicits._

    // ---------------------------------------
    // 2. Load images ONCE
    // ---------------------------------------
    val imagesDF = @@ImageSchema.readImages(
      "hdfs://namenode:8020/datasets/hateful_memes/img",
      recursive = true
    )
      .withColumn(
        "image_id",
        regexp_extract(col("image.origin"), "(\\d+)\\.png", 1)
      )
      .select(
        col("image_id"),
        col("image.data").alias("image_bytes") // keep raw bytes
      )
      .cache()

    // ---------------------------------------
    // 3. Preprocess function
    // ---------------------------------------
    def preprocessSplit(jsonPath: String, mongoCollection: String): Unit = {

      val memeDF = spark.read
        .option("multiLine", false)
        .json(jsonPath)

      val textProcessed = memeDF
        .withColumn("clean_text", lower(col("text")))
        .withColumn("clean_text",
          regexp_replace(col("clean_text"), "http[s]?://\\S+", ""))
        .withColumn("clean_text",
          regexp_replace(col("clean_text"), "[^a-zA-Z0-9\\s]", ""))
        .withColumn("clean_text",
          regexp_replace(col("clean_text"), "\\s+", " "))
        .withColumn("text_tokens", split(col("clean_text"), " "))

      val withOCR = textProcessed
        .withColumn("image_text", lit(""))
        .withColumn("image_text_tokens", array())

      val joined = withOCR.join(
        broadcast(imagesDF),
        withOCR("id") === imagesDF("image_id"),
        "inner"
      )

      val finalDF = joined.select(
        col("id"),
        col("text_tokens"),          // Text LSTM
        col("image_text_tokens"),    // OCR LSTM (placeholder)
        col("image_bytes"),          // CNN input
        when(col("label").isNull, lit(-1))
          .otherwise(col("label"))
          .cast("int")
          .alias("label")
      )

      finalDF.write
        .format("mongodb")
        .mode("overwrite")
        .option(
          "spark.mongodb.write.connection.uri",
          s"mongodb://mongodb:27017/hateful_memes.$mongoCollection"
        )
        .save()

      println(s"✅ Saved collection: $mongoCollection")
      finalDF.groupBy("label").count().show()
    }

    // ---------------------------------------
    // 4. Run ALL splits
    // ---------------------------------------
    preprocessSplit(
      "hdfs://namenode:8020/datasets/hateful_memes/train.jsonl",
      "fcm_train"
    )

    preprocessSplit(
      "hdfs://namenode:8020/datasets/hateful_memes/dev.jsonl",
      "fcm_dev"
    )

    preprocessSplit(
      "hdfs://namenode:8020/datasets/hateful_memes/test.jsonl",
      "fcm_test"
    )

    spark.stop()
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 