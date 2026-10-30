import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FCMPreprocessingToHDFS_LSTM {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("HatefulMemes-Text-Preprocessing-LSTM-HDFS")
      .master("spark://spark-master:7077")
      .getOrCreate()

    import spark.implicits._

    val RAW = "hdfs://namenode:8020/data/facebook_hateful_memes"
    val OUT = "hdfs://namenode:8020/data/facebook_hateful_memes/processed_text_LSTM"

    val MAX_SEQ_LENGTH = 80   // You can adjust for LSTM

    def preprocess(splitName: String): Unit = {

      val rawDF = spark.read
        .json(s"$RAW/raw/$splitName.jsonl")

      // -----------------------------
      // Handle label safely
      // -----------------------------
      val withLabelDF =
        if (rawDF.columns.contains("label")) {
          rawDF.withColumn("label", col("label").cast("int"))
        } else {
          rawDF.withColumn("label", lit(-1)) // for test split
        }

      val processedDF = withLabelDF
        // Lowercase
        .withColumn("clean_text", lower(col("text")))

        // Remove URLs
        .withColumn("clean_text", regexp_replace(col("clean_text"), "http[s]?://\\S+", ""))

        // Remove special characters
        .withColumn("clean_text", regexp_replace(col("clean_text"), "[^a-z0-9\\s]", ""))

        // Remove extra spaces
        .withColumn("clean_text", regexp_replace(col("clean_text"), "\\s+", " "))

        // Trim
        .withColumn("clean_text", trim(col("clean_text")))

        // Tokenize
        .withColumn("text_tokens", split(col("clean_text"), " "))

        // Optional: limit sequence length for LSTM
        .withColumn("text_tokens",
          expr(s"slice(text_tokens, 1, $MAX_SEQ_LENGTH)")
        )

        .select(
          col("id").cast("int"),
          col("img"),
          col("clean_text"),
          col("text_tokens"),
          col("label")
        )

      // -----------------------------
      // Save to HDFS
      // -----------------------------
      processedDF.write
        .mode("overwrite")
        .parquet(s"$OUT/$splitName")

      println(s"✅ Saved split: $splitName to processed_text_LSTM")
    }

    // -----------------------------
    // Process all splits
    // -----------------------------
    preprocess("train")
    preprocess("dev")
    preprocess("test")

    spark.stop()
  }
}