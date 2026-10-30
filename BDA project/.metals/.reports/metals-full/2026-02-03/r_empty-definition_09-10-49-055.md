error id: file:///C:/BDA%20project/src/main/scala/imgsplit.scala:
file:///C:/BDA%20project/src/main/scala/imgsplit.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -fs/exists.
	 -fs/exists#
	 -fs/exists().
	 -scala/Predef.fs.exists.
	 -scala/Predef.fs.exists#
	 -scala/Predef.fs.exists().
offset: 2122
uri: file:///C:/BDA%20project/src/main/scala/imgsplit.scala
text:
```scala
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.FileUtil
import java.net.URI

object ImgSplit {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FacebookHatefulMemes-ImageSplit")
      .getOrCreate()

    // 🔴 IMPORTANT: Explicit HDFS URI
    val hdfsUri = "hdfs://namenode:8020"
    val fs = FileSystem.get(new URI(hdfsUri), spark.sparkContext.hadoopConfiguration)

    // --------------------------------------------------
    // TRAIN / DEV (labeled)
    // --------------------------------------------------
    def prepareLabeled(split: String): Unit = {

      val df = spark.read.json(
        s"$hdfsUri/data/facebook_hateful_memes/raw/$split.jsonl"
      )

      df.collect().foreach { row =>

        val imgPath = row.getAs[String]("img")   // img/42953.png
        val label = row.getAs[Long]("label")

        val cls = if (label == 1) "hateful" else "non_hateful"

        val src = new Path(s"$hdfsUri/data/facebook_hateful_memes/$imgPath")
        val dstDir = new Path(s"$hdfsUri/data/facebook_hateful_memes/image_$split/$cls")
        val dst = new Path(dstDir, imgPath.split("/").last)

        if (!fs.exists(dstDir)) fs.mkdirs(dstDir)

        if (!fs.exists(dst)) {
          FileUtil.copy(fs, src, fs, dst, false, fs.getConf)
        }
      }

      println(s"✅ Finished preparing $split data")
    }

    // --------------------------------------------------
    // TEST (NO labels)
    // --------------------------------------------------
    def prepareTest(): Unit = {

      val df = spark.read.json(
        s"$hdfsUri/data/facebook_hateful_memes/raw/test.jsonl"
      )

      df.collect().foreach { row =>

        val imgPath = row.getAs[String]("img")

        val src = new Path(s"$hdfsUri/data/facebook_hateful_memes/$imgPath")
        val dstDir = new Path(s"$hdfsUri/data/facebook_hateful_memes/image_test/unknown")
        val dst = new Path(dstDir, imgPath.split("/").last)

        if (!fs.exists@@(dstDir)) fs.mkdirs(dstDir)

        if (!fs.exists(dst)) {
          FileUtil.copy(fs, src, fs, dst, false, fs.getConf)
        }
      }

      println("✅ Finished preparing test data")
    }

    // --------------------------------------------------
    // RUN ALL
    // --------------------------------------------------
    prepareLabeled("train")
    prepareLabeled("dev")
    prepareTest()

    spark.stop()
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 