import os
import warnings
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType
import sparknlp
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import RoBertaEmbeddings
from pyspark.ml import Pipeline

# ==================== CONFIGURATION ====================
HDFS_HOST = "namenode"
HDFS_PORT = "8020"
SPARK_MASTER = "spark://spark-master:7077"

INPUT_BASE = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/data/facebook_hateful_memes/processed_text"
OUTPUT_BASE = f"hdfs://{HDFS_HOST}:{HDFS_PORT}/data/facebook_hateful_memes/features/roberta_features"

MAX_LENGTH = 128                     # max tokens per text (as in paper)
BATCH_SIZE = 32                       # inference batch size
DIMENSION = 768                       # RoBERTa base dimension
CASE_SENSITIVE = True                  # RoBERTa is cased

# ==================== SPARK SESSION WITH SPARK NLP ====================
def create_spark_session():
    """Create Spark session with Spark NLP"""
    # Start Spark NLP session with appropriate memory
    spark = sparknlp.start(gpu=False, memory="16G")  # set gpu=True if you have GPU
    
    # Configure HDFS
    spark.conf.set("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_HOST}:{HDFS_PORT}")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ==================== CREATE SPARK NLP PIPELINE ====================
def create_roberta_pipeline():
    """
    Create Spark NLP pipeline for RoBERTa embedding extraction
    """
    # Document assembler: converts text column to document annotation
    document_assembler = DocumentAssembler() \
        .setInputCol("text") \
        .setOutputCol("document")
    
    # RoBERTa embeddings: generates word embeddings
    roberta_embeddings = RoBertaEmbeddings.pretrained("roberta_base", "en") \
        .setInputCols(["document"]) \
        .setOutputCol("roberta_embeddings") \
        .setCaseSensitive(CASE_SENSITIVE) \
        .setMaxSentenceLength(MAX_LENGTH) \
        .setBatchSize(BATCH_SIZE)
    
    # Finisher: converts embeddings to usable format
    finisher = Finisher() \
        .setInputCols(["roberta_embeddings"]) \
        .setOutputCols(["roberta_embeddings_vector"]) \
        .setOutputAsVector(True) \
        .setCleanAnnotations(False)
    
    # Build pipeline
    pipeline = Pipeline(stages=[
        document_assembler,
        roberta_embeddings,
        finisher
    ])
    
    return pipeline

# ==================== PROCESS SINGLE SPLIT ====================
def process_split(split_name, spark, pipeline_model):
    """
    Process a single split (train/dev/test) through the pipeline
    and save embeddings to HDFS
    """
    print(f"\n📁 Processing {split_name}...")
    
    # Read input data
    input_path = f"{INPUT_BASE}/{split_name}"
    df = spark.read.parquet(input_path)
    
    # Apply pipeline to get embeddings
    result_df = pipeline_model.transform(df)
    
    # Select original columns + embeddings
    output_df = result_df.select(
        "id", "img", "text", "label",
        "roberta_embeddings_vector"
    )
    
    # Save to HDFS
    output_path = f"{OUTPUT_BASE}_{split_name}"
    output_df.write.mode("overwrite").parquet(output_path)
    
    # Show sample
    count = output_df.count()
    print(f"✅ Saved {count} samples to {output_path}")
    
    # Show a sample embedding
    sample = output_df.select("roberta_embeddings_vector").first()[0]
    print(f"📊 Sample embedding dimension: {len(sample)}")
    
    return count

# ==================== VERIFY SAVED FEATURES ====================
def verify_features(spark):
    """Verify saved features for all splits"""
    print("\n🔍 Verifying saved features...")
    splits = ['train', 'dev', 'test']
    
    for split in splits:
        path = f"{OUTPUT_BASE}_{split}"
        try:
            df = spark.read.parquet(path)
            count = df.count()
            if count > 0:
                sample = df.select("roberta_embeddings_vector").first()[0]
                print(f"✅ {split}: {count} samples, dim = {len(sample)}")
            else:
                print(f"⚠️ {split}: empty")
        except Exception as e:
            print(f"⚠️ {split}: could not read – {e}")

# ==================== MAIN ====================
def main():
    print("=" * 60)
    print("ROBERTA FEATURE EXTRACTION WITH SPARK NLP")
    print("=" * 60)
    
    warnings.filterwarnings('ignore')
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create and fit pipeline on a sample to get the model
        print("\n🔄 Creating and fitting RoBERTa pipeline...")
        pipeline = create_roberta_pipeline()
        
        # Get a small sample to fit the pipeline
        sample_df = spark.read.parquet(f"{INPUT_BASE}/train").limit(10)
        pipeline_model = pipeline.fit(sample_df)
        print("✅ Pipeline ready")
        
        # Process all splits
        splits = ['train', 'dev', 'test']
        total_processed = 0
        
        for split in splits:
            count = process_split(split, spark, pipeline_model)
            total_processed += count
        
        # Verify results
        verify_features(spark)
        
        print("\n" + "=" * 60)
        print(f"✅ FEATURE EXTRACTION COMPLETED: {total_processed} total images")
        print("=" * 60)
        print("\n📁 Output locations (HDFS):")
        for split in splits:
            print(f"   {OUTPUT_BASE}_{split}")
        print("\n📄 Schema:")
        print("   - id: int")
        print("   - img: string")
        print("   - text: string")
        print("   - label: int")
        print("   - roberta_embeddings_vector: array<float> (768-dim)")
        
    except Exception as e:
        print(f"\n❌ Pipeline error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🛑 Stopping Spark...")
        spark.stop()
        print("👋 Done.")

if __name__ == "__main__":
    main()