package com.github.hans;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions;
import org.apache.spark.util.CollectionAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Usage: Main <FilePath>");
            System.exit(1);
        }

        final String inputFilePath = args[0];

        SparkSession spark = SparkSession.builder()
                .appName("Backpackr DE PJT Spark Application")
                .master("spark://spark-master:7077")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.access.key", "minio")
                .config("spark.hadoop.fs.s3a.secret.key", "minio123")
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .getOrCreate();

        // 예외를 수집하기 위한 Accumulator 생성
        CollectionAccumulator<String> exceptionAccumulator = spark.sparkContext().collectionAccumulator("Exceptions");

        try {
            logger.info("Processing {}", inputFilePath);

            // 스키마 정의 (필요에 따라 스키마를 조정하세요)
            StructType schema = new StructType(new StructField[]{
                    new StructField("event_time", DataTypes.TimestampType, true, Metadata.empty()),
                    new StructField("event_type", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("product_id", DataTypes.IntegerType, true, Metadata.empty()),
                    new StructField("category_id", DataTypes.DoubleType, true, Metadata.empty()),
                    new StructField("category_code", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("brand", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
                    new StructField("user_id", DataTypes.IntegerType, true, Metadata.empty()),
                    new StructField("user_session", DataTypes.StringType, true, Metadata.empty())
            });

            // 데이터 로드
            Dataset<Row> rawData = spark.read()
                    .option("header", "true")
                    .schema(schema)
                    .csv(inputFilePath);

            // event_time을 KST 기준의 etl_date로 변환하고 예외 처리
            Dataset<Row> processedData = rawData.withColumn("etl_date",
                    functions.to_date(
                            functions.from_utc_timestamp(
                                    rawData.col("event_time"), "Asia/Seoul"), "yyyy-MM-dd"));

            // 처리 실패 혹은 데이터가 존재하지 않아 기존 데이터를 삭제하는 것을 방지하기 위해 count 체크
            if (processedData.count() > 0) {
                processedData.write()
                        .partitionBy("etl_date")
                        .mode(SaveMode.Overwrite)
                        .option("compression", "snappy")
                        .parquet("s3a://warehouse/ecommerce-user-events/"); // 데이터 저장 임시 경로
            } else {
                throw new Exception("No data to save.");
            }

        } catch (Exception e) {
            exceptionAccumulator.add("Attempt failed: " + e.getMessage());
        }

        if (!exceptionAccumulator.isZero()) {
            logger.info("Exceptions occurred during processing:");

            List<String> exceptions = exceptionAccumulator.value();
            Dataset<Row> exceptionDF = spark.createDataset(exceptions, Encoders.STRING()).toDF("exception");

            Path path = Paths.get(inputFilePath);
            String fileName = path.getFileName().toString();

            String exceptionFileName = fileName + ".exceptions.parquet";
            String outputPath = "s3a://warehouse/ecommerce-user-events-exceptions/" + exceptionFileName; // 에러 로그 임시 경로

            exceptionDF.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(outputPath);
        }

        spark.stop();
    }
}


