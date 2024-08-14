# backpackr-de-spark-pjt
### 백패커 데이터 엔지니어 과제

-----
docker-compose는 Hive 테이블 조회 테스트로 활용하였습니다.  
빌드 후 다른 스파크 클러스터에서 사용하실 수 있습니다.

| Index          | description | required | default      | example                                                             |
|:---------------|:------------|:---------|:-------------|:--------------------------------------------------------------------|
| 0              | CSV 파일 경로   | yes      | -            | s3a://warehouse/ecommerce-user-events-raw/2019-Oct-sample.csv       |
| 1              | 파일 저장 경로    | yes      | -            | s3a://warehouse/ecommerce-user-events/                                                             |


Usage
-----

Build

    mvn clean package

Docker-compose

    docker-compose up -d

Jar copy to master node

    docker cp path/target/backpackr-de-spark-pjt-1.0.jar \
    spark-master:/opt/bitnami/spark/jars/backpackr-de-spark-pjt-1.0.jar

Spark submit
    
    # 1. Minio (Docker Compose 활용 시 예제)
    docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --class com.github.hans.Main \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    /opt/bitnami/spark/jars/backpackr-de-spark-pjt-1.0.jar \
    s3a://warehouse/ecommerce-user-events-raw/2019-Oct-sample.csv \ 
    s3a://warehouse/ecommerce-user-events/

    # 2. S3
    docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --class com.github.hans.Main \
    --master <SPARK MASTER NODE>:<PORT> \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.access.key=<AWS_ACCESS_KEY> \
    --conf spark.hadoop.fs.s3a.secret.key=<AWS_SECRET_KEY> \
    --conf spark.hadoop.fs.s3a.endpoint=<s3.{REGION}.amazonaws.com> \
    <PATH>/backpackr-de-spark-pjt-1.0.jar \
    <INPUT FILE PATH> \ 
    <OUTPUT FILE PATH>
    
    # 3. 로컬 파일시스템
    docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    --class com.github.hans.Main \
    --master <SPARK MASTER NODE>:<PORT> \
    /opt/bitnami/spark/jars/backpackr-de-spark-pjt-1.0.jar \
    file://<PATH>/2019-Oct-sample.csv \
    file://<PATH>/ecommerce-user-events/

데이터 조회(Docker-Compos 활용 시)
-----
테이블 생성(Trino Engine) 및 파티션 업데이트

    CREATE TABLE minio."default".ecommerce_user_events (
        event_time TIMESTAMP,
        event_type VARCHAR,
        product_id INT,
        category_id DOUBLE,
        category_code VARCHAR,
        brand VARCHAR,
        price DOUBLE,
        user_id	INT,
        user_session VARCHAR,
        etl_date DATE
    )
    WITH (
        external_location = 's3a://warehouse/ecommerce-user-events',
        format = 'PARQUET',
        partitioned_by = ARRAY['etl_date']
    );

테이블 조회
    
    CALL minio.system.sync_partition_metadata('default', 'ecommerce_user_events', 'FULL');

    SELECT
        *
    FROM minio."default".ecommerce_user_events 
    LIMIT 100;
    

    