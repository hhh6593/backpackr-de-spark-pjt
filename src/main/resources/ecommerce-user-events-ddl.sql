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