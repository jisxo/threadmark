# Usage: make run
run:
	docker exec -it spark_app /opt/spark/bin/spark-submit \
	  --master "local[*]" \
	  --driver-memory 2g \
	  --executor-memory 2g \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
	  /opt/spark-apps/processor.py

# --- MinIO 버킷 초기화 ---
init-minio:
	@echo "Waiting 5 seconds for MinIO server to start..."
	@sleep 5
	@echo "Setting up MinIO access..."
	@docker run --rm --network container:minio_datalake minio/mc alias set myminio http://localhost:9000 admin password
	@echo "Creating 'silver-layer' bucket..."
	@docker run --rm --network container:minio_datalake minio/mc mb myminio/silver-layer || echo "Bucket already exists."
	@echo "MinIO initialization completed."