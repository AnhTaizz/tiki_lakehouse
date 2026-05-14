🛒 Tiki Data Lakehouse Project
Dự án xây dựng hệ thống Data Lakehouse hiện đại để thu thập, xử lý và lưu trữ dữ liệu sản phẩm từ nền tảng Tiki.vn.

🌟 Mục tiêu & Lộ trình
Dự án được thực hiện nhằm hoàn thành mục tiêu "Sống sót và Tỏa sáng" trong lộ trình 2 tuần:

Tuần 1: Xây dựng hệ thống Spark Crawling (Hứng dữ liệu E-commerce từ API của Tiki) thông qua xử lý song song.

Tuần 2: Xây dựng Lakehouse với định dạng Apache Iceberg để quản lý dữ liệu hiệu quả và chống trùng lặp.

📊 Phạm vi dữ liệu (Current Scope)
Hiện tại, hệ thống tập trung thu thập dữ liệu từ ngành hàng: Làm đẹp - Sức khỏe.

Dữ liệu bao gồm thông tin chi tiết về sản phẩm như giá, lượt bán, thương hiệu và đánh giá từ khách hàng.

🎯 Những công nghệ và kỹ thuật đã áp dụng (Key Achievements)
Data Ingestion: Xây dựng script Python (spark_crawler.py) tự động gọi API Tiki để lấy danh sách sản phẩm theo danh mục và bóc tách dữ liệu thô, sau đó hiển thị dạng DataFrame.

Modern Data Stack: Tự setup toàn bộ hạ tầng bằng Docker Compose bao gồm:

Compute: PySpark (chạy trên môi trường Jupyter Notebook jupyter/pyspark-notebook).

Storage: MinIO Object Storage (Giả lập AWS S3).

Metadata Catalog: Apache Hive Metastore + PostgreSQL.

Change Data Capture (CDC): Áp dụng Apache Iceberg để thực hiện luồng CDC. Sử dụng lệnh MERGE INTO (Upsert) trong PySpark để so sánh ID sản phẩm và đối chiếu dữ liệu nguồn với dữ liệu đích:

INSERT: Khi ID hoàn toàn mới, chèn toàn bộ dòng dữ liệu đó vào bảng.

UPDATE: Khi tìm thấy sản phẩm đã tồn tại, cập nhật các trường dữ liệu thường xuyên thay đổi (giá cả, lượt bán, đánh giá). Cơ chế này giúp chống trùng lặp tuyệt đối.

Data Versioning (Time Travel): Ứng dụng tính năng Time Travel của Iceberg để truy vấn lại trạng thái dữ liệu (Snapshots) tại các thời điểm trong quá khứ dưới hạ tầng MinIO, tối ưu hóa lưu trữ.

🏗️ System Architecture

📂 Cấu trúc thư mục nổi bật
/tiki_lakehouse/docker-compose.yml: File setup hạ tầng container (Spark, Minio, Hive, DB).

/workspace/jobs/spark_crawler.py: Job cào dữ liệu và đẩy vào bảng Iceberg phân vùng theo ngày.

/workspace/notebooks/load_data_to_iceberg.ipynb: Notebook minh họa luồng xử lý và CDC bằng lệnh SQL.

/workspace/notebooks/iceberg_time_travel/: Thư mục thử nghiệm các tính năng quản lý phiên bản của Iceberg.

🚀 Cách chạy thử dự án (Quick Start)
Khởi động các services bằng lệnh:
docker-compose up -d

Mở Jupyter Notebook tại localhost:8888 để chạy các luồng xử lý dữ liệu.

Truy cập MinIO console tại localhost:9001 (user: admin / pass: password123) để xem file vật lý (Parquet/JSON).
