## Giới Thiệu

Dự án này được xây dựng trên các công nghệ như Apache Spark, Hive, PostgreSQL, MinIO, Trino và Jupyter. Mục đích của dự án là cung cấp môi trường đầy đủ để xử lý và phân tích dữ liệu phân tán, lưu trữ dữ liệu object và thực hiện các truy vấn SQL phức tạp. Dự án này được triển khai trong môi trường Docker, giúp dễ dàng thiết lập và quản lý các thành phần này một cách hiệu quả.

### Các Thành Phần Chính

1. **Hive Postgres**: Cung cấp cơ sở dữ liệu PostgreSQL để lưu trữ metadata cho Hive Metastore.
2. **Hive Metastore**: Dịch vụ quản lý metadata cho Hive, kết nối với PostgreSQL để lưu trữ thông tin về các bảng dữ liệu.
3. **MinIO**: Một hệ thống lưu trữ dữ liệu dưới dạng Object Storage tương tự Amazon S3, sử dụng để lưu trữ và truy cập các dữ liệu lớn.
4. **Apache Spark**: Cung cấp môi trường tính toán phân tán, bao gồm:
   - **Spark Master**: Điều phối các tác vụ Spark.
   - **Spark Worker**: Xử lý các tác vụ tính toán.
   - **Spark Thrift Server**: Cung cấp giao diện JDBC/ODBC cho các công cụ phân tích dữ liệu.
5. **Trino**: Công cụ truy vấn phân tán, cho phép truy vấn dữ liệu từ các nguồn khác nhau như Hive, MinIO.
6. **Jupyter**: Cung cấp môi trường notebook để thực thi mã Spark, phân tích dữ liệu, và làm việc trực tiếp với dữ liệu trong môi trường đồ họa.

### Cấu Trúc Thư Mục

Dưới đây là cấu trúc thư mục chi tiết của dự án:

```plaintext
.
conf/                                  # Cấu hình
│── trino/                             # Cấu hình cho Trino
│   |── hive-site.xml                  # Cấu hình Hive
│   ├── iceberg.properties             # Cấu hình Iceberg
│   └── spark-defaults.conf            # Cấu hình Spark
containers/                            # Dockerfiles cho các container
│   ├── jupyter/                       # Dockerfile cho Jupyter
│   ├── spark/                         # Dockerfile cho Spark
├── dag/                               # Chứa dự án DBT
├── notebooks/                         # Notebooks của Jupyter
│   ├── demo.ipynb                     # Mẫu notebook cho Spark
├── logs/                              # Lưu trữ log của các 
└── README.md                          # Tài liệu hướng dẫn sử dụng
```

## Cài Đặt

### Prerequisites

Trước khi bắt đầu, bạn cần đảm bảo rằng bạn đã cài đặt các công cụ sau:

- **Docker** (với phiên bản mới nhất)
- **Docker Compose** (với phiên bản mới nhất)

### Các Bước Cài Đặt

1. **Clone repository về máy**:

    Đầu tiên, bạn cần clone dự án này từ GitHub về máy của mình.

    ```bash
    git clone <repository_url>
    cd <project_directory>
    ```

2. **Khởi động các dịch vụ với Docker Compose**:

    Sau khi đã clone xong, bạn có thể khởi động toàn bộ hệ thống dịch vụ trong Docker bằng cách chạy lệnh sau:

    ```bash
    docker-compose up -d
    ```

    Lệnh này sẽ tự động tải các image Docker cần thiết cho các dịch vụ và khởi động chúng trong chế độ background.

3. **Kiểm Tra Trạng Thái Các Container**:

    Bạn có thể kiểm tra các container đang chạy với lệnh:

    ```bash
    docker-compose ps
    ```

    Các dịch vụ sẽ được khởi chạy ở các cổng sau:

    - **Jupyter**: [http://localhost:8888](http://localhost:8888)
    - **Trino**: [http://localhost:8082](http://localhost:8082)
    - **Spark UI**: [http://localhost:8080](http://localhost:8080)
    - **Hive Metastore**: [http://localhost:9083](http://localhost:9083)
    - **MinIO Console**: [http://localhost:9001](http://localhost:9001)

### Các Thành Phần Trong Docker Compose

Dưới đây là thông tin chi tiết về các dịch vụ được cấu hình trong `docker-compose.yml`:

1. **Hive Postgres**:
   - Sử dụng PostgreSQL để lưu trữ metadata của Hive Metastore.
   - Cấu hình mặc định là `POSTGRES_USER=hive`, `POSTGRES_PASSWORD=hivepw`.

2. **Hive Metastore**:
   - Quản lý metadata của Hive và kết nối với PostgreSQL.
   - Sử dụng các thư viện JAR cần thiết để tích hợp với Hadoop và AWS.

3. **MinIO**:
   - Cung cấp dịch vụ lưu trữ dữ liệu kiểu object storage (tương tự Amazon S3).
   - Sử dụng các key `minioadmin` cho `MINIO_ACCESS_KEY` và `MINIO_SECRET_KEY`.

4. **Apache Spark**:
   - **Spark Master**: Điều phối các worker nodes và xử lý các tác vụ Spark.
   - **Spark Worker**: Xử lý các tác vụ tính toán do Master gửi đến.
   - **Spark Thrift Server**: Cung cấp giao diện JDBC/ODBC cho các ứng dụng ngoài Spark.

5. **Trino**:
   - Công cụ truy vấn phân tán hỗ trợ SQL, cho phép truy vấn dữ liệu từ Hive và MinIO.

6. **Jupyter**:
   - Cung cấp giao diện notebook để thực thi mã Spark, phân tích dữ liệu, và visualizing kết quả.

### Cấu Hình DBT

DBT (Data Build Tool) được sử dụng trong thư mục `dag/` để xử lý các phép biến đổi dữ liệu. DBT giúp xây dựng pipeline dữ liệu từ các bảng gốc, áp dụng các phép biến đổi SQL, và tạo ra các bảng hoặc views mới.

Các bước sử dụng DBT:

1. **Cấu hình DBT**: DBT sẽ sử dụng cấu hình trong thư mục `dag/` để thực hiện các tác vụ biến đổi.
2. **Chạy DBT**: Bạn có thể thực hiện các phép biến đổi trong DBT qua các lệnh CLI hoặc tích hợp vào các pipeline tự động.

## Sử Dụng

### Trino

Trino là công cụ truy vấn phân tán, hỗ trợ truy vấn SQL trên nhiều nguồn dữ liệu như Hive và MinIO. Các cấu hình của Trino được đặt trong thư mục `conf/trino/` và được mount vào container Trino thông qua Docker.

Có thể sử dụng Trino để truy vấn dữ liệu từ Hive, MinIO hoặc các nguồn dữ liệu khác mà bạn kết nối vào Trino.

### Apache Spark và Jupyter

Spark được sử dụng để xử lý dữ liệu phân tán, và bạn có thể sử dụng Jupyter notebooks trong thư mục `notebooks/` để viết mã và phân tích dữ liệu trực tiếp.

### DBT

DBT được sử dụng trong thư mục `dag/` để thực hiện các tác vụ biến đổi dữ liệu. Các tác vụ có thể được cấu hình và chạy thông qua Docker container để tạo ra các views và bảng mới từ các bảng gốc.

### Lưu Trữ Dữ Liệu

MinIO lưu trữ các tệp dữ liệu lớn dưới dạng object storage, có thể truy cập và sử dụng thông qua Trino hoặc Spark.