# Nexar_exam

## Chuẩn bị

### Chuẩn bị hệ thống
Hệ thống chạy dựa trên các container của Docker. Thực hiện tải Docker về hệ thống nếu hệ thống chưa có Docker Engine hoặc Docker Desktop

### Chuẩn bị image

1. Custom image Airflow
```bash
  #build image
  sudo docker build -t airflow_custom -f DockerFile .
```

2. Custom image cung cấp api để tải file ndjson
```bash
  #di chuyển vào thư mục Fake_api
  cd Fake_api
  #build image
  sudo docker build -t fake_api -f Dockerfile .
```
### Chuẩn bị môi trường cho Airflow
1. Thực hiện thêm key của google cloud vào folder dags
2. Tạo connection liên kết tới google cloud
3. Thay các biến connection_id, bucket_name, project_id, location, dataset và table_name để phù hợp với trường hợp của bản thân

## Hướng dẫn manual, auto

### Deploy hệ thống
```bash
  ### Lưu ý: Di chuyển tới thư mục Nexar_Exam trước khi thực hiện chạy các lệnh sau

  # Khởi động hệ thống với lệnh
  sudo docker compose up -d

  # Để tắt hệ thống, sử dụng lệnh
  sudo docker compose down
```

### Chú ý:
- Hệ thống sử dụng các câu lệnh SQL để thực hiện hai tác vụ là tạo bảng và load dữ liệu từ gcs vào bảng trong Bigquery.

-> Vì thế, các câu lệnh được lập trình để thực hiện các tác vụ phục vụ cho url cung cấp sẵn của hệ thống

-> Nếu muốn download file ndjson khác, thực hiện thay đổi biến url trong file dags/download_task.py. Cùng với đó thay đổi các câu lệnh SQL tạo bảng và load dữ liệu trong folder dags/sql để pipeline có thể thực hiện toàn bộ chức năng của nó

