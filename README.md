# Big Data Official Project

## 📌 Giới thiệu
Dự án được triển khai và vận hành thông qua **Docker**.  
Toàn bộ quá trình build & run được tự động hóa bằng script `deploy.sh`.

---

## ⚙️ Yêu cầu hệ thống

Trước khi chạy chương trình, đảm bảo máy bạn đã cài đặt:

- **Docker** (bắt buộc)
- **Docker Compose** (đi kèm Docker Desktop)
- Một trong hai môi trường:
  - **Git Bash** (Windows)
  - **WSL** (Windows Subsystem for Linux)
  - Hoặc Linux / macOS terminal

👉 Kiểm tra Docker đã cài chưa:
```bash
docker --version
docker compose version
```
Di chuyển vào thư mục dự án
```bash
  cd big_data_offical
```
Khởi động hệ thống
```bash
./deploy.sh start

