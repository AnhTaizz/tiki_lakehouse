# Hướng Dẫn Sử Dụng Apache Superset (Tiki Data Lakehouse)

Tài liệu này hướng dẫn cách kết nối và trực quan hóa dữ liệu (Visualize) từ Data Lakehouse lên **Apache Superset**. 

Data flow: `Silver Iceberg` → `Spark Gold Job` → `Reporting Postgres (port 5432)` → `Superset (port 8088)`.

---

## 1. Đăng nhập Superset
Sau khi chạy `docker compose up -d`, truy cập vào trình duyệt:
- **URL**: [http://localhost:8088](http://localhost:8088)
- **Username mặc định**: `admin`
- **Password mặc định**: `password123` 
*(Nếu bạn đã thay đổi trong file `.env` ở biến `SUPERSET_ADMIN_PASSWORD`, hãy dùng mật khẩu đó).*

---

## 2. Kết Nối Database (Data Source)
Bước đầu tiên là khai báo cho Superset biết nơi chứa dữ liệu Gold.
1. Ở góc trên cùng bên phải, chọn **Settings** → **Database Connections**.
2. Bấm nút **+ Database**.
3. Chọn **PostgreSQL** và điền thông tin sau:
   - **Host:** `reporting-postgres` *(đây là tên container chứa dữ liệu Gold)*
   - **Port:** `5432`
   - **Database Name:** `reporting`
   - **Username:** `reporting`
   - **Password:** `reporting123`
   - **Display Name:** Tùy chọn (Ví dụ: *Tiki Gold Data*)
4. Bấm **Test Connection**. Nếu hiện thông báo xanh lá là thành công → Bấm **Connect** và **Finish**.

---

## 3. Khai báo Bảng (Tạo Dataset)
Superset chỉ lấy lên những bảng mà bạn cho phép.
1. Ở Menu trên cùng, chọn **Datasets**.
2. Bấm nút **+ Dataset**.
3. Khai báo lần lượt:
   - Database: `Tiki Gold Data` (Vừa tạo ở bước 2).
   - Schema: `public`
   - Table: Chọn bảng bạn muốn vẽ (vd: `brand_performance`, `top_products`, ...).
4. Bấm **Create Dataset and Create Chart**.

---

## 4. Xây Dựng Biểu Đồ (Charts)
Trong giao diện Explore (Tạo biểu đồ), hãy làm theo các bước sau để có biểu đồ đẹp nhất:

### Chọn Trục & Số Liệu
- **X-axis (Trục ngang):** Kéo thả hoặc chọn cột mô tả (Ví dụ: `brand_name`). *Bắt buộc phải có đối với các biểu đồ Echarts*.
- **Metrics (Giá trị đo lường):** Chọn phép tính. Ví dụ: `SUM(total_quantity_sold)` hoặc `AVG(avg_rating)`.

### Các Mẹo Chỉnh Sửa Quan Trọng (Troubleshooting)

| Vấn Đề Thường Gặp | Cách Xử Lý |
| :--- | :--- |
| Nút **"Create chart" bị mờ**, báo lỗi *"Add required control values..."* | Do bạn chưa điền ô **X-axis**. Hãy kéo cột hiển thị tên (vd: `brand_name`) vào ô X-axis. Không để ở ô Dimensions. |
| Biểu đồ lộn xộn, các cột cao thấp không đều | Do Superset mặc định xếp theo thứ tự ABC. Cuộn xuống ô **X-Axis Sort By**, chọn metric của bạn (vd: `SUM(...)`) và bật **Sort Descending** (Sắp xếp giảm dần). |
| Có quá nhiều cột, chữ bị đè lên nhau | Chỉnh thông số ở ô **Row limit** (hoặc Series limit) thành `10` hoặc `20` để chỉ lấy Top 10/Top 20. Nếu muốn dễ đọc hơn, đổi loại biểu đồ sang **Bar Chart (Horizontal)**. |
| Tên cột tiếng Anh khó đọc (vd: `SUM(total_quantity_sold)`) | Click thẳng vào ô Metrics đó → Trong cửa sổ nhỏ hiện ra, click vào **biểu tượng cây bút chì ✏️** cạnh tên ở trên cùng → Gõ tên tiếng Việt (vd: *Tổng số lượng bán*) → Bấm Save. |

---

## 5. Đưa Biểu Đồ Vào Dashboard
1. Sau khi chỉnh sửa biểu đồ ưng ý, bấm nút **Update Chart** (để xem trước).
2. Bấm **Save** ở góc trên cùng bên phải.
3. Đặt tên Chart (vd: *Top 20 Brand Bán Chạy Nhất*).
4. Ở mục "Add to dashboard", chọn **Add to new dashboard** và đặt tên cho Dashboard (vd: *Tiki Overview*).
5. Bấm **Save & Go to Dashboard**.

Lặp lại quy trình từ **Bước 3** cho các bảng Gold khác, chọn "Add to existing dashboard" để gom tất cả biểu đồ vào một bảng điều khiển duy nhất dùng để báo cáo cho Mentor.
