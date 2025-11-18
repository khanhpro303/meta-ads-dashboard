# 📚 Hướng Dẫn Sử Dụng Meta Ads Dashboard

Chào mừng bạn đến với hệ thống quản trị dữ liệu quảng cáo và fanpage. Dưới đây là hướng dẫn chi tiết để giúp bạn khai thác tối đa hiệu quả của Dashboard.

---

## 1. 🧭 Điều Hướng & Chức Năng Các Panel

Hệ thống được chia thành 3 khu vực làm việc chính. Bạn có thể chuyển đổi qua lại bằng thanh menu bên trái.

| Panel | Icon | Chức năng chính |
| :--- | :---: | :--- |
| **Tổng quan** | 🏠 | Xem bức tranh toàn cảnh về chi tiêu, hiển thị, và hiệu quả quảng cáo (Ads) của toàn bộ tài khoản. Dữ liệu tập trung vào dòng tiền và conversion. |
| **Fanpage Overview** | 📘 | Phân tích sức khỏe của Fanpage (Organic & Paid). Theo dõi lượt like mới, tương tác bài viết, và xem Top Content hiệu quả nhất. |
| **Chiến dịch** | 📊 | Phân tích sâu (Deep-dive) vào từng chiến dịch. Bao gồm bản đồ nhiệt (Geo Map), biểu đồ nhân khẩu học (Age/Gender) và phân tích sản phẩm (Waffle Chart). |

---

## 2. 🤖 Lưu Ý Khi Sử Dụng Chatbox AI

Trợ lý ảo AI được tích hợp để giúp bạn tra cứu số liệu nhanh chóng. Tuy nhiên, AI cần ngữ cảnh rõ ràng để trả lời chính xác.

> **⚠️ Nguyên tắc vàng:** "Hỏi cụ thể - Không viết tắt - Đúng tên riêng"

### ✅ Nên làm vs ❌ Không nên làm

| ❌ Không nên (Mơ hồ) | ✅ Nên (Cụ thể) |
| :--- | :--- |
| "Doanh thu sao rồi?" | "Tổng doanh thu (Purchase Value) của tài khoản trong **tháng 10** là bao nhiêu?" |
| "Cái mũ E-22 bán tốt ko?" | "So sánh hiệu quả của chiến dịch **EGO_Oct_E-22** và **LS2_Oct_FF800** về số lượt mua." |
| "Fanpage dạo này thế nào?" | "Số lượng **New Likes** của fanpage **LS2 Helmets Vietnam** trong **7 ngày qua** là bao nhiêu?" |

---

## 3. 🏷️ Quy Định Đặt Tên (Naming Convention) - QUAN TRỌNG 🔴

Để hệ thống báo cáo tự động (đặc biệt là **Biểu đồ Waffle**) hoạt động chính xác, việc đặt tên chiến dịch, nhóm quảng cáo và quảng cáo phải tuân thủ nghiêm ngặt quy tắc phân cách bằng dấu gạch dưới `_`.

### A. Tên Chiến Dịch (Campaign Name)
Cấu trúc bắt buộc:
`Brand`_`Thời gian`_`Sản phẩm/Mục tiêu`_`Ghi chú (Tùy chọn)`

**Quy tắc trích xuất dữ liệu:**
Hệ thống sẽ tự động lấy **yếu tố thứ 3** để làm nhãn (Label) cho biểu đồ phân tích sản phẩm.

* **Ví dụ chuẩn (4 yếu tố):** `LS2_Dec 2025_FF800_PromoTet`
    * ➔ Hệ thống lấy: **FF800** (Làm nhãn báo cáo)
* **Ví dụ chuẩn (3 yếu tố):** `EGO_Q4 2025_E-22`
    * ➔ Hệ thống lấy: **E-22** (Làm nhãn báo cáo)

> **❌ Sai:** `LS2 Dec 2025 FF800` (Dùng dấu cách thay vì `_` sẽ làm hệ thống không đọc được tên sản phẩm).

### B. Tên Nhóm Quảng Cáo (Adset Name)
Cấu trúc khuyến nghị: `Khu vực`_`Độ tuổi`
* Ví dụ: `VN_22+`
* Ví dụ: `HCM_18-35`

### C. Tên Quảng Cáo (Ad Name)
Cấu trúc khuyến nghị: `Tên SP`_`Định dạng`_`ID Bài/Content`
* Ví dụ: `FF800_Video_ContentA`
* Ví dụ: `E22_Anh_ContentB`

---

## 4. 🔄 Hướng Dẫn Làm Mới Dữ Liệu (Refresh Data)

Hệ thống được thiết kế để tối ưu hóa tốc độ bằng cách lưu trữ dữ liệu vào Database riêng. Do đó, dữ liệu **không tự động cập nhật theo thời gian thực (Real-time)** từ Facebook.

### 🕒 Quy định Refresh
1.  **Tần suất:** Bạn cần thực hiện thao tác "Làm mới" (Refresh) **MỖI NGÀY** vào đầu buổi làm việc.
2.  **Lý do:** Hệ thống không lưu cache dữ liệu quá khứ quá lâu để đảm bảo tính chính xác. Việc refresh giúp đồng bộ dữ liệu mới nhất từ hôm qua về hệ thống.

### 🛠️ Cách thực hiện
1.  Chọn khoảng thời gian cần cập nhật (Ví dụ: "Hôm qua" hoặc "Tùy chỉnh" chọn ngày cụ thể).
2.  Nhấn nút **`Làm mới`** (nút màu xám có icon 🔄) ở góc phải bộ lọc.
3.  Đợi hệ thống báo "Thành công" rồi nhấn **`Áp dụng`** để xem số liệu mới.

> **Lưu ý:** Hạn chế chọn khoảng thời gian quá dài (ví dụ: 30 ngày) khi nhấn nút "Làm mới" để tránh quá tải hệ thống. Hãy refresh từng ngày hoặc khoảng ngắn 1-2 ngày.

---

*BBI Marketing Dashboard - Version 1.0*