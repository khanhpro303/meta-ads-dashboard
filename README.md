# 📈 Meta Ads Intelligence Dashboard & AI Assistant

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![Meta Graph API](https://img.shields.io/badge/Meta_Graph_API-v24.0-blue)
![Gemini](https://img.shields.io/badge/Google%20Gemini-1.5_Flash-8E75B2?style=flat&logo=googlegemini&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-success)

## 📖 Giới thiệu (Overview)

**Meta Ads Intelligence** là một giải pháp phân tích dữ liệu quảng cáo tập trung, được thiết kế bởi một Marketing Planner, giúp Marketing Team và Business Owner theo dõi hiệu quả chiến dịch theo thời gian thực.

Điểm đặc biệt của dự án là việc tích hợp **Chatbot thông minh (AI Analyst)**. Thay vì phải tự lọc hàng trăm dòng dữ liệu để tìm insight, người dùng có thể đặt câu hỏi bằng ngôn ngữ tự nhiên (ví dụ: *"Tại sao CPR hôm qua lại tăng cao?"*) và nhận được câu trả lời dựa trên phân tích dữ liệu thực tế.

## 🚀 Tính năng chính (Key Features)

### 1. Interactive Dashboard (Bảng điều khiển trực quan)
* **Real-time Tracking:** Chỉ cần một nút bấm! Tự động đồng bộ dữ liệu từ Meta Ads (Facebook/Instagram) thông qua API.
* **KPI Visualization:** Biểu đồ hóa các chỉ số quan trọng: Chi tiêu (Spend), Doanh thu (Revenue), ROAS, CTR, CPM...
* **Custom Filters:** Lọc dữ liệu theo Tài khoản, Chiến dịch (Campaign), Nhóm quảng cáo (Adset), hoặc khoảng thời gian tùy chỉnh.

### 2. AI Chatbot Assistant (Trợ lý ảo chuyên dụng)
* **Natural Language Query:** Cho phép hỏi đáp về dữ liệu quảng cáo bằng tiếng Việt tự nhiên.
* **Instant Insights:** Chatbot tự động tính toán và so sánh hiệu suất (VD: So sánh doanh số tuần này với tuần trước).
* **Recommendations:** Đưa ra gợi ý tối ưu (tắt ads, tăng ngân sách) dựa trên rule được lập trình sẵn hoặc logic của AI.
* **Vision Analyse:** Có khả năng phân tích hình ảnh để hỗ trợ định hướng về mặt media production.

## 🛠 Công nghệ sử dụng (Tech Stack)

Dự án được xây dựng dựa trên hệ sinh thái Python, tập trung vào khả năng xử lý dữ liệu lớn và tính năng Real-time.

### 🔙 Backend & Core
* ![Flask](https://img.shields.io/badge/Flask-2.0+-000000?style=flat&logo=flask&logoColor=white): Framework chính để xây dựng Web Server và API.
* **Flask-Login:** Quản lý xác thực (Authentication) và phiên làm việc (Session management) bảo mật.
* **Threading (Concurrency):** Xử lý bất đồng bộ (Asynchronous) cho các tác vụ ETL nặng (Refresh dữ liệu Ads/Fanpage) mà không làm treo giao diện người dùng.
* **Server-Sent Events (SSE):** Kỹ thuật Streaming response giúp Chatbot trả lời từng từ (token) theo thời gian thực, tối ưu trải nghiệm UX như ChatGPT.

### 📊 Data Processing & Database
* ![Pandas](https://img.shields.io/badge/Pandas-Data_Analysis-150458?style=flat&logo=pandas&logoColor=white): Làm sạch, tổng hợp và biến đổi dữ liệu (Data Manipulation).
* ![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM-red?style=flat): Tương tác với cơ sở dữ liệu.
    * *Kiến trúc:* **Star Schema** (Mô hình Kim cương) với các bảng Dimension (Chiến dịch, Adset) và Fact (Hiệu suất, Demographic) để tối ưu truy vấn báo cáo.
* **SQLite/PostgreSQL:** Hệ quản trị cơ sở dữ liệu quan hệ (RDBMS).

### 📈 Visualization & Reporting
* **Folium:** Vẽ bản đồ nhiệt (Heatmap/Cluster Map) tương tác để phân tích hiệu suất quảng cáo theo vùng miền (Geo-spatial analysis).
* **Matplotlib (Agg Backend) & PyWaffle:** Render các biểu đồ tĩnh phức tạp (như Waffle Chart) từ phía server và chuyển đổi sang Base64 để hiển thị.
* **Chart.js (JSON API):** Backend cung cấp API cấu trúc chuẩn JSON để Frontend vẽ biểu đồ tương tác.

### 🤖 AI & 3rd Party Integrations
* **Meta Graph API (v19.0+):** Kết nối trực tiếp với Facebook Ads Manager để lấy dữ liệu Real-time (Spend, Impressions, CPR, Reach...).
* **OpenAI API (GPT-4 Integration):** Xử lý ngôn ngữ tự nhiên (NLP) cho tính năng "AI Analyst", cho phép hỏi đáp về dữ liệu (Text-to-SQL/Data Analysis).

### ⚙️ DevOps & Deployment
* **Heroku Ready:** Cấu hình tương thích với `gunicorn` và biến môi trường (`python-dotenv`).
* **Logging System:** Hệ thống log chi tiết để theo dõi lỗi và trạng thái của các tiến trình chạy ngầm (Workers).

## 📸 Demo

*(Dán link ảnh chụp màn hình Dashboard hoặc GIF demo Chatbot đang trả lời tại đây)*

## 💡 Bối cảnh & Giải pháp (Problem & Solution)

**Vấn đề:** Việc đăng nhập vào Ads Manager hàng ngày để làm báo cáo rất tốn thời gian. Các chỉ số thường rời rạc, khó nhìn thấy bức tranh tổng quan và nguyên nhân tăng/giảm giá thầu.

**Giải pháp:**
1.  Tự động hóa luồng dữ liệu (ETL Pipeline).
2.  Tạo Dashboard tập trung để nhìn nhanh sức khỏe tài khoản.
3.  Sử dụng AI để đóng vai trò như một Data Analyst, trả lời nhanh các câu hỏi khó mà không cần thao tác Excel phức tạp.

## ⚙️ Hướng dẫn cài đặt (Installation)

1.  Clone repository:
    ```bash
    git clone [https://github.com/username/meta-ads-dashboard.git](https://github.com/username/meta-ads-dashboard.git)
    cd meta-ads-dashboard
    ```

2.  Cài đặt thư viện:
    ```bash
    pip install -r requirements.txt
    ```

3.  Cấu hình biến môi trường (`.env`):
    ```
    META_ACCESS_TOKEN=your_token_here
    GEMINI_API_KEY=your_key_here
    ```

4.  Chạy ứng dụng:
    ```bash
    flask run
    ```

---
**Contact:** Mr. Khải Đoàn - kdoan4820@gmail.com
