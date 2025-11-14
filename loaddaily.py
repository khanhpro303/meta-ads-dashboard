import logging
import time
from datetime import date, timedelta
from database_manager import DatabaseManager

# Cấu hình logging cơ bản
# Đảm bảo bạn có thể thấy output trong console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- CẤU HÌNH ---
# Ngày bắt đầu (bao gồm)
START_DATE = date(2025, 9, 18)
# Ngày kết thúc (bao gồm)
END_DATE = date(2025, 11, 15)
# Thời gian chờ (giây) giữa mỗi lần nạp dữ liệu
WAIT_SECONDS = 10
# ------------------

def main():
    """
    Script chính để lặp qua các ngày và nạp dữ liệu.
    """
    logging.info("--- BẮT ĐẦU SCRIPT NẠP DỮ LIỆU TỰ ĐỘNG ---")
    
    try:
        db_manager = DatabaseManager()
        # Đảm bảo các bảng đã tồn tại
        logging.info("Kiểm tra/Tạo bảng trong database...")
        db_manager.create_all_tables()
        logging.info("Kiểm tra/Tạo bảng hoàn tất.")
    except Exception as e:
        logging.error(f"Không thể khởi tạo DatabaseManager hoặc tạo bảng. Lỗi: {e}")
        return

    current_date = START_DATE
    total_days = (END_DATE - START_DATE).days + 1
    day_count = 1

    while current_date <= END_DATE:
        # Định dạng ngày thành chuỗi 'YYYY-MM-DD'
        current_date_str = current_date.strftime('%Y-%m-%d')
        
        logging.info(f"--- [Ngày {day_count}/{total_days}] Bắt đầu nạp dữ liệu cho ngày: {current_date_str} ---")
        
        try:
            # Gọi hàm refresh_data cho CHỈ MỘT NGÀY
            # Bằng cách đặt start_date và end_date giống nhau
            db_manager.refresh_data(
                start_date=current_date_str,
                end_date=current_date_str
            )
            logging.info(f"--- [Ngày {day_count}/{total_days}] Hoàn thành nạp dữ liệu cho ngày: {current_date_str} ---")

        except Exception as e:
            # Nếu một ngày bị lỗi, ghi lại lỗi và tiếp tục ngày tiếp theo
            logging.error(f"--- [Ngày {day_count}/{total_days}] Gặp lỗi khi nạp dữ liệu cho ngày {current_date_str}. Lỗi: {e} ---", exc_info=True)
        
        # Tăng ngày lên
        current_date += timedelta(days=1)
        day_count += 1
        
        # Nếu chưa phải ngày cuối cùng, chờ
        if current_date <= END_DATE:
            logging.info(f"Đang chờ {WAIT_SECONDS} giây trước khi nạp ngày tiếp theo...")
            time.sleep(WAIT_SECONDS)

    logging.info("--- ĐÃ HOÀN THÀNH TOÀN BỘ QUÁ TRÌNH NẠP DỮ LIỆU ---")

if __name__ == "__main__":
    main()