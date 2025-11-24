# Đây là script Python để tự động nạp dữ liệu hàng ngày (ADS + FANPAGE)
# Script sẽ lặp qua từng ngày trong khoảng thời gian đã định.
# Quy trình mỗi ngày: 
# 1. Nạp Ads Data -> 2. Sleep 1.5s -> 3. Nạp Fanpage Data -> 4. Sleep 4s -> Ngày tiếp theo.

import logging
import time
from datetime import date, timedelta
from database_manager import DatabaseManager

# Cấu hình logging cơ bản
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- CẤU HÌNH ---
# Ngày bắt đầu (bao gồm)
START_DATE = date(2025, 11, 1)
# Ngày kết thúc (bao gồm)
END_DATE = date(2025, 11, 10)

# Thời gian chờ kết thúc 1 ngày (giây)
WAIT_SECONDS_END_OF_DAY = 4
# Thời gian chờ giữa Ads task và Fanpage task (giây)
WAIT_SECONDS_BETWEEN_TASKS = 1.5
# ------------------

def main():
    """
    Script chính để lặp qua các ngày và nạp dữ liệu tổng hợp.
    """
    logging.info("--- BẮT ĐẦU SCRIPT NẠP DỮ LIỆU TỰ ĐỘNG (ADS & FANPAGE) ---")
    
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
        
        logging.info(f"========== [Ngày {day_count}/{total_days} - {current_date_str}] BẮT ĐẦU XỬ LÝ ==========")
        
        # ---------------------------------------------------------
        # BƯỚC 1: NẠP DỮ LIỆU QUẢNG CÁO (ADS)
        # ---------------------------------------------------------
        try:
            logging.info(f"-> [1/2] Đang nạp ADS DATA cho ngày: {current_date_str}...")
            db_manager.refresh_data(
                start_date=current_date_str,
                end_date=current_date_str
            )
            logging.info(f"-> [1/2] Hoàn thành nạp ADS DATA.")
        except Exception as e:
            logging.error(f"-> [1/2] LỖI nạp ADS DATA ngày {current_date_str}: {e}", exc_info=True)
        
        # ---------------------------------------------------------
        # BƯỚC 2: SLEEP 1.5 GIÂY
        # ---------------------------------------------------------
        logging.info(f"... Nghỉ {WAIT_SECONDS_BETWEEN_TASKS}s trước khi nạp Fanpage ...")
        time.sleep(WAIT_SECONDS_BETWEEN_TASKS)

        # ---------------------------------------------------------
        # BƯỚC 3: NẠP DỮ LIỆU FANPAGE
        # ---------------------------------------------------------
        try:
            logging.info(f"-> [2/2] Đang nạp FANPAGE DATA cho ngày: {current_date_str}...")
            db_manager.refresh_data_fanpage(
                start_date=current_date_str,
                end_date=current_date_str
            )
            logging.info(f"-> [2/2] Hoàn thành nạp FANPAGE DATA.")
        except Exception as e:
            logging.error(f"-> [2/2] LỖI nạp FANPAGE DATA ngày {current_date_str}: {e}", exc_info=True)

        logging.info(f"========== [Ngày {day_count}/{total_days} - {current_date_str}] HOÀN TẤT ==========")

        # Tăng ngày lên
        current_date += timedelta(days=1)
        day_count += 1
        
        # ---------------------------------------------------------
        # BƯỚC 4: SLEEP 4 GIÂY (NẾU CÒN NGÀY TIẾP THEO)
        # ---------------------------------------------------------
        if current_date <= END_DATE:
            logging.info(f"Đang chờ {WAIT_SECONDS_END_OF_DAY} giây trước khi sang ngày tiếp theo...")
            time.sleep(WAIT_SECONDS_END_OF_DAY)

    logging.info("--- ĐÃ HOÀN THÀNH TOÀN BỘ QUÁ TRÌNH NẠP DỮ LIỆU ---")

if __name__ == "__main__":
    main()