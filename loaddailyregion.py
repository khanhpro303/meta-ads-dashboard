# Đây là script Python để tự động nạp dữ liệu REGION HÀNG NGÀY
# Script sẽ lặp qua từng ngày trong khoảng thời gian đã định và
# CHỈ gọi các hàm liên quan đến việc lấy 'get_all_insights_region'
# và nạp vào 'fact_performance_region'.
# Giữa mỗi lần nạp dữ liệu, script sẽ chờ một khoảng thời gian định sẵn.

import logging
import time
from datetime import date, timedelta, datetime
from database_manager import DatabaseManager
from fbads_extract import FacebookAdsExtractor 

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
END_DATE = date(2025, 11, 17)
# Thời gian chờ (giây) giữa mỗi lần nạp dữ liệu
WAIT_SECONDS = 5
# ------------------

def main():
    """
    Script chính để lặp qua các ngày và chỉ nạp dữ liệu REGION.
    """
    logging.info("--- BẮT ĐẦU SCRIPT NẠP DỮ LIỆU REGION TỰ ĐỘNG ---")
    
    try:
        db_manager = DatabaseManager()
        # Khởi tạo extractor để lấy dữ liệu
        extractor = FacebookAdsExtractor()
        
        # Đảm bảo các bảng đã tồn tại
        logging.info("Kiểm tra/Tạo bảng trong database...")
        db_manager.create_all_tables()
        logging.info("Kiểm tra/Tạo bảng hoàn tất.")
    except Exception as e:
        logging.error(f"Không thể khởi tạo DatabaseManager hoặc Extractor. Lỗi: {e}")
        return

    # Lấy danh sách tài khoản (chỉ lấy 1 lần bên ngoài vòng lặp)
    logging.info("Đang lấy danh sách tài khoản quảng cáo...")
    try:
        accounts = extractor.get_all_ad_accounts()
        # Lọc các tài khoản không mong muốn
        accounts = [acc for acc in accounts if acc['name'] != 'Nguyen Xuan Trang' and acc['name'] != 'Lâm Khải']
        logging.info(f"Đã lọc, sẽ xử lý cho {len(accounts)} tài khoản.")
    except Exception as e:
        logging.error(f"Không thể lấy danh sách tài khoản. Dừng script. Lỗi: {e}")
        return

    current_date = START_DATE
    total_days = (END_DATE - START_DATE).days + 1
    day_count = 1

    while current_date <= END_DATE:
        # Định dạng ngày thành chuỗi 'YYYY-MM-DD'
        current_date_str = current_date.strftime('%Y-%m-%d')
        
        logging.info(f"--- [Ngày {day_count}/{total_days}] Bắt đầu nạp dữ liệu REGION cho ngày: {current_date_str} ---")
        
        try:
            # === BƯỚC 1: TRÍCH XUẤT (EXTRACT) ===
            all_insights_region = []
            logging.info(f"Lấy Insights region cho ngày {current_date_str}...")
            
            for account in accounts:
                account_id = account['id']
                try:
                    insights_region = extractor.get_all_insights_region(
                        account_id=account_id,
                        start_date=current_date_str,
                        end_date=current_date_str
                    )
                    if insights_region:
                        all_insights_region.extend(insights_region)
                except Exception as e:
                    logging.error(f"Lỗi khi lấy region insight cho tài khoản {account_id} ngày {current_date_str}: {e}")
            
            if not all_insights_region:
                logging.warning(f"Không có dữ liệu insights (region) nào được tìm thấy cho ngày {current_date_str}. Bỏ qua...")
                
                # Vẫn tăng ngày và sleep
                current_date += timedelta(days=1)
                day_count += 1
                if current_date <= END_DATE:
                    logging.info(f"Đang chờ {WAIT_SECONDS} giây...")
                    time.sleep(WAIT_SECONDS)
                continue # Chuyển sang vòng lặp tiếp theo

            logging.info(f"Tổng cộng có {len(all_insights_region)} bản ghi region được lấy về.")

            # === BƯỚC 2: BIẾN ĐỔI & NẠP (TRANSFORM & LOAD) ===
            
            # 2.1 Cập nhật DimDate
            # Cần chuyển đổi current_date (kiểu date) sang datetime
            target_date_dt = datetime.combine(current_date, datetime.min.time())
            db_manager.upsert_dates(target_date_dt, target_date_dt)
            
            # 2.2 Nạp FactPerformanceRegion
            logging.info("Đang nạp dữ liệu vào fact_performance_region...")
            db_manager.upsert_performance_region_data(all_insights_region)
            
            # 2.3 Làm giàu DimRegion
            logging.info("Đang làm giàu (enrich) dữ liệu Geo cho DimRegion...")
            db_manager._enrich_region_geo_data()
            
            logging.info(f"--- [Ngày {day_count}/{total_days}] Hoàn thành nạp dữ liệu REGION cho ngày: {current_date_str} ---")

        except Exception as e:
            # Nếu một ngày bị lỗi, ghi lại lỗi và tiếp tục ngày tiếp theo
            logging.error(f"--- [Ngày {day_count}/{total_days}] Gặp lỗi khi nạp dữ liệu REGION cho ngày {current_date_str}. Lỗi: {e} ---", exc_info=True)
        
        # Tăng ngày lên
        current_date += timedelta(days=1)
        day_count += 1
        
        # Nếu chưa phải ngày cuối cùng, chờ
        if current_date <= END_DATE:
            logging.info(f"Đang chờ {WAIT_SECONDS} giây trước khi nạp ngày tiếp theo...")
            time.sleep(WAIT_SECONDS)

    logging.info("--- ĐÃ HOÀN THÀNH TOÀN BỘ QUÁ TRÌNH NẠP DỮ LIỆU REGION ---")

if __name__ == "__main__":
    main()