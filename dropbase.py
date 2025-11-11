import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Import Base từ file của bạn (ví dụ: database_manager.py)
# Đảm bảo file này import TẤT CẢ các model (DimAd, FactPerformance, v.v.)
from database_manager import Base 

# Tải biến môi trường
load_dotenv()
db_url = os.getenv('DATABASE_URL')
if db_url and db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)

if not db_url:
    print("Lỗi: Không tìm thấy DATABASE_URL.")
else:
    # Tạo engine
    engine = create_engine(db_url)

    print("!!! CẢNH BÁO !!!")
    print("Hành động này sẽ XÓA TẤT CẢ CÁC BẢNG do SQLAlchemy quản lý.")
    print("Tất cả dữ liệu sẽ bị MẤT VĨNH VIỄN.")
    
    confirm = input("Bạn có chắc chắn muốn tiếp tục? (nhập 'yes' để xác nhận): ")
    
    if confirm == 'yes':
        try:
            print("Đang kết nối và xóa bảng...")
            # Đây là lệnh chính
            Base.metadata.drop_all(bind=engine)
            print("Đã xóa tất cả các bảng thành công.")
        except Exception as e:
            print(f"Có lỗi xảy ra: {e}")
    else:
        print("Đã hủy thao tác.")