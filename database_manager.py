import os
import logging
from typing import Dict, List, Any
from datetime import datetime
from sqlalchemy import create_engine, Column, String, DateTime, MetaData, Table, ForeignKey, func, Float, BigInteger, Integer, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pytz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy Base (Lớp cơ sở cho các model)
Base = declarative_base()

# --- ĐỊNH NGHĨA STAR SCHEMA ---

class DimAdAccount(Base):
    """
    Bảng Dimension (chiều): Lưu trữ thông tin mô tả về Tài khoản Quảng cáo.
    Đây là bảng bạn yêu cầu.
    """
    __tablename__ = 'dim_ad_account'
    
    # Dùng 'act_12345' làm Primary Key (Khóa chính)
    ad_account_id = Column(String, primary_key=True) 
    name = Column(String)
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

class DimCampaign(Base):
    """
    Bảng Dimension: Lưu trữ thông tin mô tả về Chiến dịch.
    """
    __tablename__ = 'dim_campaign'
    
    campaign_id = Column(String, primary_key=True)
    name = Column(String)
    objective = Column(String)
    status = Column(String)
    created_time = Column(DateTime)
    start_time = Column(DateTime)
    # Thêm Foreign Key (Khóa ngoại) liên kết đến Bảng Dimension AdAccount
    ad_account_id = Column(String, ForeignKey('dim_ad_account.ad_account_id'))
    updated_at = Column(DateTime, default=datetime.now(), onupdate=datetime.now())

class DimDate(Base):
    """
    Bảng Dimension: Lưu trữ thông tin về ngày tháng.
    Giúp phân tích theo ngày, tháng, quý, năm dễ dàng.
    """
    __tablename__ = 'dim_date'
    
    date_key = Column(Integer, primary_key=True) # Ví dụ: 20251108
    full_date = Column(Date)
    day = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    quarter = Column(Integer)

class FactPerformance(Base):
    """
    Bảng Fact (sự thật): Lưu trữ các chỉ số (metrics) hiệu suất.
    Liên kết với tất cả các bảng Dimension.
    """
    __tablename__ = 'fact_performance'
    
    # Khóa chính của bảng Fact
    performance_id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # --- Khóa ngoại (Foreign Keys) ---
    date_key = Column(Integer, ForeignKey('dim_date.date_key'))
    ad_account_id = Column(String, ForeignKey('dim_ad_account.ad_account_id'))
    campaign_id = Column(String, ForeignKey('dim_campaign.campaign_id'))
    
    # --- Các chỉ số (Measures/Facts) ---
    spend = Column(Float)
    impressions = Column(BigInteger)
    clicks = Column(BigInteger)
    conversions = Column(BigInteger) # Bạn có thể thêm nhiều chỉ số khác

# --- CLASS QUẢN LÝ DATABASE ---

class DatabaseManager:
    
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL')
        if not self.db_url:
            raise ValueError("Biến môi trường DATABASE_URL chưa được thiết lập.")
        
        # Sửa lỗi của Heroku: Heroku cung cấp 'postgres://'
        # SQLAlchemy yêu cầu 'postgresql://'
        if self.db_url.startswith("postgres://"):
            self.db_url = self.db_url.replace("postgres://", "postgresql://", 1)
            
        self.engine = create_engine(self.db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        logger.info("Đã kết nối đến Database.")

    def create_all_tables(self):
        """
        Tạo tất cả các bảng (Dim, Fact) nếu chúng chưa tồn tại.
        """
        try:
            Base.metadata.create_all(bind=self.engine, checkfirst=True)
            logger.info("Tất cả các bảng trong Star Schema đã được kiểm tra/tạo.")
        except Exception as e:
            logger.error(f"Lỗi khi tạo bảng: {e}")

    def upsert_ad_accounts(self, accounts_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' (Insert hoặc Update nếu đã tồn tại)
        cho bảng dim_ad_account.
        """
        if not accounts_data:
            logger.warning("Không có dữ liệu Ad Accounts để upsert.")
            return

        # Chuẩn bị dữ liệu (đổi tên key cho khớp với cột)
        prepared_data = [
            {'ad_account_id': acc['id'], 'name': acc['name']} 
            for acc in accounts_data
        ]

        # Câu lệnh INSERT... ON CONFLICT DO UPDATE (tính năng của Postgres)
        stmt = pg_insert(DimAdAccount).values(prepared_data)
        
        # Nếu `ad_account_id` bị trùng (conflict), thì UPDATE cột `name`
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['ad_account_id'],
            set_={
                'name': stmt.excluded.name,
                'updated_at': datetime.now()
            }
        )
        
        session = self.SessionLocal()
        try:
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} tài khoản vào dim_ad_account.")
        except Exception as e:
            logger.error(f"Lỗi khi Upsert Ad Accounts: {e}")
            session.rollback()
        finally:
            session.close()
            
    # Bạn có thể thêm các hàm upsert khác ở đây
    # def upsert_campaigns(self, campaigns_data: List[Dict[str, Any]]):
    #     ...
        
    # def upsert_performance(self, performance_data: List[Dict[str, Any]]):
    #     ...