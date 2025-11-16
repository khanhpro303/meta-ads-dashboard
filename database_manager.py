import os
import logging
from typing import Dict, List, Any, Set, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import json
import requests
from sqlalchemy import UniqueConstraint, create_engine, Column, String, DateTime, MetaData, Table, ForeignKey, func, Float, BigInteger, Integer, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pytz

# Tải biến môi trường từ file .env
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy Base (Lớp cơ sở cho các model)
Base = declarative_base()

def parse_datetime_flexible(date_string: str) -> Optional[datetime]:
    """
    Thử phân tích một chuỗi ngày tháng với nhiều định dạng khác nhau.
    """
    if not date_string:
        return None
    
    # Danh sách các định dạng cần thử, từ phức tạp đến đơn giản
    formats_to_try = [
        '%Y-%m-%dT%H:%M:%S%z',  # Định dạng đầy đủ với timezone (e.g., 2023-10-26T10:30:00+0700)
        '%Y-%m-%d',            # Định dạng chỉ có ngày (e.g., 2023-10-26)
    ]
    
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue # Thử định dạng tiếp theo nếu thất bại
            
    logger.warning(f"Không thể phân tích chuỗi ngày tháng: '{date_string}' với các định dạng đã biết.")
    return None

# --- ĐỊNH NGHĨA STAR SCHEMA ---

class DimAdAccount(Base):
    """
    Bảng Dimension (chiều): Lưu trữ thông tin mô tả về Tài khoản Quảng cáo.
    """
    __tablename__ = 'dim_ad_account'
    
    # Dùng 'act_12345' làm Primary Key (Khóa chính)
    ad_account_id = Column(String, primary_key=True) 
    name = Column(String)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

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
    stop_time = Column(DateTime)
    # Thêm Foreign Key (Khóa ngoại) liên kết đến Bảng Dimension AdAccount
    ad_account_id = Column(String, ForeignKey('dim_ad_account.ad_account_id'))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class DimAdset(Base):
    """
    Bảng Dimension: Lưu trữ thông tin mô tả về Nhóm Quảng cáo.
    """
    __tablename__ = 'dim_adset'
    
    adset_id = Column(String, primary_key=True)
    name = Column(String)
    status = Column(String)
    created_time = Column(DateTime)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    # Thêm Foreign Key (Khóa ngoại) liên kết đến Bảng Dimension Campaign
    campaign_id = Column(String, ForeignKey('dim_campaign.campaign_id'))
    # Thêm Foreign Key (Khóa ngoại) liên kết đến Bảng Dimension AdAccount thông qua Campaign
    ad_account_id = Column(String, ForeignKey('dim_ad_account.ad_account_id'))

class DimAd(Base):
    """
    Bảng Dimension: Lưu trữ thông tin mô tả về Quảng cáo.
    """
    __tablename__ = 'dim_ad'
    
    ad_id = Column(String, primary_key=True)
    name = Column(String)
    status = Column(String)
    created_time = Column(DateTime)
    ad_schedule_start_time = Column(DateTime)
    ad_schedule_end_time = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    # Thêm Foreign Key (Khóa ngoại) liên kết đến Bảng Dimension Adset và Campaign
    campaign_id = Column(String, ForeignKey('dim_campaign.campaign_id'))
    adset_id = Column(String, ForeignKey('dim_adset.adset_id'))
    # Thêm Foreign Key (Khóa ngoại) liên kết đến Bảng Dimension AdAccount thông qua Campaign
    ad_account_id = Column(String, ForeignKey('dim_ad_account.ad_account_id'))

class DimPlatform(Base):
    """Bảng Dimension: Nền tảng (Facebook, Instagram, etc.)."""
    __tablename__ = 'dim_platform'
    platform_id = Column(Integer, primary_key=True, autoincrement=True)
    platform_name = Column(String, unique=True, nullable=False)

class DimPlacement(Base):
    """Bảng Dimension: Vị trí hiển thị (feed, stories, etc.)."""
    __tablename__ = 'dim_placement'
    placement_id = Column(Integer, primary_key=True, autoincrement=True)
    placement_name = Column(String, unique=True, nullable=False)

class DimDate(Base):
    """
    Bảng Dimension: Lưu trữ thông tin về ngày tháng.
    Giúp phân tích theo ngày, tháng, quý, năm dễ dàng.
    """
    __tablename__ = 'dim_date'
    
    date_key = Column(Integer, primary_key=True) # Ví dụ: 20251108
    full_date = Column(Date, unique=True)
    day = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    quarter = Column(Integer)

class FactPerformancePlatform(Base):
    """
    Bảng Fact: Lưu trữ các chỉ số hiệu suất chi tiết theo từng breakdown theo nền tảng và vị trí quảng cáo.
    """
    __tablename__ = 'fact_performance_platform'
    
    # Khóa chính tự tăng
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey('dim_date.date_key'), nullable=False)
    campaign_id = Column(String, ForeignKey('dim_campaign.campaign_id'), nullable=False)
    adset_id = Column(String, ForeignKey('dim_adset.adset_id'), nullable=False)
    ad_id = Column(String, ForeignKey('dim_ad.ad_id'), nullable=False)
    platform_id = Column(Integer, ForeignKey('dim_platform.platform_id'))
    placement_id = Column(Integer, ForeignKey('dim_placement.placement_id'))
    
    spend = Column(Float, default=0.0)
    impressions = Column(BigInteger, default=0)
    clicks = Column(BigInteger, default=0)
    ctr = Column(Float, default=0.0)
    cpm = Column(Float, default=0.0)
    reach = Column(BigInteger, default=0)
    frequency = Column(Float, default=0.0)
    
    messages_started = Column(Integer, default=0)
    purchases = Column(Integer, default=0)
    purchase_value = Column(Float, default=0.0)
    post_engagement = Column(Integer, default=0)
    link_click = Column(Integer, default=0)
    
    __table_args__ = (UniqueConstraint('date_key', 'ad_id', 'platform_id', 'placement_id', name='_ad_performance_platform_uc'),)

class FactPerformanceDemographic(Base):
    """
    Bảng Fact: Lưu trữ các chỉ số hiệu suất chi tiết theo từng breakdown theo nhân khẩu học.
    """
    __tablename__ = 'fact_performance_demographic'
    
    # Khóa chính tự tăng
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey('dim_date.date_key'), nullable=False)
    campaign_id = Column(String, ForeignKey('dim_campaign.campaign_id'), nullable=False)
    adset_id = Column(String, ForeignKey('dim_adset.adset_id'), nullable=False)
    ad_id = Column(String, ForeignKey('dim_ad.ad_id'), nullable=False)
    gender = Column(String)
    age = Column(String)
    
    spend = Column(Float, default=0.0)
    impressions = Column(BigInteger, default=0)
    clicks = Column(BigInteger, default=0)
    ctr = Column(Float, default=0.0)
    cpm = Column(Float, default=0.0)
    reach = Column(BigInteger, default=0)
    frequency = Column(Float, default=0.0)
    
    messages_started = Column(Integer, default=0)
    purchases = Column(Integer, default=0)
    purchase_value = Column(Float, default=0.0)
    post_engagement = Column(Integer, default=0)
    link_click = Column(Integer, default=0)

    __table_args__ = (UniqueConstraint('date_key', 'ad_id', 'gender', 'age', name='_ad_performance_demographic_uc'),)

class DimFanpage(Base):
    """
    Bảng Dimension: Lưu trữ thông tin mô tả về Fanpage.
    """
    __tablename__ = 'dim_fanpage'
    
    page_id = Column(String, primary_key=True)
    name = Column(String)
    # Lưu Page Access Token để có thể refresh dữ liệu cho page này
    page_access_token = Column(String, nullable=False)
    category = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class FactPageMetricsDaily(Base):
    """
    Bảng Fact: Lưu trữ các chỉ số TỔNG HỢP của Page, chia theo ngày.
    """
    __tablename__ = 'fact_page_metrics_daily'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey('dim_date.date_key'), nullable=False)
    page_id = Column(String, ForeignKey('dim_fanpage.page_id'), nullable=False)
    
    # Metrics từ hàm get_page_metrics_by_day
    page_fans = Column(BigInteger, default=0) # Chỉ số lifetime lượt like của page (accumulative theo ngày)
    page_impressions = Column(BigInteger, default=0)
    page_post_engagements = Column(BigInteger, default=0)
    page_video_views = Column(BigInteger, default=0)
    page_impressions_unique = Column(BigInteger, default=0)
    page_fan_removes = Column(BigInteger, default=0)
    page_fan_adds_unique = Column(BigInteger, default=0) # Người like mới unique
    
    # Thêm Unique Constraint để thực hiện ON CONFLICT
    __table_args__ = (UniqueConstraint('date_key', 'page_id', name='_page_metrics_daily_uc'),)

class FactPostPerformance(Base):
    """
    Bảng Fact: Lưu trữ các chỉ số LIFETIME của từng Post.
    """
    __tablename__ = 'fact_post_performance'
    
    # Dùng post_id làm Primary Key
    post_id = Column(String, primary_key=True)
    page_id = Column(String, ForeignKey('dim_fanpage.page_id'), nullable=False)
    
    # Thông tin mô tả của Post
    created_time = Column(DateTime, nullable=False) # Đây là "dấu ngày" bạn cần
    post_type = Column(String, nullable=True)
    message = Column(String, nullable=True)
    full_picture_url = Column(String, nullable=True)
    
    # Các chỉ số Lifetime (LT)
    shares_count = Column(Integer, default=0)
    comments_total_count = Column(Integer, default=0)
    lt_post_reactions_like_total = Column(BigInteger, default=0)
    lt_post_impressions = Column(BigInteger, default=0)
    lt_post_clicks = Column(BigInteger, default=0)
    
    # (Bạn có thể thêm các cột metric khác ở đây nếu cần)
    
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

# --- CLASS QUẢN LÝ DATABASE ---

class DatabaseManager:
    
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL')
        if not self.db_url:
            logger.error("DATABASE_URL không được cấu hình trong biến môi trường.")
            return
        
        if self.db_url.startswith("postgres://"):
            self.db_url = self.db_url.replace("postgres://", "postgresql://", 1)
            
        self.engine = create_engine(self.db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        logger.info("Đã khởi tạo DatabaseManager.")
        self.base_url = os.getenv('BASE_URL', 'https://graph.facebook.com/v24.0')

    def create_all_tables(self):
        """
        Tạo tất cả các bảng (Dim, Fact) nếu chúng chưa tồn tại.
        """
        try:
            Base.metadata.create_all(bind=self.engine, checkfirst=True)
            logger.info("Tất cả các bảng trong Star Schema đã được kiểm tra/tạo.")
        except Exception as e:
            logger.error(f"Lỗi khi tạo bảng: {e}")

    # === Hàm tiện ích để điền dữ liệu cho DimDate ===
    def upsert_dates(self, start_date: datetime, end_date: datetime):
        """
        Tạo và upsert các bản ghi ngày tháng vào bảng dim_date
        cho một khoảng thời gian nhất định.
        """
        session = self.SessionLocal()
        try:
            dates_to_insert = []
            current_date = start_date
            while current_date <= end_date:
                dates_to_insert.append({
                    'date_key': int(current_date.strftime('%Y%m%d')),
                    'full_date': current_date.date(),
                    'day': current_date.day,
                    'month': current_date.month,
                    'year': current_date.year,
                    'quarter': (current_date.month - 1) // 3 + 1
                })
                current_date += timedelta(days=1)

            if not dates_to_insert:
                return

            stmt = pg_insert(DimDate).values(dates_to_insert)
            # Nếu ngày đã tồn tại, không làm gì cả
            on_conflict_stmt = stmt.on_conflict_do_nothing(index_elements=['full_date'])
            
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã upsert {len(dates_to_insert)} ngày vào dim_date.")
        except Exception as e:
            logger.error(f"Lỗi khi upsert dates: {e}")
            session.rollback()
        finally:
            session.close()

    def upsert_ad_accounts(self, accounts_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng dim_ad_account.
        """
        if not accounts_data:
            return

        prepared_data = [
            {'ad_account_id': acc['id'], 'name': acc['name']} 
            for acc in accounts_data
        ]

        stmt = pg_insert(DimAdAccount).values(prepared_data)
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
    
    def upsert_campaigns(self, campaigns_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng dim_campaign.
        """
        if not campaigns_data:
            return

        prepared_data = []
        for camp in campaigns_data:
            prepared_data.append({
                'campaign_id': camp['id'],
                'name': camp.get('name'),
                'objective': camp.get('objective'),
                'status': camp.get('status'),
                'created_time': parse_datetime_flexible(camp.get('created_time')),
                'start_time': parse_datetime_flexible(camp.get('start_time')),
                'stop_time': parse_datetime_flexible(camp.get('stop_time')),
                'ad_account_id': camp['account_id']
            })

        stmt = pg_insert(DimCampaign).values(prepared_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['campaign_id'],
            set_={
                'name': stmt.excluded.name,
                'objective': stmt.excluded.objective,
                'status': stmt.excluded.status,
                'created_time': stmt.excluded.created_time,
                'start_time': stmt.excluded.start_time,
                'stop_time': stmt.excluded.stop_time,
                'ad_account_id': stmt.excluded.ad_account_id,
                'updated_at': datetime.now()
            }
        )
        
        session = self.SessionLocal()
        try:
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} chiến dịch vào dim_campaign.")
        except Exception as e:
            logger.error(f"Lỗi khi Upsert Campaigns: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_adsets(self, adsets_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng dim_adset.
        """
        if not adsets_data:
            return

        prepared_data = []
        for adset in adsets_data:
            prepared_data.append({
                'adset_id': adset['id'],
                'name': adset.get('name'),
                'status': adset.get('status'),
                'created_time': parse_datetime_flexible(adset.get('created_time')),
                'start_time': parse_datetime_flexible(adset.get('start_time')),
                'end_time': parse_datetime_flexible(adset.get('end_time')),
                # === BỔ SUNG KHÓA NGOẠI ===
                'campaign_id': adset.get('campaign_id'),
                'ad_account_id': adset.get('account_id')
            })

        stmt = pg_insert(DimAdset).values(prepared_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['adset_id'],
            set_={
                'name': stmt.excluded.name,
                'status': stmt.excluded.status,
                'created_time': stmt.excluded.created_time,
                'start_time': stmt.excluded.start_time,
                'end_time': stmt.excluded.end_time,
                # === BỔ SUNG CẬP NHẬT KHÓA NGOẠI ===
                'campaign_id': stmt.excluded.campaign_id,
                'ad_account_id': stmt.excluded.ad_account_id,
                'updated_at': datetime.now()
            }
        )
        
        session = self.SessionLocal()
        try:
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} nhóm quảng cáo vào dim_adset.")
        except Exception as e:
            logger.error(f"Lỗi khi Upsert Adsets: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_ads(self, ads_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng dim_ad.
        """
        if not ads_data:
            return

        prepared_data = []
        for ad in ads_data:
            prepared_data.append({
                'ad_id': ad['id'],
                'name': ad.get('name'),
                'status': ad.get('status'),
                'created_time': parse_datetime_flexible(ad.get('created_time')),
                'ad_schedule_start_time': parse_datetime_flexible(ad.get('ad_schedule_start_time')),
                'ad_schedule_end_time': parse_datetime_flexible(ad.get('ad_schedule_end_time')),
                # === BỔ SUNG CÁC KHÓA NGOẠI ===
                'adset_id': ad.get('adset_id'),
                'campaign_id': ad.get('campaign_id'),
                'ad_account_id': ad.get('account_id')
            })

        stmt = pg_insert(DimAd).values(prepared_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['ad_id'],
            set_={
                'name': stmt.excluded.name,
                'status': stmt.excluded.status,
                'created_time': stmt.excluded.created_time,
                'ad_schedule_start_time': stmt.excluded.ad_schedule_start_time,
                'ad_schedule_end_time': stmt.excluded.ad_schedule_end_time,
                # === BỔ SUNG CẬP NHẬT CÁC KHÓA NGOẠI ===
                'adset_id': stmt.excluded.adset_id,
                'campaign_id': stmt.excluded.campaign_id,
                'ad_account_id': stmt.excluded.ad_account_id,
                'updated_at': datetime.now()
            }
        )
        
        session = self.SessionLocal()
        try:
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} quảng cáo vào dim_ad.")
        except Exception as e:
            logger.error(f"Lỗi khi Upsert Ads: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def _get_or_create_dim_records(self, session, model, column_name: str, values: Set[str], pk_name: str) -> Dict[str, int]:
        """
        Hàm helper chung để lấy hoặc tạo các bản ghi trong bảng dimension.
        Trả về một dictionary map từ tên -> id.
        """
        if not values:
            return {}
        
        # 1. Lấy các bản ghi đã tồn tại
        existing_records = session.query(model).filter(getattr(model, column_name).in_(values)).all()
        mapping = {getattr(r, column_name): getattr(r, pk_name) for r in existing_records}
        
        # 2. Xác định các giá trị mới cần insert
        new_values_set = values - set(mapping.keys())
        
        # 3. Insert các giá trị mới nếu có
        if new_values_set:
            new_records_data = [{column_name: val} for val in new_values_set]
            
            # Insert các bản ghi mới (không cần return_defaults)
            session.bulk_insert_mappings(model, new_records_data)
            session.commit()
            
            # Sau khi commit, query lại chính các bản ghi đó để lấy ID
            logger.info(f"Đã tạo {len(new_records_data)} bản ghi. Đang lấy lại ID...")
            inserted_records = session.query(model).filter(getattr(model, column_name).in_(new_values_set)).all()
            
            # Cập nhật lại mapping với các bản ghi vừa tạo
            for record in inserted_records:
                mapping[getattr(record, column_name)] = getattr(record, pk_name)
            
            logger.info(f"Đã tạo và map {len(inserted_records)} bản ghi mới trong bảng {model.__tablename__}.")
            
        return mapping

    def upsert_performance_platform_data(self, insights_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng fact_performance.
        Sử dụng phương pháp "Pre-loading Dimensions" để tối ưu hiệu suất.
        """
        if not insights_data:
            logger.info("Không có dữ liệu performance platform để upsert.")
            return

        session = self.SessionLocal()
        try:
            # --- BƯỚC 1: Tải trước các Dimension vào bộ nhớ ---
            
            # Lấy tất cả các giá trị duy nhất từ dữ liệu thô
            all_dates = {rec['date_start'] for rec in insights_data if 'date_start' in rec}
            all_platforms = {rec['publisher_platform'] for rec in insights_data if 'publisher_platform' in rec}
            all_placements = {rec['platform_position'] for rec in insights_data if 'platform_position' in rec}

            # Lấy mapping từ DB hoặc tạo mới nếu chưa có
            date_map = {str(r.full_date): r.date_key for r in session.query(DimDate.full_date, DimDate.date_key).filter(DimDate.full_date.in_([datetime.fromisoformat(d).date() for d in all_dates]))}
            platform_map = self._get_or_create_dim_records(session, DimPlatform, 'platform_name', all_platforms, 'platform_id')
            placement_map = self._get_or_create_dim_records(session, DimPlacement, 'placement_name', all_placements, 'placement_id')

            # --- BƯỚC 2: Chuẩn bị dữ liệu để load vào Fact Table ---
            prepared_data = []
            for record in insights_data:
                # Bỏ qua các bản ghi không có các ID chính
                if not all([record.get('date_start'), record.get('ad_id'), record.get('adset_id'), record.get('campaign_id')]):
                    continue

                # Bóc tách các giá trị từ 'actions' và 'action_values'
                messages_started = 0
                purchases = 0
                purchase_value = 0.0
                post_engagement = 0
                link_click = 0

                if 'actions' in record:
                    for action in record.get('actions', []):
                        action_type = action.get('action_type')
                        value = int(action.get('value', 0))
                        if action_type == 'onsite_conversion.messaging_conversation_started_7d':
                            messages_started = value
                        elif action_type == 'onsite_conversion.purchase':
                            purchases = value
                        elif action_type == 'post_engagement':
                            post_engagement = value
                        elif action_type == 'link_click':
                            link_click = value
                
                if 'action_values' in record:
                    for action in record.get('action_values', []):
                        if action.get('action_type') == 'onsite_conversion.purchase':
                            purchase_value = float(action.get('value', 0.0))

                # Map dữ liệu thô với các khóa ngoại đã tra cứu
                fact_record = {
                    'date_key': date_map.get(record['date_start']),
                    'campaign_id': record['campaign_id'],
                    'adset_id': record['adset_id'],
                    'ad_id': record['ad_id'],
                    'platform_id': platform_map.get(record.get('publisher_platform')),
                    'placement_id': placement_map.get(record.get('platform_position')),
                    'spend': float(record.get('spend', 0.0)),
                    'impressions': int(record.get('impressions', 0)),
                    'clicks': int(record.get('clicks', 0)),
                    'ctr': float(record.get('ctr', 0.0)),
                    'cpm': float(record.get('cpm', 0.0)),
                    'reach': int(record.get('reach', 0)),
                    'frequency': float(record.get('frequency', 0.0)),
                    'messages_started': messages_started,
                    'purchases': purchases,
                    'purchase_value': purchase_value,
                    'post_engagement': post_engagement,
                    'link_click': link_click
                }
                # Chỉ thêm vào danh sách nếu có date_key hợp lệ
                if fact_record['date_key']:
                    prepared_data.append(fact_record)

            if not prepared_data:
                logger.warning("Không có dữ liệu hợp lệ để chèn vào fact_performance_platform sau khi chuẩn bị.")
                return

            # --- BƯỚC 3: Load hàng loạt vào Fact Table ---
            stmt = pg_insert(FactPerformancePlatform).values(prepared_data)
            on_conflict_stmt = stmt.on_conflict_do_update(
                constraint='_ad_performance_platform_uc', # Sử dụng Unique Constraint đã định nghĩa
                set_={
                    'spend': stmt.excluded.spend,
                    'impressions': stmt.excluded.impressions,
                    'clicks': stmt.excluded.clicks,
                    'ctr': stmt.excluded.ctr,
                    'cpm': stmt.excluded.cpm,
                    'reach': stmt.excluded.reach,
                    'frequency': stmt.excluded.frequency,
                    'messages_started': stmt.excluded.messages_started,
                    'purchases': stmt.excluded.purchases,
                    'purchase_value': stmt.excluded.purchase_value,
                    'post_engagement': stmt.excluded.post_engagement,
                    'link_click': stmt.excluded.link_click
                }
            )
            
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} bản ghi vào fact_performance_platform.")

        except Exception as e:
            logger.error(f"Lỗi khi Upsert Performance Data: {e}", exc_info=True)
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_performance_demographic_data(self, insights_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng fact_performance_demographic.
        Sử dụng phương pháp "Pre-loading Dimensions" để tối ưu hiệu suất.
        """
        if not insights_data:
            logger.info("Không có dữ liệu performance demographic để upsert.")
            return

        session = self.SessionLocal()
        try:
            # --- BƯỚC 1: Tải trước các Dimension vào bộ nhớ ---
            
            # Lấy tất cả các giá trị duy nhất từ dữ liệu thô
            all_dates = {rec['date_start'] for rec in insights_data if 'date_start' in rec}

            # Lấy mapping từ DB hoặc tạo mới nếu chưa có
            date_map = {str(r.full_date): r.date_key for r in session.query(DimDate.full_date, DimDate.date_key).filter(DimDate.full_date.in_([datetime.fromisoformat(d).date() for d in all_dates]))}

            # --- BƯỚC 2: Chuẩn bị dữ liệu để load vào Fact Table ---
            prepared_data = []
            for record in insights_data:
                # Bỏ qua các bản ghi không có các ID chính
                if not all([record.get('date_start'), record.get('ad_id'), record.get('adset_id'), record.get('campaign_id')]):
                    continue

                # Bóc tách các giá trị từ 'actions' và 'action_values'
                messages_started = 0
                purchases = 0
                purchase_value = 0.0
                post_engagement = 0
                link_click = 0

                if 'actions' in record:
                    for action in record.get('actions', []):
                        action_type = action.get('action_type')
                        value = int(action.get('value', 0))
                        if action_type == 'onsite_conversion.messaging_conversation_started_7d':
                            messages_started = value
                        elif action_type == 'onsite_conversion.purchase':
                            purchases = value
                        elif action_type == 'post_engagement':
                            post_engagement = value
                        elif action_type == 'link_click':
                            link_click = value
                
                if 'action_values' in record:
                    for action in record.get('action_values', []):
                        if action.get('action_type') == 'onsite_conversion.purchase':
                            purchase_value = float(action.get('value', 0.0))

                # Map dữ liệu thô với các khóa ngoại đã tra cứu
                fact_record = {
                    'date_key': date_map.get(record['date_start']),
                    'campaign_id': record['campaign_id'],
                    'adset_id': record['adset_id'],
                    'ad_id': record['ad_id'],
                    'gender': record.get('gender', 'Unknown'),
                    'age': record.get('age', 'Unknown'),
                    'spend': float(record.get('spend', 0.0)),
                    'impressions': int(record.get('impressions', 0)),
                    'clicks': int(record.get('clicks', 0)),
                    'ctr': float(record.get('ctr', 0.0)),
                    'cpm': float(record.get('cpm', 0.0)),
                    'reach': int(record.get('reach', 0)),
                    'frequency': float(record.get('frequency', 0.0)),
                    'messages_started': messages_started,
                    'purchases': purchases,
                    'purchase_value': purchase_value,
                    'post_engagement': post_engagement,
                    'link_click': link_click
                }
                # Chỉ thêm vào danh sách nếu có date_key hợp lệ
                if fact_record['date_key']:
                    prepared_data.append(fact_record)

            if not prepared_data:
                logger.warning("Không có dữ liệu hợp lệ để chèn vào fact_performance_demographic sau khi chuẩn bị.")
                return

            # --- BƯỚC 3: Load hàng loạt vào Fact Table ---
            stmt = pg_insert(FactPerformanceDemographic).values(prepared_data)
            on_conflict_stmt = stmt.on_conflict_do_update(
                constraint='_ad_performance_demographic_uc', # Sử dụng Unique Constraint đã định nghĩa
                set_={
                    'spend': stmt.excluded.spend,
                    'impressions': stmt.excluded.impressions,
                    'clicks': stmt.excluded.clicks,
                    'ctr': stmt.excluded.ctr,
                    'cpm': stmt.excluded.cpm,
                    'reach': stmt.excluded.reach,
                    'frequency': stmt.excluded.frequency,
                    'messages_started': stmt.excluded.messages_started,
                    'purchases': stmt.excluded.purchases,
                    'purchase_value': stmt.excluded.purchase_value,
                    'post_engagement': stmt.excluded.post_engagement,
                    'link_click': stmt.excluded.link_click
                }
            )
            
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} bản ghi vào fact_performance_demographic.")

        except Exception as e:
            logger.error(f"Lỗi khi Upsert Performance Data: {e}", exc_info=True)
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_fanpages(self, fanpages_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng dim_fanpage.
        """
        if not fanpages_data:
            return

        prepared_data = [
            {
                'page_id': page['id'], 
                'name': page['name'],
                'page_access_token': page['access_token'], # Rất quan trọng
                'category': page.get('category')
            } 
            for page in fanpages_data if 'access_token' in page # Chỉ lưu page có token
        ]

        if not prepared_data:
            logger.warning("Không có dữ liệu fanpage (với access token) để upsert.")
            return

        stmt = pg_insert(DimFanpage).values(prepared_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['page_id'],
            set_={
                'name': stmt.excluded.name,
                'page_access_token': stmt.excluded.page_access_token,
                'category': stmt.excluded.category,
                'updated_at': datetime.now()
            }
        )
        
        session = self.SessionLocal()
        try:
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} Fanpage vào dim_fanpage.")
        except Exception as e:
            logger.error(f"Lỗi khi Upsert Fanpages: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_page_metrics_daily(self, metrics_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng fact_page_metrics_daily.
        Dữ liệu đầu vào đã được "pivot" (nhóm theo ngày).
        """
        if not metrics_data:
            logger.info("Không có dữ liệu page metrics daily để upsert.")
            return

        session = self.SessionLocal()
        try:
            # 1. Tải trước DimDate
            all_dates_str = {rec['date'] for rec in metrics_data if 'date' in rec}
            if not all_dates_str:
                logger.warning("Dữ liệu page metrics không có trường 'date'.")
                return
                
            all_dates_obj = {datetime.strptime(d, '%Y-%m-%d').date() for d in all_dates_str}
            date_map = {str(r.full_date): r.date_key for r in session.query(DimDate.full_date, DimDate.date_key).filter(DimDate.full_date.in_(all_dates_obj))}

            # 2. Chuẩn bị dữ liệu
            prepared_data = []
            for record in metrics_data:
                date_key = date_map.get(record.get('date'))
                if not date_key:
                    continue # Bỏ qua nếu ngày không có trong DimDate

                prepared_data.append({
                    'date_key': date_key,
                    'page_id': record.get('page_id'),
                    'page_fans': record.get('page_fans', 0),
                    'page_impressions': record.get('page_impressions', 0),
                    'page_post_engagements': record.get('page_post_engagements', 0),
                    'page_video_views': record.get('page_video_views', 0),
                    'page_impressions_unique': record.get('page_impressions_unique', 0),
                    'page_fan_removes': record.get('page_fan_removes', 0),
                    'page_fan_adds_unique': record.get('page_fan_adds_unique', 0)
                })

            if not prepared_data:
                logger.warning("Không có dữ liệu page metrics hợp lệ sau khi map date_key.")
                return

            # 3. Load hàng loạt
            stmt = pg_insert(FactPageMetricsDaily).values(prepared_data)
            on_conflict_stmt = stmt.on_conflict_do_update(
                constraint='_page_metrics_daily_uc', # Unique constraint
                set_={
                    'page_fans': stmt.excluded.page_fans,
                    'page_impressions': stmt.excluded.page_impressions,
                    'page_post_engagements': stmt.excluded.page_post_engagements,
                    'page_video_views': stmt.excluded.page_video_views,
                    'page_impressions_unique': stmt.excluded.page_impressions_unique,
                    'page_fan_removes': stmt.excluded.page_fan_removes,
                    'page_fan_adds_unique': stmt.excluded.page_fan_adds_unique,
                }
            )
            
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} bản ghi vào fact_page_metrics_daily.")

        except Exception as e:
            logger.error(f"Lỗi khi Upsert Page Metrics Daily: {e}", exc_info=True)
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_post_performance(self, posts_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng fact_post_performance (dữ liệu lifetime).
        Dữ liệu đầu vào đã được "flat" (mỗi post 1 dòng).
        """
        if not posts_data:
            logger.info("Không có dữ liệu post performance để upsert.")
            return

        prepared_data = []
        for post in posts_data:
            # Phân tích cú pháp created_time
            created_time_dt = parse_datetime_flexible(post.get('created_time'))
            if not created_time_dt:
                logger.warning(f"Bỏ qua post {post.get('post_id')} do không có created_time hợp lệ.")
                continue

            prepared_data.append({
                'post_id': post.get('post_id'),
                'page_id': post.get('page_id'), # Phải được thêm vào ở hàm refresh
                'created_time': created_time_dt,
                'post_type': post.get('properties'),
                'message': post.get('message'),
                'full_picture_url': post.get('full_picture_url'),
                'shares_count': post.get('shares_count', 0),
                'comments_total_count': post.get('comments_total_count', 0),
                'lt_post_reactions_like_total': post.get('post_reactions_like_total', 0),
                'lt_post_impressions': post.get('post_impressions', 0),
                'lt_post_clicks': post.get('post_clicks', 0),
            })

        if not prepared_data:
            logger.warning("Không có dữ liệu post performance hợp lệ sau khi chuẩn bị.")
            return

        stmt = pg_insert(FactPostPerformance).values(prepared_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['post_id'],
            set_={
                'page_id': stmt.excluded.page_id,
                'created_time': stmt.excluded.created_time,
                'post_type': stmt.excluded.post_type,
                'message': stmt.excluded.message,
                'full_picture_url': stmt.excluded.full_picture_url,
                'shares_count': stmt.excluded.shares_count,
                'comments_total_count': stmt.excluded.comments_total_count,
                'lt_post_reactions_like_total': stmt.excluded.lt_post_reactions_like_total,
                'lt_post_impressions': stmt.excluded.lt_post_impressions,
                'lt_post_clicks': stmt.excluded.lt_post_clicks,
                'updated_at': datetime.now()
            }
        )
        
        session = self.SessionLocal()
        try:
            session.execute(on_conflict_stmt)
            session.commit()
            logger.info(f"Đã Upsert thành công {len(prepared_data)} bản ghi vào fact_post_performance.")
        except Exception as e:
            logger.error(f"Lỗi khi Upsert Post Performance: {e}")
            session.rollback()
            raise
        finally:
            session.close()
        
    
    def refresh_data(self, start_date: str = None, end_date: str = None, date_preset: str = None):
        """
        Hàm chính để điều phối toàn bộ quy trình ETL:
        1. Lấy dữ liệu mới từ Meta Ads API.
        2. Cập nhật các bảng Dimension.
        3. Cập nhật bảng Fact.
        """
        from fbads_extract import FacebookAdsExtractor
        extractor = FacebookAdsExtractor()

        try:
            # --- BƯỚC 1: LẤY VÀ CẬP NHẬT CÁC BẢNG DIMENSION CƠ BẢN ---
            logger.info("Bước 1: Lấy và cập nhật danh sách tài khoản quảng cáo...")
            accounts = extractor.get_all_ad_accounts()
            # Không lấy ad acc tên trong danh sách sau: Nguyen Xuan Trang, Lâm Khải
            accounts = [acc for acc in accounts if acc['name'] != 'Nguyen Xuan Trang' and acc['name'] != 'Lâm Khải']
            logger.info("=> Hoàn thành cập nhật tài khoản loại bỏ 'Nguyen Xuan Trang' và 'Lâm Khải'.")
            self.upsert_ad_accounts(accounts)

            all_campaigns = []
            all_adsets = []
            all_ads = []
            all_insights_platform = []
            all_insights_demographic = []

            for account in accounts:
                account_id = account['id']
                logger.info(f"--- Đang xử lý cho tài khoản: {account['name']} ({account_id}) ---")

                campaigns = extractor.get_campaigns_for_account(account_id=account_id, start_date=start_date, end_date=end_date, date_preset=date_preset)
                if campaigns:
                    for c in campaigns:
                    # Ghi đè account_id ('1465010674743789')
                    # bằng account_id ('act_1465010674743789')
                        c['account_id'] = account_id
                    all_campaigns.extend(campaigns)
                    campaign_ids = [c['id'] for c in campaigns]

                    adsets = extractor.get_adsets_for_campaigns(account_id=account_id, campaign_id=campaign_ids, start_date=start_date, end_date=end_date, date_preset=date_preset)
                    if adsets:
                        for a in adsets:
                            # Ghi đè campaign_id và account_id
                            a['account_id'] = account_id

                        all_adsets.extend(adsets)
                        adset_ids = [a['id'] for a in adsets]

                        ads = extractor.get_ads_for_adsets(account_id=account_id, adset_id=adset_ids, start_date=start_date, end_date=end_date, date_preset=date_preset)
                        if ads:
                            for ad in ads:
                                # Ghi đè campaign_id, adset_id và account_id
                                ad['account_id'] = account_id
                            all_ads.extend(ads)

            # Upsert hàng loạt vào các bảng Dimension
            logger.info("Bước 2: Cập nhật các bảng Dimension (Campaign, Adset, Ad)...")
            self.upsert_campaigns(all_campaigns)
            self.upsert_adsets(all_adsets)
            self.upsert_ads(all_ads)
            logger.info("=> Hoàn thành cập nhật Dimension.")

            # --- BƯỚC 2: LẤY VÀ CẬP NHẬT BẢNG FACT ---
            # Custom logs theo date_preset hoặc start_date/end_date
            if date_preset:
                logger.info(f"Lấy dữ liệu Insights cho khoảng thời gian preset: {date_preset}...")
            elif start_date and end_date:
                logger.info(f"Lấy dữ liệu Insights cho khoảng thời gian từ {start_date} đến {end_date}...")
            else:
                logger.info("Lấy dữ liệu Insights cho khoảng thời gian mặc định...")
            for account in accounts:
                account_id = account['id']
                logger.info(f"Lấy Insights platform cho tài khoản: {account_id}")
                insights_platform = extractor.get_all_insights_platform(
                    account_id=account_id,
                    date_preset=date_preset,
                    start_date=start_date,
                    end_date=end_date
                )
                if insights_platform:
                    all_insights_platform.extend(insights_platform)
                logger.info(f"Lấy Insights demographic cho tài khoản: {account_id}")
                insights_demographic = extractor.get_all_insights_demo(
                    account_id=account_id,
                    date_preset=date_preset,
                    start_date=start_date,
                    end_date=end_date
                )
                if insights_demographic:
                    all_insights_demographic.extend(insights_demographic)
            
            if not all_insights_platform and not all_insights_demographic:
                logger.warning("Không có dữ liệu insights nào được trả về từ API. Kết thúc quy trình.")
                return

            logger.info(f"Tổng cộng có {len(all_insights_platform) + len(all_insights_demographic)} bản ghi insights được lấy về.")

            # Cập nhật DimDate
            logger.info("Bước 4: Cập nhật bảng DimDate...")
            min_date_str = min(rec['date_start'] for rec in all_insights_platform + all_insights_demographic)
            max_date_str = max(rec['date_start'] for rec in all_insights_platform + all_insights_demographic)
            min_date = datetime.fromisoformat(min_date_str)
            max_date = datetime.fromisoformat(max_date_str)
            self.upsert_dates(min_date, max_date)
            logger.info("=> Hoàn thành cập nhật DimDate.")

            # Cập nhật FactPerformance
            logger.info("Bước 5: Cập nhật bảng FactPerformance...")
            self.upsert_performance_platform_data(all_insights_platform)
            self.upsert_performance_demographic_data(all_insights_demographic)
            logger.info("=> Hoàn thành cập nhật FactPerformance.")

        except Exception as e:
            logger.error(f"LỖI NGHIÊM TRỌNG trong quá trình làm mới dữ liệu: {e}", exc_info=True)
            # Ném lại lỗi để endpoint có thể bắt và trả về thông báo lỗi
            raise
    
    def refresh_data_fanpage(self, start_date: str, end_date: str):
        """
        Hàm chính để điều phối quy trình ETL cho Fanpage:
        1. Lấy danh sách Fanpage.
        2. Lấy Page Metrics (theo ngày).
        3. Lấy Post Metrics (lifetime).
        4. Cập nhật các bảng Dim và Fact.
        
        Args:
            start_date (str): Ngày bắt đầu (YYYY-MM-DD).
            end_date (str): Ngày kết thúc (YYYY-MM-DD).
        """
        from fbads_extract import FacebookAdsExtractor
        extractor = FacebookAdsExtractor()
        
        logger.info("--- BẮT ĐẦU QUY TRÌNH REFRESH DỮ LIỆU FANPAGE ---")
        
        try:
            # --- BƯỚC 1: LẤY VÀ CẬP NHẬT DIM_FANPAGE ---
            logger.info("Bước 1: Lấy và cập nhật dim_fanpage...")
            fanpages = extractor.get_all_fanpages()
            self.upsert_fanpages(fanpages)
            logger.info("=> Hoàn thành cập nhật dim_fanpage.")
            
            # --- BƯỚC 2: LẤY DỮ LIỆU TỪ API (CHO TẤT CẢ PAGES) ---
            all_page_metrics = []
            all_post_metrics = []

            for page in fanpages:
                page_id = page.get('id')
                page_token = page.get('access_token')
                page_name = page.get('name')
                
                if not page_token:
                    logger.warning(f"Bỏ qua Page {page_name} ({page_id}) vì không có Page Access Token.")
                    continue
                
                logger.info(f"--- Đang xử lý cho Fanpage: {page_name} ({page_id}) ---")
                
                # 2.1 Lấy Page Metrics (theo ngày)
                logger.info(f"Lấy Page Metrics (daily) từ {start_date} đến {end_date}...")
                page_metrics = extractor.get_page_metrics_by_day(
                    page_id=page_id,
                    page_access_token=page_token,
                    start_date=start_date,
                    end_date=end_date
                )
                all_page_metrics.extend(page_metrics)
                
                # 2.2 Lấy Post Metrics (lifetime, theo ngày post)
                logger.info(f"Lấy Post Metrics (lifetime) cho các post tạo từ {start_date} đến {end_date}...")
                post_metrics = extractor.get_posts_with_lifetime_insights(
                    page_id=page_id,
                    page_access_token=page_token,
                    start_date=start_date,
                    end_date=end_date,
                    metrics_list=None # Dùng default metrics
                )
                
                # Thêm page_id vào mỗi record post để load vào DB
                for post in post_metrics:
                    post['page_id'] = page_id
                all_post_metrics.extend(post_metrics)

            if not all_page_metrics and not all_post_metrics:
                logger.warning("Không có dữ liệu Fanpage nào được trả về từ API. Kết thúc.")
                return

            logger.info(f"Tổng cộng có {len(all_page_metrics)} bản ghi Page Metrics và {len(all_post_metrics)} bản ghi Post Metrics.")
            
            # --- BƯỚC 3: CẬP NHẬT DIM_DATE ---
            logger.info("Bước 3: Cập nhật bảng DimDate...")
            
            # Lấy tất cả các ngày từ cả hai nguồn dữ liệu
            page_dates = {rec['date'] for rec in all_page_metrics if 'date' in rec}
            post_dates = {rec['created_time'] for rec in all_post_metrics if 'created_time' in rec}
            
            all_dates_str = list(page_dates) + list(post_dates)
            
            if not all_dates_str:
                 logger.warning("Không tìm thấy ngày tháng nào trong dữ liệu, bỏ qua cập nhật DimDate.")
            else:
                # Chuyển đổi tất cả sang datetime object
                all_dates_obj = []
                for d_str in all_dates_str:
                    dt = parse_datetime_flexible(d_str)
                    if dt:
                        if dt.tzinfo is not None:
                            dt_naive = dt.astimezone(pytz.utc).replace(tzinfo=None)
                            all_dates_obj.append(dt_naive)
                        else:
                            all_dates_obj.append(dt)
                
                if all_dates_obj:
                    min_date = min(all_dates_obj)
                    max_date = max(all_dates_obj)
                    self.upsert_dates(min_date, max_date)
                    logger.info(f"=> Hoàn thành cập nhật DimDate (Từ {min_date.date()} đến {max_date.date()}).")

            # --- BƯỚC 4: CẬP NHẬT CÁC BẢNG FACT ---
            logger.info("Bước 4: Cập nhật các bảng Fact (Page Metrics và Post Performance)...")
            self.upsert_page_metrics_daily(all_page_metrics)
            self.upsert_post_performance(all_post_metrics)
            logger.info("=> Hoàn thành cập nhật các bảng Fact.")
            
            logger.info("--- QUY TRÌNH REFRESH DỮ LIỆU FANPAGE HOÀN TẤT ---")

        except Exception as e:
            logger.error(f"LỖI NGHIÊM TRỌNG trong quá trình làm mới dữ liệu Fanpage: {e}", exc_info=True)
            raise