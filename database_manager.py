import os
import logging
from typing import Dict, List, Any, Set
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import UniqueConstraint, create_engine, Column, String, DateTime, MetaData, Table, ForeignKey, func, Float, BigInteger, Integer, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pytz
from fbads_extract import FacebookAdsExtractor

# Tải biến môi trường từ file .env
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy Base (Lớp cơ sở cho các model)
Base = declarative_base()

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
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    # Thêm Foreign Key (Khóa ngoại) liên kết đến Bảng Dimension Campaign
    campaign_id = Column(String, ForeignKey('dim_campaign.campaign_id'))

class DimAd(Base):
    """
    Bảng Dimension: Lưu trữ thông tin mô tả về Quảng cáo.
    """
    __tablename__ = 'dim_ad'
    
    ad_id = Column(String, primary_key=True)
    name = Column(String)
    status = Column(String)
    created_time = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    # Thêm Foreign Key (Khóa ngoại) liên kết đến Bảng Dimension Adset và Campaign
    campaign_id = Column(String, ForeignKey('dim_campaign.campaign_id'))
    adset_id = Column(String, ForeignKey('dim_adset.adset_id'))

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

class FactPerformance(Base):
    """
    Bảng Fact: Lưu trữ các chỉ số hiệu suất chi tiết theo từng breakdown.
    """
    __tablename__ = 'fact_performance'
    
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
    
    __table_args__ = (UniqueConstraint('date_key', 'ad_id', 'platform_id', 'placement_id', name='_ad_performance_uc'),)

# --- CLASS QUẢN LÝ DATABASE ---

class DatabaseManager:
    
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL')
        if not self.db_url:
            self.db_url = "sqlite:///meta_ads_dashboard.db"
            logger.warning("DATABASE_URL không được thiết lập, sử dụng SQLite local.")
        
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
                'campaign_id': camp['campaign_id'],
                'name': camp.get('campaign_name'),
                'objective': camp.get('objective'),
                'created_time': datetime.strptime(camp['created_time'], '%Y-%m-%dT%H:%M:%S%z') if 'created_time' in camp else None,
                'ad_account_id': camp['account_id']
            })

        stmt = pg_insert(DimCampaign).values(prepared_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['campaign_id'],
            set_={
                'name': stmt.excluded.name,
                'objective': stmt.excluded.objective,
                'created_time': stmt.excluded.created_time,
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
        finally:
            session.close()

    def _enrich_status_campaigns(self):
        """
        Cập nhật trường 'status' cho các chiến dịch trong dim_campaign.
        """
        session = self.SessionLocal()
        try:
            campaigns = session.query(DimCampaign).all()
            # Gọi API để lấy trạng thái mới nhất cho mỗi chiến dịch
            def fetch_campaign_status_from_api(campaign_id: str) -> str:
                import requests
                url = f"{self.base_url}/{campaign_id}"
                params = {
                    'fields': 'status',
                    'access_token': os.getenv('SECRET_KEY')
                }
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    return data.get('status')
                else:
                    logger.error(f"Lỗi API khi lấy trạng thái cho chiến dịch {campaign_id}: {response.text}")
                    return None
            for campaign in campaigns:
                latest_status = fetch_campaign_status_from_api(campaign.campaign_id)
                if latest_status and campaign.status != latest_status:
                    campaign.status = latest_status
                    campaign.updated_at = datetime.now()
            session.commit()
            logger.info("Đã cập nhật trạng thái cho các chiến dịch trong dim_campaign.")
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật trạng thái chiến dịch: {e}")
            session.rollback()
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
                'adset_id': adset['adset_id'],
                'name': adset.get('adset_name'),
                'status': adset.get('status'),
                'created_time': datetime.strptime(adset['created_time'], '%Y-%m-%dT%H:%M:%S%z') if 'created_time' in adset else None,
                # === BỔ SUNG KHÓA NGOẠI ===
                'campaign_id': adset.get('campaign_id')
            })

        stmt = pg_insert(DimAdset).values(prepared_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['adset_id'],
            set_={
                'name': stmt.excluded.name,
                'status': stmt.excluded.status,
                'created_time': stmt.excluded.created_time,
                # === BỔ SUNG CẬP NHẬT KHÓA NGOẠI ===
                'campaign_id': stmt.excluded.campaign_id,
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
        finally:
            session.close()

    def _enrich_status_adsets(self):
        """
        Cập nhật trường 'status' cho các nhóm quảng cáo trong dim_adset.
        """
        session = self.SessionLocal()
        try:
            adsets = session.query(DimAdset).all()
            # Gọi API để lấy trạng thái mới nhất cho mỗi nhóm quảng cáo
            def fetch_adset_status_from_api(adset_id: str) -> str:
                import requests
                url = f"{self.base_url}/{adset_id}"
                params = {
                    'fields': 'status',
                    'access_token': os.getenv('SECRET_KEY')
                }
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    return data.get('status')
                else:
                    logger.error(f"Lỗi API khi lấy trạng thái cho nhóm quảng cáo {adset_id}: {response.text}")
                    return None
            for adset in adsets:
                latest_status = fetch_adset_status_from_api(adset.adset_id)
                if latest_status and adset.status != latest_status:
                    adset.status = latest_status
                    adset.updated_at = datetime.now()
            session.commit()
            logger.info("Đã cập nhật trạng thái cho các nhóm quảng cáo trong dim_adset.")
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật trạng thái nhóm quảng cáo: {e}")
            session.rollback()
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
                'ad_id': ad['ad_id'],
                'name': ad.get('ad_name'),
                'status': ad.get('status'),
                'created_time': datetime.strptime(ad['created_time'], '%Y-%m-%dT%H:%M:%S%z') if 'created_time' in ad else None,
                # === BỔ SUNG CÁC KHÓA NGOẠI ===
                'adset_id': ad.get('adset_id'),
                'campaign_id': ad.get('campaign_id')
            })

        stmt = pg_insert(DimAd).values(prepared_data)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=['ad_id'],
            set_={
                'name': stmt.excluded.name,
                'status': stmt.excluded.status,
                'created_time': stmt.excluded.created_time,
                # === BỔ SUNG CẬP NHẬT CÁC KHÓA NGOẠI ===
                'adset_id': stmt.excluded.adset_id,
                'campaign_id': stmt.excluded.campaign_id,
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
        finally:
            session.close()

    def _enrich_status_ads(self):
        """
        Cập nhật trường 'status' cho các quảng cáo trong dim_ad.
        """
        session = self.SessionLocal()
        try:
            ads = session.query(DimAd).all()
            # Gọi API để lấy trạng thái mới nhất cho mỗi quảng cáo
            def fetch_ad_status_from_api(ad_id: str) -> str:
                import requests
                url = f"{self.base_url}/{ad_id}"
                params = {
                    'fields': 'status',
                    'access_token': os.getenv('SECRET_KEY')
                }
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    return data.get('status')
                else:
                    logger.error(f"Lỗi API khi lấy trạng thái cho quảng cáo {ad_id}: {response.text}")
                    return None
            for ad in ads:
                latest_status = fetch_ad_status_from_api(ad.ad_id)
                if latest_status and ad.status != latest_status:
                    ad.status = latest_status
                    ad.updated_at = datetime.now()
            session.commit()
            logger.info("Đã cập nhật trạng thái cho các quảng cáo trong dim_ad.")
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật trạng thái quảng cáo: {e}")
            session.rollback()
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
        new_values = values - set(mapping.keys())
        
        # 3. Insert các giá trị mới nếu có
        if new_values:
            new_records_data = [{column_name: val} for val in new_values]
            # Dùng bulk_insert_mappings để insert và lấy lại ID
            session.bulk_insert_mappings(model, new_records_data, return_defaults=True)
            session.commit()
            
            # Cập nhật lại mapping với các bản ghi vừa tạo
            for record_data in new_records_data:
                mapping[record_data[column_name]] = record_data[pk_name]
            logger.info(f"Đã tạo {len(new_values)} bản ghi mới trong bảng {model.__tablename__}.")
            
        return mapping

    def upsert_performance_data(self, insights_data: List[Dict[str, Any]]):
        """
        Thực hiện 'UPSERT' cho bảng fact_performance.
        Sử dụng phương pháp "Pre-loading Dimensions" để tối ưu hiệu suất.
        """
        if not insights_data:
            logger.info("Không có dữ liệu performance để upsert.")
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
            platform_map = self._get_or_create_dim_records(session, DimPlatform, 'platform_name', all_platforms)
            placement_map = self._get_or_create_dim_records(session, DimPlacement, 'placement_name', all_placements)

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
                logger.warning("Không có dữ liệu hợp lệ để chèn vào fact_performance sau khi chuẩn bị.")
                return

            # --- BƯỚC 3: Load hàng loạt vào Fact Table ---
            stmt = pg_insert(FactPerformance).values(prepared_data)
            on_conflict_stmt = stmt.on_conflict_do_update(
                constraint='_ad_performance_uc', # Sử dụng Unique Constraint đã định nghĩa
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
            logger.info(f"Đã Upsert thành công {len(prepared_data)} bản ghi vào fact_performance.")

        except Exception as e:
            logger.error(f"Lỗi khi Upsert Performance Data: {e}", exc_info=True)
            session.rollback()
        finally:
            session.close()
    
    def refresh_data(self, start_date: str = None, end_date: str = None, date_preset: str = None):
        """
        Hàm chính để điều phối toàn bộ quy trình ETL:
        1. Lấy dữ liệu mới từ Meta Ads API.
        2. Cập nhật các bảng Dimension.
        3. Cập nhật bảng Fact.
        """
        logger.info("===== BẮT ĐẦU QUY TRÌNH LÀM MỚI DỮ LIỆU =====")
        extractor = FacebookAdsExtractor()

        try:
            # --- BƯỚC 1: LẤY VÀ CẬP NHẬT CÁC BẢNG DIMENSION CƠ BẢN ---
            logger.info("Bước 1: Lấy và cập nhật danh sách tài khoản quảng cáo...")
            accounts = extractor.get_all_ad_accounts()
            self.upsert_ad_accounts(accounts)
            # Không lấy ad acc tên Nguyen Xuan Trang
            accounts = [acc for acc in accounts if acc['name'] != 'Nguyen Xuan Trang']
            logger.info("=> Hoàn thành cập nhật tài khoản loại bỏ 'Nguyen Xuan Trang'.")

            all_campaigns = []
            all_adsets = []
            all_ads = []
            all_insights = []

            for account in accounts:
                account_id = account['id']
                logger.info(f"--- Đang xử lý cho tài khoản: {account['name']} ({account_id}) ---")

                logger.info("Lấy dữ liệu chiến dịch...")
                campaigns = extractor.get_campaigns_for_account(account_id=account_id, start_date=start_date, end_date=end_date, date_preset=date_preset)
                if campaigns:
                    all_campaigns.extend(campaigns)
                    campaign_ids = [c['campaign_id'] for c in campaigns]

                    logger.info("Lấy dữ liệu nhóm quảng cáo...")
                    adsets = extractor.get_adsets_for_campaigns(account_id=account_id, campaign_ids=campaign_ids, start_date=start_date, end_date=end_date, date_preset=date_preset)
                    if adsets:
                        all_adsets.extend(adsets)
                        adset_ids = [a['adset_id'] for a in adsets]

                        logger.info("Lấy dữ liệu quảng cáo...")
                        ads = extractor.get_ads_for_adsets(account_id=account_id, adset_ids=adset_ids, start_date=start_date, end_date=end_date, date_preset=date_preset)
                        if ads:
                            all_ads.extend(ads)

            # Upsert hàng loạt vào các bảng Dimension
            logger.info("Bước 2: Cập nhật các bảng Dimension (Campaign, Adset, Ad)...")
            self.upsert_campaigns(all_campaigns)
            self.upsert_adsets(all_adsets)
            self.upsert_ads(all_ads)
            # Enrich trạng thái
            self._enrich_status_campaigns()
            self._enrich_status_adsets()
            self._enrich_status_ads()
            logger.info("=> Hoàn thành cập nhật Dimension.")

            # --- BƯỚC 2: LẤY VÀ CẬP NHẬT BẢNG FACT ---
            logger.info(f"Bước 3: Lấy dữ liệu Insights với khoảng thời gian (preset: {date_preset} hoặc start: {start_date}, end: {end_date})...")
            for account in accounts:
                account_id = account['id']
                logger.info(f"Lấy Insights cho tài khoản: {account_id}")
                insights = extractor.get_all_insights(
                    account_id=account_id,
                    date_preset=date_preset,
                    start_date=start_date,
                    end_date=end_date
                )
                if insights:
                    all_insights.extend(insights)
            
            if not all_insights:
                logger.warning("Không có dữ liệu insights nào được trả về từ API. Kết thúc quy trình.")
                return

            logger.info(f"Tổng cộng có {len(all_insights)} bản ghi insights được lấy về.")

            # Cập nhật DimDate
            logger.info("Bước 4: Cập nhật bảng DimDate...")
            min_date_str = min(rec['date_start'] for rec in all_insights)
            max_date_str = max(rec['date_start'] for rec in all_insights)
            min_date = datetime.fromisoformat(min_date_str)
            max_date = datetime.fromisoformat(max_date_str)
            self.upsert_dates(min_date, max_date)
            logger.info("=> Hoàn thành cập nhật DimDate.")

            # Cập nhật FactPerformance
            logger.info("Bước 5: Cập nhật bảng FactPerformance...")
            self.upsert_performance_data(all_insights)
            logger.info("=> Hoàn thành cập nhật FactPerformance.")

            logger.info("===== KẾT THÚC QUY TRÌNH LÀM MỚI DỮ LIỆU THÀNH CÔNG =====")

        except Exception as e:
            logger.error(f"LỖI NGHIÊM TRỌNG trong quá trình làm mới dữ liệu: {e}", exc_info=True)
            # Ném lại lỗi để endpoint có thể bắt và trả về thông báo lỗi
            raise