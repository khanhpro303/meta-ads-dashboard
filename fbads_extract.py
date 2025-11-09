#!/usr/bin/env python3

import os
import json
import logging
import shutil
from datetime import datetime, date
from typing import Dict, List, Any, Optional
import pytz
import requests
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def format_date_with_timezone(date_str: str, timezone_str: str = 'Asia/Ho_Chi_Minh') -> str:
    """
    Chuyển một chuỗi ngày (ví dụ: '2025-10-01') thành định dạng ISO 8601
    với múi giờ được chỉ định (ví dụ: '2025-10-01T00:00:00+0700').
    """
    try:
        # 1. Phân tích chuỗi ngày thành đối tượng datetime (mặc định giờ là 00:00:00)
        naive_dt = datetime.strptime(date_str, '%Y-%m-%d')
        
        # 2. Lấy đối tượng múi giờ
        target_timezone = pytz.timezone(timezone_str)
        
        # 3. Gán múi giờ cho đối tượng datetime "ngây thơ" (naive)
        aware_dt = target_timezone.localize(naive_dt)
        
        # 4. Định dạng lại thành chuỗi theo chuẩn ISO 8601 với offset
        # %z sẽ tạo ra offset dạng +HHMM (ví dụ: +0700)
        return aware_dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    except Exception as e:
        logger.error(f"Lỗi khi định dạng ngày '{date_str}': {e}")
        return "" # Trả về chuỗi rỗng nếu có lỗi

class FacebookAdsExtractor:
    def __init__(self):
        load_dotenv()
        self.access_token = os.getenv("SECRET_KEY")
        self.base_url = os.getenv("BASE_URL", "https://graph.facebook.com/v19.0")
        self.account_ids = []
        self.account_names = []
        if not self.access_token:
            raise ValueError("SECRET_KEY không được cấu hình")
        
    def save_to_json(self, data: Dict[str, Any], filename: str = "ads_data.json") -> bool:
        """
        Lưu dữ liệu vào file JSON một cách an toàn.
        Sử dụng file tạm thời để đảm bảo tính toàn vẹn dữ liệu."""
        import tempfile
        
        try:
            temp_filename = filename + '.tmp'
            with open(temp_filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            shutil.move(temp_filename, filename)
            
            logger.info(f"Dữ liệu đã được lưu vào {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu file: {e}")
            # Clean up temporary file if it exists
            try:
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
            except:
                pass
            return False
    def generate_sample_data(self) -> Dict[str, Any]:
        sample_data = {
            'extraction_date': datetime.now().isoformat(),
            'start_date': '2023-01-01',
            'campaigns': [
                {
                    'account_id': 'act_123456789',
                    'campaign_id': '123456789',
                    'campaign_name': 'Campaign A - Brand Awareness',
                    'status': 'ACTIVE',
                    'objective': 'BRAND_AWARENESS',
                    'created_time': '2023-01-15T00:00:00+0000',
                    'start_time': '2023-01-15T00:00:00+0000',
                    'stop_time': None,
                    'insights': {
                        'campaign_name': 'Campaign A - Brand Awareness',
                        'impressions': '150000',
                        'clicks': '2500',
                        'spend': '5000.00',
                        'ctr': '1.67',
                        'cpc': '2.00',
                        'cpm': '33.33'
                    }
                },
                {
                    'account_id': 'act_123456789',
                    'campaign_id': '123456790',
                    'campaign_name': 'Campaign B - Conversions',
                    'status': 'ACTIVE',
                    'objective': 'CONVERSIONS',
                    'created_time': '2023-02-01T00:00:00+0000',
                    'start_time': '2023-02-01T00:00:00+0000',
                    'stop_time': None,
                    'insights': {
                        'campaign_name': 'Campaign B - Conversions',
                        'impressions': '80000',
                        'clicks': '4000',
                        'spend': '8000.00',
                        'ctr': '5.00',
                        'cpc': '2.00',
                        'cpm': '100.00'
                    }
                },
                {
                    'account_id': 'act_123456789',
                    'campaign_id': '123456791',
                    'campaign_name': 'Campaign C - Traffic',
                    'status': 'ACTIVE',
                    'objective': 'TRAFFIC',
                    'created_time': '2023-03-01T00:00:00+0000',
                    'start_time': '2023-03-01T00:00:00+0000',
                    'stop_time': None,
                    'insights': {
                        'campaign_name': 'Campaign C - Traffic',
                        'impressions': '120000',
                        'clicks': '3000',
                        'spend': '6000.00',
                        'ctr': '2.50',
                        'cpc': '2.00',
                        'cpm': '50.00'
                    }
                }
            ]
        }
        
        return sample_data
    
    def test_connection(self) -> bool:
        test_url = f"{self.base_url}/me/adaccounts"
        params = {
            'access_token': self.access_token,
            'fields': 'id,name'
        }
        try:
            response = requests.get(test_url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Kết nối đến Facebook API thành công. Tìm thấy {len(data.get('data', []))} tài khoản quảng cáo.")
            return True
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi kết nối: {e}")
            return False
    
    def get_all_ad_accounts(self) -> List[Dict[str, Any]]:
        """
        Lấy TẤT CẢ các tài khoản quảng cáo mà token có quyền truy cập.
        Tự động xử lý phân trang (pagination).
        """
        logger.info("Bắt đầu lấy danh sách tất cả tài khoản quảng cáo...")
        all_accounts = []
        
        url = f"{self.base_url}/me/adaccounts"
        params = {
            'access_token': self.access_token,
            'fields': 'id,name',
            'limit': 100 # Lấy 100 tài khoản mỗi lượt
        }
        
        page_count = 0
        while url:
            try:
                page_count += 1
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                accounts_page = data.get('data', [])
                if not accounts_page:
                    logger.info("Không tìm thấy thêm tài khoản nào.")
                    break
                    
                all_accounts.extend(accounts_page)
                logger.info(f"Đã lấy được {len(accounts_page)} tài khoản (Tổng: {len(all_accounts)}).")

                # Xử lý phân trang (Pagination)
                next_page_url = data.get('paging', {}).get('next')
                url = next_page_url # Nếu next_page_url là None, vòng lặp sẽ dừng
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy Ad Accounts (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break
        
        logger.info(f"Hoàn tất! Lấy được tổng cộng {len(all_accounts)} tài khoản quảng cáo.")
        # Cập nhật lại danh sách account_ids trong class
        self.account_ids = [acc['id'] for acc in all_accounts]
        self.account_names = [acc['name'] for acc in all_accounts]
        return all_accounts

    def get_campaigns_for_account(self, account_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Lấy tất cả chiến dịch quảng cáo cho một tài khoản cụ thể trong một khoảng thời gian.
        """
        logger.info(f"Lấy chiến dịch cho tài khoản {account_id} từ ngày {start_date} đến {end_date}...")
        campaigns = []
        
        url = f"{self.base_url}/{account_id}/insights"
        params = {
            'access_token': self.access_token,
            'fields': 'campaign_id,campaign_name,created_time',
            'limit': 100,
            'time_range': json.dumps({
                'since': start_date,
                'until': end_date
            }),
            'level': 'campaign'
        }
        
        page_count = 0
        while url:
            try:
                page_count += 1
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                campaigns_page = data.get('data', [])
                if not campaigns_page:
                    logger.info("Không tìm thấy thêm chiến dịch nào.")
                    break
                    
                campaigns.extend(campaigns_page)
                logger.info(f"Đã lấy được {len(campaigns_page)} chiến dịch (Tổng: {len(campaigns)}).")

                # Xử lý phân trang (Pagination)
                next_page_url = data.get('paging', {}).get('next')
                url = next_page_url # Nếu next_page_url là None, vòng lặp sẽ dừng
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy Campaigns cho tài khoản {account_id} (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break
        
        logger.info(f"Hoàn tất! Lấy được tổng cộng {len(campaigns)} chiến dịch cho tài khoản {account_id}.")
        return campaigns

    def get_adsets_for_campaign(self, campaign_id: str) -> List[Dict[str, Any]]:
        """
        Lấy tất cả adsets cho một chiến dịch cụ thể.
        """
        logger.info(f"Lấy nhóm quảng cáo cho chiến dịch {campaign_id}...")
        adsets = []

        url = f"{self.base_url}/{campaign_id}/adsets"
        params = {
            'access_token': self.access_token,
            'fields': 'id,name,created_time,start_time,stop_time',
            'limit': 100
        }
        
        page_count = 0
        while url:
            try:
                page_count += 1
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                adsets_page = data.get('data', [])
                if not adsets_page:
                    logger.info("Không tìm thấy thêm nhóm quảng cáo nào.")
                    break
                    
                adsets.extend(adsets_page)
                logger.info(f"Đã lấy được {len(adsets_page)} nhóm quảng cáo (Tổng: {len(adsets)}).")

                # Xử lý phân trang (Pagination)
                next_page_url = data.get('paging', {}).get('next')
                url = next_page_url # Nếu next_page_url là None, vòng lặp sẽ dừng
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy Ad Sets cho chiến dịch {campaign_id} (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break

        logger.info(f"Hoàn tất! Lấy được tổng cộng {len(adsets)} nhóm quảng cáo cho chiến dịch {campaign_id}.")
        return adsets

    def get_ads_for_adset(self, adset_id: str) -> List[Dict[str, Any]]:
        """
        Lấy tất cả quảng cáo cho một nhóm quảng cáo cụ thể.
        """
        logger.info(f"Lấy quảng cáo cho nhóm quảng cáo {adset_id}...")
        ads = []

        url = f"{self.base_url}/{adset_id}/ads"
        params = {
            'access_token': self.access_token,
            'fields': 'id,name',
            'limit': 100
        }
        
        page_count = 0
        while url:
            try:
                page_count += 1
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                ads_page = data.get('data', [])
                if not ads_page:
                    logger.info("Không tìm thấy thêm quảng cáo nào.")
                    break
                    
                ads.extend(ads_page)
                logger.info(f"Đã lấy được {len(ads_page)} quảng cáo (Tổng: {len(ads)}).")

                # Xử lý phân trang (Pagination)
                next_page_url = data.get('paging', {}).get('next')
                url = next_page_url # Nếu next_page_url là None, vòng lặp sẽ dừng
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy Ads cho nhóm quảng cáo {adset_id} (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break

        logger.info(f"Hoàn tất! Lấy được tổng cộng {len(ads)} quảng cáo cho nhóm quảng cáo {adset_id}.")
        return ads
    
    def get_insights(self, campaign_id: str, start_date: str, end_date: str) -> Optional[Dict[str, Any]]:
        """
        Hàm master để lấy dữ liệu thống kê cho một chiến dịch trong khoảng thời gian nhất định.
        """
        logger.info(f"Lấy số liệu thống kê cho chiến dịch {campaign_id} từ {start_date} đến {end_date}...")
        
        url = f"{self.base_url}/{campaign_id}/insights"
        params = {
            'access_token': self.access_token,
            'fields': 'impressions,clicks,spend,ctr,cpc,cpm',
            'time_range': json.dumps({
                'since': start_date,
                'until': end_date
            }),
            'limit': 1
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            insights = data.get('data', [])
            if insights:
                logger.info(f"Lấy số liệu thành công cho chiến dịch {campaign_id}.")
                return insights[0]
            else:
                logger.info(f"Không có số liệu nào cho chiến dịch {campaign_id}.")
                return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi lấy số liệu thống kê cho chiến dịch {campaign_id}: {e}")
            if e.response is not None:
                logger.error(f"Response: {e.response.json()}")
            return None
        except Exception as e:
            logger.error(f"Lỗi không xác định: {e}")
            return None

def main():
    try:
        adsets = []
        extractor = FacebookAdsExtractor()
        if extractor.test_connection():
            accounts = extractor.get_all_ad_accounts()
            if accounts:
                first_account_id = accounts[4]['id']
                campaigns = extractor.get_campaigns_for_account(first_account_id, '2025-10-01', '2025-10-10')
                if campaigns:
                    first_campaign_id = campaigns[0]['campaign_id']
                    adsets = extractor.get_adsets_for_campaign(first_campaign_id)
                    if adsets:
                        first_adset_id = adsets[0]['id']
                        ads = extractor.get_ads_for_adset(first_adset_id)
                        print(f"In mẫu dữ liệu: {ads[:1]}")  # In ra một ad mẫu
            print(f"In mẫu dữ liệu: {adsets[:1]}")  # In ra một adset mẫu
        else:
            logger.error("Không thể kết nối đến Facebook API với token hiện tại.")
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")

if __name__ == "__main__":
    main()