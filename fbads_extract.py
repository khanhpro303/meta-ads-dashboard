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
from database_manager import DatabaseManager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATE_PRESET = ['today', 'yesterday', 'this_month', 'last_month', 'this_quarter', 'maximum', 'data_maximum', 'last_3d', 'last_7d', 'last_14d', 'last_28d', 'last_30d', 'last_90d', 'last_week_mon_sun', 'last_week_sun_sat', 'last_quarter', 'last_year', 'this_week_mon_today', 'this_week_sun_today', 'this_year']

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
        self.base_url = os.getenv("BASE_URL", "https://graph.facebook.com/v24.0")
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
        return all_accounts

    def get_campaigns_for_account(self, account_id: str, start_date: str, end_date: str, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả chiến dịch quảng cáo cho một tài khoản cụ thể trong một khoảng thời gian.
        Nếu có date_preset thì sử dụng date_preset thay vì start_date và end_date.
        """
        logger.info(f"Lấy chiến dịch cho tài khoản {account_id} từ ngày {start_date} đến {end_date}...")
        campaigns = []
        url = f"{self.base_url}/{account_id}/insights"

        if date_preset and date_preset in DATE_PRESET:
            params = {
                'access_token': self.access_token,
                'fields': 'account_id,campaign_id,campaign_name,created_time,objective,status',
                'limit': 100,
                'date_preset': date_preset,
                'level': 'campaign'
            }
        else:
            params = {
                'access_token': self.access_token,
                'fields': 'campaign_id,campaign_name,created_time,objective,status,account_id',
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

    def get_adsets_for_campaigns(self, account_id: str, campaign_id: List[str], start_date: str, end_date: str, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả adsets cho một hoặc nhiều chiến dịch cụ thể của tài khoản QC trong một khoảng thời gian.
        Nếu có date_preset thì sử dụng date_preset thay vì start_date và end_date.
        """
        logger.info(f"Lấy nhóm quảng cáo cho chiến dịch của tài khoản {account_id} và nhóm campaign {campaign_id}...")
        adsets = []
        url = f"{self.base_url}/{account_id}/insights"

        filtering_structure = [
            {
                'field': 'campaign.id',
                'operator': 'IN',
                'value': campaign_id
            }
        ]
        
        # Chuyển cấu trúc dữ liệu thành chuỗi JSON hợp lệ
        filtering_json_string = json.dumps(filtering_structure)
        if date_preset and date_preset in DATE_PRESET:
            params = {
                'level': 'adset',
                'filtering': filtering_json_string,
                'fields': 'campaign_id,adset_id,adset_name',
                'access_token': self.access_token,
                'limit': 100,
                'date_preset': date_preset
            }
        elif start_date and end_date:
            params = {
                'level': 'adset',
                'filtering': filtering_json_string,
                'fields': 'campaign_id,adset_id,adset_name',
                'access_token': self.access_token,
                'limit': 100,
                'time_range': json.dumps({
                    'since': start_date,
                    'until': end_date
                })
            }
        else:
            logger.error("Phải cung cấp hoặc date_preset hoặc cả start_date và end_date.")
            return adsets

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
                logger.error(f"Lỗi khi lấy Ad Sets cho chiến dịch cho tài khoản {account_id} (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break

        logger.info(f"Hoàn tất! Lấy được tổng cộng {len(adsets)} nhóm quảng cáo cho chiến dịch cho tài khoản {account_id}.")
        return adsets

    def get_ads_for_adsets(self, account_id: str, adset_id: List[str], start_date: str, end_date: str, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả quảng cáo cho một nhóm quảng cáo cụ thể thuộc một hoặc nhiều adset cụ thể của tài khoản quảng cáo trong khoảng thời gian.
        Nếu có date_preset thì sử dụng date_preset thay vì start_date và end_date.
        """
        logger.info(f"Lấy quảng cáo cho nhóm quảng cáo {adset_id} thuộc tài khoản {account_id}...")
        ads = []
        url = f"{self.base_url}/{account_id}/insights"
        filtering_structure = [
            {
                'field': 'adset.id',
                'operator': 'IN',
                'value': adset_id
            }
        ]
        # Chuyển cấu trúc dữ liệu thành chuỗi JSON hợp lệ
        filtering_json_string = json.dumps(filtering_structure)

        if date_preset and date_preset in DATE_PRESET:
            params = {
                'level': 'ad',
                'filtering': filtering_json_string,
                'fields': 'campaign_id,adset_id,ad_id,ad_name',
                'access_token': self.access_token,
                'limit': 100,
                'date_preset': date_preset
            }
        elif start_date and end_date:
            params = {
                'level': 'ad',
                'filtering': filtering_json_string,
                'fields': 'campaign_id,adset_id,ad_id,ad_name',
                'access_token': self.access_token,
                'limit': 100,
                'time_range': json.dumps({
                    'since': start_date,
                    'until': end_date
                })
            }
        else:
            logger.error("Phải cung cấp hoặc date_preset hoặc cả start_date và end_date.")
            return ads

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

    def get_insights(self, account_id: str, campaign_id: Optional[List[str]] = None, adset_id: Optional[List[str]] = None, ad_id: Optional[List[str]] = None, date_preset: Optional[str] = 'last_7d',
                     start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy dữ liệu insights cho các chiều (campaigns, adsets hoặc ads) tuỳ vào input.
        Nếu không có ID nào được cung cấp, sẽ lấy insights tổng hợp cho toàn bộ tài khoản.
        """
        logger.info(f"Lấy dữ liệu insights cho tài khoản {account_id}...")
        insights = []
        url = f"{self.base_url}/{account_id}/insights"

        params = {
            'access_token': self.access_token,
            'limit': 100,
            'fields': 'impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,action_values',
            'time_increment': 1,  # Lấy dữ liệu nhóm theo hàng ngày
            'breakdowns': 'publisher_platform,platform_position',
        }

        filtering_structure = []
        if campaign_id:
            filtering_structure.append({'field': 'campaign.id', 'operator': 'IN', 'value': campaign_id})
        if adset_id:
            filtering_structure.append({'field': 'adset.id', 'operator': 'IN', 'value': adset_id})
        if ad_id:
            filtering_structure.append({'field': 'ad.id', 'operator': 'IN', 'value': ad_id})
        
        if filtering_structure:
            params['filtering'] = json.dumps(filtering_structure)
        else:
            params['level'] = 'account'

        if start_date and end_date:
            params['time_range'] = json.dumps({'since': start_date, 'until': end_date})
        # Chỉ sử dụng date_preset nếu không có time_range.
        elif date_preset and date_preset in DATE_PRESET:
            params['date_preset'] = date_preset

        page_count = 0
        while url:
            try:
                page_count += 1
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                insights_page = data.get('data', [])
                if not insights_page:
                    logger.info("Không tìm thấy thêm dữ liệu insights nào.")
                    break
                    
                insights.extend(insights_page)
                logger.info(f"Đã lấy được {len(insights_page)} bản ghi insights (Tổng: {len(insights)}).")

                # Xử lý phân trang (Pagination)
                next_page_url = data.get('paging', {}).get('next')
                url = next_page_url # Nếu next_page_url là None, vòng lặp sẽ dừng
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy Insights cho tài khoản {account_id} (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break
        return insights

    def get_all_insights(self, account_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả dữ liệu insights cho tài khoản quảng cáo, dùng để làm database tổng hợp.,
        bao gồm chiến dịch, nhóm quảng cáo và quảng cáo, chia theo nền tảng và vị trí hiển thị.
        Nếu có date_preset thì sử dụng date_preset thay vì start_date và end_date
        """
        logger.info(f"Lấy tất cả dữ liệu insights cho tài khoản {account_id}...")
        all_insights = []
        url = f"{self.base_url}/{account_id}/insights"

        params = {
            'access_token': self.access_token,
            'level': 'ad',
            'limit': 100,
            'fields': 'campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,action_values',
            'time_increment': 1,  # Lấy dữ liệu nhóm theo hàng ngày
            'breakdowns': 'publisher_platform,platform_position',
        }

        if start_date and end_date:
            params['time_range'] = json.dumps({'since': start_date, 'until': end_date})
        # Chỉ sử dụng date_preset nếu không có time_range.
        elif date_preset and date_preset in DATE_PRESET:
            params['date_preset'] = date_preset

        page_count = 0
        while url:
            try:
                page_count += 1
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                insights_page = data.get('data', [])
                if not insights_page:
                    logger.info("Không tìm thấy thêm dữ liệu insights nào.")
                    break
                    
                all_insights.extend(insights_page)
                logger.info(f"Đã lấy được {len(insights_page)} bản ghi insights (Tổng: {len(all_insights)}).")

                # Xử lý phân trang (Pagination)
                next_page_url = data.get('paging', {}).get('next')
                url = next_page_url # Nếu next_page_url là None, vòng lặp sẽ dừng
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy Insights cho tài khoản {account_id} (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break
        return all_insights

def main():
    try:
        extractor = FacebookAdsExtractor()
        if extractor.test_connection():
            accounts = extractor.get_all_ad_accounts()
            if accounts:
                # Test xuất all insights
                first_account_id = accounts[4]['id']
                date_preset = 'last_7d'
                all_insights = extractor.get_all_insights(account_id=first_account_id, date_preset=date_preset)
                logger.info(f"Tổng số bản ghi insights lấy được cho tài khoản {first_account_id}: {len(all_insights)}")
                # Lưu dữ liệu vào file JSON
                extractor.save_to_json({'insights': all_insights}, filename='all_insights.json')
        else:
            logger.error("Không thể kết nối đến Facebook API với token hiện tại.")
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")

if __name__ == "__main__":
    main()