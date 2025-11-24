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
from dateutil.relativedelta import relativedelta
from storage_manager import StorageManager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATE_PRESET = ['today', 'yesterday', 'this_month', 'last_month', 'this_quarter', 'maximum', 'data_maximum', 'last_3d', 'last_7d', 'last_14d', 'last_28d', 'last_30d', 'last_90d', 'last_week_mon_sun', 'last_week_sun_sat', 'last_quarter', 'last_year', 'this_week_mon_today', 'this_week_sun_today', 'this_year']

class FacebookAdsExtractor:
    def __init__(self):
        load_dotenv()
        self.access_token = os.getenv("SECRET_KEY")
        self.base_url = os.getenv("BASE_URL", "https://graph.facebook.com/v24.0")
        self.account_ids = []
        self.account_names = []
        self.storage_manager = StorageManager()
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
        return all_accounts

    def get_campaigns_for_account(self, account_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả chiến dịch quảng cáo cho một tài khoản cụ thể trong một khoảng thời gian.
        Nếu có date_preset thì sử dụng date_preset thay vì start_date và end_date.
        """
        campaigns = []
        url = f"{self.base_url}/{account_id}/campaigns"

        filtering_structure = [
            {
                'field': 'effective_status',
                'operator': 'IN',
                'value': ['ACTIVE', 'PAUSED', 'ARCHIVED', 'DELETED']
            }
        ]
        filtering_json_string = json.dumps(filtering_structure)

        if date_preset and date_preset in DATE_PRESET:
            params = {
                'access_token': self.access_token,
                'fields': 'account_id,id,name,created_time,objective,status,start_time,stop_time',
                'limit': 100,
                'date_preset': date_preset,
                'filtering': filtering_json_string
            }
            logger.info(f"Lấy chiến dịch cho tài khoản {account_id} với khoảng '{date_preset}'...")
        else:
            params = {
                'access_token': self.access_token,
                'fields': 'account_id,id,name,created_time,objective,status,start_time,stop_time',
                'limit': 100,
                'time_range': json.dumps({
                    'since': start_date,
                    'until': end_date
                }),
                'filtering': filtering_json_string
            }
            logger.info(f"Lấy chiến dịch cho tài khoản {account_id} từ {start_date} đến {end_date}...")
        
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

    def get_adsets_for_campaigns(self, account_id: str, campaign_id: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả adsets cho một hoặc nhiều chiến dịch cụ thể của tài khoản QC trong một khoảng thời gian.
        Nếu có date_preset thì sử dụng date_preset thay vì start_date và end_date.
        """
        adsets = []
        url = f"{self.base_url}/{account_id}/adsets"

        filtering_structure = [
            {
                'field': 'campaign.id',
                'operator': 'IN',
                'value': campaign_id
            },
            {
                'field': 'effective_status',
                'operator': 'IN',
                'value': ['ACTIVE', 'PAUSED', 'ARCHIVED', 'DELETED']
            }
        ]
        
        # Chuyển cấu trúc dữ liệu thành chuỗi JSON hợp lệ
        filtering_json_string = json.dumps(filtering_structure)
        if date_preset and date_preset in DATE_PRESET:
            params = {
                'filtering': filtering_json_string,
                'fields': 'account_id,campaign_id,id,name,created_time,status,start_time,end_time',
                'access_token': self.access_token,
                'limit': 100,
                'date_preset': date_preset
            }
            logger.info(f"Lấy nhóm quảng cáo cho chiến dịch của tài khoản {account_id} và của tổng {len(campaign_id)} chiến dịch trong khoảng '{date_preset}'...")
        elif start_date and end_date:
            params = {
                'filtering': filtering_json_string,
                'fields': 'account_id,campaign_id,id,name,created_time,status,start_time,end_time',
                'access_token': self.access_token,
                'limit': 100,
                'time_range': json.dumps({
                    'since': start_date,
                    'until': end_date
                })
            }
            logger.info(f"Lấy nhóm quảng cáo cho chiến dịch của tài khoản {account_id} và của tổng {len(campaign_id)} chiến dịch từ {start_date} đến {end_date}...")
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

    def get_ads_for_adsets(self, account_id: str, adset_id: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả quảng cáo cho một nhóm quảng cáo cụ thể thuộc một hoặc nhiều adset cụ thể của tài khoản quảng cáo trong khoảng thời gian.
        Nếu có date_preset thì sử dụng date_preset thay vì start_date và end_date.
        """
        ads = []
        url = f"{self.base_url}/{account_id}/ads"
        filtering_structure = [
            {
                'field': 'adset.id',
                'operator': 'IN',
                'value': adset_id
            },
            {
                'field': 'effective_status',
                'operator': 'IN',
                'value': ['ACTIVE', 'PAUSED', 'ARCHIVED', 'DELETED']
            }
        ]
        # Chuyển cấu trúc dữ liệu thành chuỗi JSON hợp lệ
        filtering_json_string = json.dumps(filtering_structure)

        if date_preset and date_preset in DATE_PRESET:
            params = {
                'filtering': filtering_json_string,
                'fields': 'account_id,campaign_id,adset_id,id,name,created_time,status,ad_schedule_start_time,ad_schedule_end_time',
                'access_token': self.access_token,
                'limit': 100,
                'date_preset': date_preset
            }
            logger.info(f"Lấy quảng cáo cho tổng {len(adset_id)} nhóm quảng cáo thuộc tài khoản {account_id} trong khoảng '{date_preset}'...")
        elif start_date and end_date:
            params = {
                'filtering': filtering_json_string,
                'fields': 'account_id,campaign_id,adset_id,id,name,created_time,status,ad_schedule_start_time,ad_schedule_end_time',
                'access_token': self.access_token,
                'limit': 100,
                'time_range': json.dumps({
                    'since': start_date,
                    'until': end_date
                })
            }
            logger.info(f"Lấy quảng cáo cho tổng {len(adset_id)} nhóm quảng cáo thuộc tài khoản {account_id} từ {start_date} đến {end_date}...")
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

        logger.info(f"Hoàn tất! Lấy được tổng cộng {len(ads)} quảng cáo cho tổng {len(adset_id)} nhóm quảng cáo.")
        return ads

    def get_insights_platform(self, account_id: str, campaign_id: Optional[List[str]] = None, adset_id: Optional[List[str]] = None, ad_id: Optional[List[str]] = None, date_preset: Optional[str] = 'last_7d',
                     start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy dữ liệu insights breakdown theo vị trí quảng cáo.
        Nếu không có ID nào được cung cấp, sẽ lấy insights tổng hợp cho toàn bộ tài khoản.
        """
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
            filtering_structure.append({'level': 'campaign'})
        elif adset_id:
            filtering_structure.append({'field': 'adset.id', 'operator': 'IN', 'value': adset_id})
            filtering_structure.append({'level': 'adset'})
        elif ad_id:
            filtering_structure.append({'field': 'ad.id', 'operator': 'IN', 'value': ad_id})
            filtering_structure.append({'level': 'ad'})
        
        if filtering_structure:
            params['filtering'] = json.dumps(filtering_structure)
        else:
            params['level'] = 'account'

        # Chỉ sử dụng date_preset nếu không có time_range.
        if date_preset and date_preset in DATE_PRESET:
            params['date_preset'] = date_preset
            logger.info(f"Lấy dữ liệu insights cho tài khoản {account_id} với khoảng '{date_preset}'...")
        elif start_date and end_date:
            params['time_range'] = json.dumps({'since': start_date, 'until': end_date})
            logger.info(f"Lấy dữ liệu insights cho tài khoản {account_id} từ {start_date} đến {end_date}...")

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
    
    def get_insights_demo(self, account_id: str, campaign_id: Optional[List[str]] = None, adset_id: Optional[List[str]] = None, ad_id: Optional[List[str]] = None, date_preset: Optional[str] = 'last_7d',
                     start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy dữ liệu insights breakdown theo nhân khẩu học giới tính và độ tuổi.
        Nếu không có ID nào được cung cấp, sẽ lấy insights tổng hợp cho toàn bộ tài khoản.
        """
        insights = []
        url = f"{self.base_url}/{account_id}/insights"

        params = {
            'access_token': self.access_token,
            'limit': 100,
            'fields': 'impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,action_values',
            'time_increment': 1,  # Lấy dữ liệu nhóm theo hàng ngày
            'breakdowns': 'age,gender',
        }

        filtering_structure = []
        if campaign_id:
            filtering_structure.append({'field': 'campaign.id', 'operator': 'IN', 'value': campaign_id})
            filtering_structure.append({'level': 'campaign'})
        elif adset_id:
            filtering_structure.append({'field': 'adset.id', 'operator': 'IN', 'value': adset_id})
            filtering_structure.append({'level': 'adset'})
        elif ad_id:
            filtering_structure.append({'field': 'ad.id', 'operator': 'IN', 'value': ad_id})
            filtering_structure.append({'level': 'ad'})
        
        if filtering_structure:
            params['filtering'] = json.dumps(filtering_structure)
        else:
            params['level'] = 'account'

        # Chỉ sử dụng date_preset nếu không có time_range.
        if date_preset and date_preset in DATE_PRESET:
            params['date_preset'] = date_preset
            logger.info(f"Lấy dữ liệu insights cho tài khoản {account_id} với khoảng '{date_preset}'...")
        elif start_date and end_date:
            params['time_range'] = json.dumps({'since': start_date, 'until': end_date})
            logger.info(f"Lấy dữ liệu insights cho tài khoản {account_id} từ {start_date} đến {end_date}...")

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

    def get_all_insights_platform(self, account_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả dữ liệu insights theo từ cấp độ quảng cáo cho theo vị trí quảng cáo
        """
        all_insights = []
        filtering_structure = [
            {
                'field': 'ad.effective_status',
                'operator': 'IN',
                'value': ['ACTIVE', 'PAUSED', 'ARCHIVED', 'DELETED']
            }
        ]
        url = f"{self.base_url}/{account_id}/insights"

        params = {
            'access_token': self.access_token,
            'level': 'ad',
            'limit': 100,
            'fields': 'campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,action_values',
            'time_increment': 1,  # Lấy dữ liệu nhóm theo hàng ngày
            'breakdowns': 'publisher_platform,platform_position',
            'filtering': json.dumps(filtering_structure),
        }

        # Chỉ sử dụng date_preset nếu không có time_range.
        if date_preset and date_preset in DATE_PRESET:
            params['date_preset'] = date_preset
            logger.info(f"Lấy tất cả dữ liệu insights cho tài khoản {account_id} với khoảng '{date_preset}'...")
        elif start_date and end_date:
            params['time_range'] = json.dumps({'since': start_date, 'until': end_date})
            logger.info(f"Lấy tất cả dữ liệu insights cho tài khoản {account_id} từ {start_date} đến {end_date}...")

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
    
    def get_all_insights_demo(self, account_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả dữ liệu insights theo từ cấp độ quảng cáo cho theo nhân khẩu học.
        """
        all_insights = []
        url = f"{self.base_url}/{account_id}/insights"

        params = {
            'access_token': self.access_token,
            'level': 'ad',
            'limit': 100,
            'fields': 'campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,action_values',
            'time_increment': 1,  # Lấy dữ liệu nhóm theo hàng ngày
            'breakdowns': 'age,gender',
        }

        # Chỉ sử dụng date_preset nếu không có time_range.
        if date_preset and date_preset in DATE_PRESET:
            params['date_preset'] = date_preset
            logger.info(f"Lấy tất cả dữ liệu insights cho tài khoản {account_id} với khoảng '{date_preset}'...")
        elif start_date and end_date:
            params['time_range'] = json.dumps({'since': start_date, 'until': end_date})
            logger.info(f"Lấy tất cả dữ liệu insights cho tài khoản {account_id} từ {start_date} đến {end_date}...")

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
    
    def get_all_insights_region(self, account_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_preset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy tất cả dữ liệu insights theo từ cấp độ quảng cáo cho theo khu vực (region).
        """
        all_insights = []
        url = f"{self.base_url}/{account_id}/insights"

        params = {
            'access_token': self.access_token,
            'level': 'ad',
            'limit': 100,
            'fields': 'campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,action_values',
            'time_increment': 1,  # Lấy dữ liệu nhóm theo hàng ngày
            'breakdowns': 'region',
        }

        # Chỉ sử dụng date_preset nếu không có time_range.
        if date_preset and date_preset in DATE_PRESET:
            params['date_preset'] = date_preset
            logger.info(f"Lấy tất cả dữ liệu insights (region) cho tài khoản {account_id} với khoảng '{date_preset}'...")
        elif start_date and end_date:
            params['time_range'] = json.dumps({'since': start_date, 'until': end_date})
            logger.info(f"Lấy tất cả dữ liệu insights (region) cho tài khoản {account_id} từ {start_date} đến {end_date}...")
        else:
            # Thêm fallback để tránh lỗi
            logger.error("Phải cung cấp date_preset hoặc (start_date, end_date) cho get_all_insights_region.")
            return all_insights

        page_count = 0
        while url:
            try:
                page_count += 1
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                insights_page = data.get('data', [])
                if not insights_page:
                    logger.info("Không tìm thấy thêm dữ liệu insights (region) nào.")
                    break
                    
                all_insights.extend(insights_page)
                logger.info(f"Đã lấy được {len(insights_page)} bản ghi insights (region) (Tổng: {len(all_insights)}).")

                # Xử lý phân trang (Pagination)
                next_page_url = data.get('paging', {}).get('next')
                url = next_page_url # Nếu next_page_url là None, vòng lặp sẽ dừng
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy Insights (region) cho tài khoản {account_id} (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break
        return all_insights
    
    def get_total_metric(self, account_id: str, metric_name: str, 
                         campaign_ids: Optional[List[str]] = None, 
                         adset_ids: Optional[List[str]] = None, 
                         ad_ids: Optional[List[str]] = None, 
                         date_preset: Optional[str] = 'last_7d',
                         start_date: Optional[str] = None, 
                         end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Lấy một metric tổng hợp (ví dụ: reach, spend) cho một cấp độ cụ thể 
        mà KHÔNG CÓ breakdown hoặc time_increment.
        
        Vì có các metric không thể cộng dồn như 'reach'.
        
        Nếu truyền vào một danh sách ID (ví dụ 5 campaign_id), nhận lại một danh sách 5 kết quả, mỗi kết quả là tổng metric cho một campaign đó.
        """
        insights_data = []
        url = f"{self.base_url}/{account_id}/insights"

        params = {
            'access_token': self.access_token,
            'limit': 100,
            'fields': metric_name,
        }

        filtering_structure = []
        level = 'account'
        id_list_len = 0 # Dùng cho logging

        # Logic xác định level và filtering
        if campaign_ids:
            level = 'campaign'
            filtering_structure.append({'field': 'campaign.id', 'operator': 'IN', 'value': campaign_ids})
            id_list_len = len(campaign_ids)
        elif adset_ids:
            level = 'adset'
            filtering_structure.append({'field': 'adset.id', 'operator': 'IN', 'value': adset_ids})
            id_list_len = len(adset_ids)
        elif ad_ids:
            level = 'ad'
            filtering_structure.append({'field': 'ad.id', 'operator': 'IN', 'value': ad_ids})
            id_list_len = len(ad_ids)
        
        params['level'] = level
        
        if filtering_structure:
            params['filtering'] = json.dumps(filtering_structure)

        # Log
        logger_msg = f"Lấy metric tổng '{metric_name}' cho cấp {level}"
        if filtering_structure:
             logger_msg += f" (lọc {id_list_len} IDs)"

        # Logic về thời gian
        if date_preset and date_preset in DATE_PRESET:
            params['date_preset'] = date_preset
            logger.info(f"{logger_msg} với khoảng '{date_preset}'...")
        elif start_date and end_date:
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            params['time_range'] = json.dumps({'since': start_date_str, 'until': end_date_str})
            logger.info(f"{logger_msg} từ {start_date} đến {end_date}...")
        else:
            logger.error("Cần cung cấp date_preset hoặc (start_date và end_date)")
            return insights_data # Trả về list rỗng

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
                    
                insights_data.extend(insights_page)
                logger.info(f"Đã lấy được {len(insights_page)} bản ghi metric (Tổng: {len(insights_data)}).")

                # Xử lý phân trang (Pagination)
                next_page_url = data.get('paging', {}).get('next')
                url = next_page_url # Nếu next_page_url là None, vòng lặp sẽ dừng
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy metric '{metric_name}' (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break
                
        return insights_data

    def get_all_fanpages(self) -> List[Dict[str, Any]]:
        """
        Lấy TẤT CẢ các Fanpage.
        Tự động xử lý phân trang (pagination).
        """
        all_pages = []
        
        url = f"{self.base_url}/me/accounts"
        
        # Các trường (fields) rất quan trọng cho Fanpage
        # - access_token: Đây là Page Access Token, bạn cần nó để
        #                 thực hiện hành động (post bài, đọc insights) cho Trang đó.
        # - tasks: Cho bạn biết bạn có những quyền gì trên trang này (ví dụ: MANAGE, MODERATE)
        params = {
            'access_token': self.access_token,
            'fields': 'id,name,access_token,category,tasks', 
            'limit': 100 
        }
        
        page_count = 0
        while url:
            try:
                page_count += 1
                # Chỉ sử dụng params cho lần gọi đầu tiên
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                pages_page = data.get('data', [])
                if not pages_page:
                    logger.info("Không tìm thấy thêm Fanpage nào.")
                    break
                    
                all_pages.extend(pages_page)
                logger.info(f"Đã lấy được {len(pages_page)} Fanpage (Tổng: {len(all_pages)}).")

                # Xử lý phân trang (Pagination) y hệt logic cũ
                paging_data = data.get('paging', {})
                if paging_data:
                    url = paging_data.get('next') # Nếu next_page_url là None, vòng lặp sẽ dừng
                else:
                    url = None # Dừng nếu không có key 'paging'
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi lấy Fanpages (Trang {page_count}): {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break
                
        logger.info(f"Hoàn tất! Lấy được tổng cộng {len(all_pages)} Fanpage.")
        return all_pages
    
    def get_page_metrics_by_day(self, page_id: str, page_access_token: str, 
                                  start_date: Optional[str] = None, end_date: Optional[str] = None,
                                  metrics_list: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Lấy TỔNG HỢP metrics cho Fanpage, trả về một danh sách "phẳng" (flat list)
        nhóm theo NGÀY, phù hợp để load vào database. Backend JS sẽ xử lý date_preset và input vào sau
        
        Args:
            page_id (str): ID của Fanpage.
            page_access_token (str): Page Access Token của Fanpage đó.
            start_date (str): Ngày bắt đầu (YYYY-MM-DD).
            end_date (str): Ngày kết thúc (YYYY-MM-DD).
            metrics_list (Optional[List[str]]): Danh sách metric.
        """
        final_daily_list = []
        # 1. Xử lý input ngày tháng
        try:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError:
            logger.error("Định dạng ngày không hợp lệ. Vui lòng dùng 'YYYY-MM-DD'.")
            return []

        # 2. Thiết lập metrics
        if not metrics_list:
            metrics_list = [
                'page_follows', # Accumulative lifetime 
                'page_media_view', # Page impression
                'page_post_engagements', 
                'page_video_views',
                'page_impressions_unique',
                'page_daily_unfollows_unique',
                'page_daily_follows_unique'
            ]
        metrics_str = ",".join(metrics_list)
        
        # 3. Xây dựng URL và params ban đầu
        url = f"{self.base_url}/{page_id}/insights"
        
        params = {
            'access_token': page_access_token,
            'metric': metrics_str,
            'period': 'day',
            'since': start_date,
            'until': end_date,
            'debug': 'all',
            'limit': 100 # Yêu cầu tối đa 100 ngày mỗi lần
        }
        
        # 4. Dictionary để "pivot" dữ liệu theo ngày
        #    Key là ngày (VD: '2025-10-01'), 
        #    Value là một dict chứa tất cả metric của ngày đó
        daily_data_pivot = {}
        
        page_count = 0
        keep_looping = True # Cờ để dừng vòng lặp ngoài

        while url and keep_looping:
            try:
                page_count += 1
                current_params = params if page_count == 1 else {}
                
                response = requests.get(url, params=current_params)
                response.raise_for_status()
                
                data = response.json()
                metrics_page = data.get('data', [])

                if not metrics_page:
                    logger.info("Không tìm thấy thêm dữ liệu metrics nào.")
                    break # Dừng vòng lặp while

                # Lặp qua các metric (VD: page_impressions, page_fans...)
                for metric in metrics_page:
                    metric_name = metric.get('name')
                    if not metric_name:
                        continue
                    
                    # Lặp qua các giá trị (ngày) trong metric đó
                    for value_entry in metric.get('values', []):
                        end_time_str = value_entry.get('end_time')
                        metric_value = value_entry.get('value')
                        if not end_time_str:
                            continue
                            
                        try:
                            # Lấy object date (VD: date(2025, 10, 6))
                            end_time_date = datetime.fromisoformat(end_time_str).date()
                        except ValueError:
                            logger.warning(f"Không thể parse ngày: {end_time_str}. Bỏ qua...")
                            continue

                        # 7. LOGIC KIỂM SOÁT (Giữ nguyên)
                        if start_date_obj <= end_time_date <= end_date_obj:
                            # Ngày này hợp lệ, tiến hành "pivot"
                            date_key = end_time_date.isoformat() # Dùng 'YYYY-MM-DD' làm key
                            
                            # Nếu chưa có "dòng" (record) nào cho ngày này, tạo mới
                            if date_key not in daily_data_pivot:
                                daily_data_pivot[date_key] = {
                                    "page_id": page_id,
                                    "date": date_key
                                }
                            
                            # Thêm "cột" (metric) vào "dòng" (ngày)
                            # VD: daily_data_pivot['2025-10-06']['page_impressions'] = 46773
                            daily_data_pivot[date_key][metric_name] = metric_value
                            
                        else:
                            # Ngày này nằm ngoài khoảng yêu cầu -> Dừng
                            logger.warning(f"Phát hiện dữ liệu ngày {end_time_date} "
                                           f"nằm ngoài khoảng yêu cầu ({start_date_obj} - {end_date_obj}). "
                                           "Dừng phân trang.")
                            keep_looping = False
                            break # Dừng vòng lặp 'for value_entry...'
                    
                    if not keep_looping:
                        break # Dừng vòng lặp 'for metric...'

                # 8. Lấy URL trang tiếp theo (Giữ nguyên)
                if keep_looping: 
                    paging_data = data.get('paging', {})
                    url = paging_data.get('next')
                    if not url:
                        logger.info("Hoàn tất phân trang (không còn 'next').")
                        break
                else:
                    logger.info("Dừng phân trang do phát hiện ngày nằm ngoài khoảng.")
                    url = None

            except requests.exceptions.RequestException as e:
                is_token_error = False
                if e.response is not None:
                    try:
                        error_data = e.response.json().get('error', {})
                        if error_data.get('code') == 190: # 190 is OAuthException
                            is_token_error = True
                    except requests.exceptions.JSONDecodeError:
                        pass # Không phải lỗi JSON
                
                if is_token_error:
                    logger.warning(f"Phát hiện lỗi Token (190) cho Page {page_id} (trong get_page_metrics_by_day).")
                    raise e # Ném lại lỗi để (database_manager) bắt
                
                # Lỗi request khác (ví dụ: 500, 404), chỉ log và break
                logger.error(f"Lỗi khi lấy Page Metrics (Trang {page_count}) cho Page {page_id}: {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break # Break the `while` loop
            
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break

        # --- THAY ĐỔI LOGIC: Bước 9 ---
        # 9. Trả về kết quả
        # Chuyển đổi dict pivot (keyed by date) thành một danh sách các "dòng"
        final_daily_list = list(daily_data_pivot.values())
        logger.info(f"Tổng hợp hoàn tất. Trả về {len(final_daily_list)} bản ghi (ngày).")
        return final_daily_list
        
    def get_posts_with_lifetime_insights(self, page_id: str, page_access_token: str,
                                         start_date: str, end_date: str,
                                         metrics_list: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Lấy các bài post được TẠO trong khoảng
        start_date và end_date. Lấy media, shares, comment_count, và metrics LIFETIME. Backend JS sẽ xử lý date_preset và input vào sau.
        
        Args:
            page_id (str): ID của Fanpage.
            page_access_token (str): Page Access Token của Fanpage đó.
            start_date (str): Ngày bắt đầu (YYYY-MM-DD).
            end_date (str): Ngày kết thúc (YYYY-MM-DD).
            metrics_list (Optional[List[str]]): Danh sách metric. Nếu None, dùng mặc định.
        """
        all_posts_data = []
        
        # 1. Xử lý metric (nếu không nhập, dùng mặc định của bạn)
        if not metrics_list:
            metrics_list = [
                'post_reactions_like_total',
                'post_impressions_unique',
                'post_clicks'
            ]
        metrics_str = ",".join(metrics_list)
        
        # 2. Xây dựng chuỗi fields
        base_fields = 'id,message,created_time,full_picture,shares,properties'
        insights_field = f'insights.metric({metrics_str})'
        comments_field = 'comments.summary(total_count)'
        
        fields_query = f"{base_fields},{insights_field},{comments_field}"
        
        # 3. Xây dựng endpoint và params
        url = f"{self.base_url}/{page_id}/posts"

        # Endpoint /posts yêu cầu 'since' < 'until'.
        # Chúng ta sẽ cộng 1 ngày vào 'end_date' để làm 'until_date'.
        try:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            until_date_obj = end_date_obj + relativedelta(days=1)
            until_date_str = until_date_obj.strftime('%Y-%m-%d')
        except ValueError:
            logger.error(f"Định dạng ngày '{end_date}' không hợp lệ. Sử dụng ngày gốc.")
            until_date_str = end_date # Fallback
        
        params = {
            'access_token': page_access_token,
            'since': start_date,
            'until': until_date_str,
            'fields': fields_query,
            'limit': 50 # Giảm limit một chút vì query này khá nặng
        }
        
        page_count = 0
        
        while url:
            try:
                page_count += 1
                response = requests.get(url, params=params if page_count == 1 else {})
                response.raise_for_status()
                data = response.json()

                posts_page = data.get('data', [])
                if not posts_page:
                    logger.info("Không tìm thấy thêm bài đăng nào trong khoảng này.")
                    break
                
                # 4. Xử lý (Parsing) dữ liệu chi tiết
                for post in posts_page:
                    properties_text = None
                    properties_list = post.get('properties', []) # Đây là một list
                    if properties_list and isinstance(properties_list, list) and len(properties_list) > 0:
                        # Lấy 'text' từ dict ĐẦU TIÊN trong list
                        properties_text = properties_list[0].get('text')
                    post_id = post.get('id')
                    original_url = post.get('full_picture')
                    final_picture_url = original_url
                    if original_url:
                        # Gọi hàm upload sang R2
                        final_picture_url = self.storage_manager.process_and_upload_image(
                            original_url, 
                            post_id
                        )
                    post_data = {
                        'post_id': post.get('id'),
                        'message': post.get('message', 'Không có nội dung text'),
                        'created_time': post.get('created_time'),
                        'full_picture_url': final_picture_url,
                        'shares_count': post.get('shares', {}).get('count', 0),
                        'properties': properties_text,
                        'fetch_range': f"{start_date}_to_{end_date}"
                    }
                    
                    # Lấy tổng số comment
                    post_data['comments_total_count'] = post.get('comments', {}).get('summary', {}).get('total_count', 0)
                    
                    # Lấy dữ liệu insights (chỉ quan tâm lifetime)
                    insights_data = post.get('insights', {}).get('data', [])
                    
                    for metric in insights_data:
                        metric_name = metric.get('name')
                        metric_period = metric.get('period')
                        
                        # Chỉ lấy metric 'lifetime' như bạn yêu cầu
                        if metric_period == 'lifetime' and metric_name in metrics_list:
                            metric_value = metric.get('values', [{}])[0].get('value', 0)
                            if metric_name == 'post_impressions_unique':
                                # API trả về 'post_impressions_unique'
                                # nhưng ta lưu là 'post_impressions' để CSDL không bị ảnh hưởng
                                post_data['post_impressions'] = metric_value
                            elif metric_name in metrics_list:
                                # Các metric khác (post_clicks, post_reactions_like_total)
                                post_data[metric_name] = metric_value
                    
                    # Gán giá trị 0 cho các metric không tìm thấy (để đảm bảo cột)
                    for m in metrics_list:
                        if m not in post_data:
                            post_data[m] = 0
                            
                    all_posts_data.append(post_data)

                logger.info(f"Đã lấy được {len(posts_page)} bài đăng (Tổng: {len(all_posts_data)}).")

                # 5. Xử lý phân trang (Pagination)
                paging_data = data.get('paging', {})
                if paging_data:
                    url = paging_data.get('next')
                else:
                    url = None
            
            except requests.exceptions.RequestException as e:
                is_token_error = False
                if e.response is not None:
                    try:
                        error_data = e.response.json().get('error', {})
                        if error_data.get('code') == 190: # 190 is OAuthException
                            is_token_error = True
                    except requests.exceptions.JSONDecodeError:
                        pass # Không phải lỗi JSON

                if is_token_error:
                    logger.warning(f"Phát hiện lỗi Token (190) cho Page {page_id} (trong get_posts_with_lifetime_insights).")
                    raise e # Ném lại lỗi để (database_manager) bắt

                # Lỗi request khác (ví dụ: 500, 404), chỉ log và break
                logger.error(f"Lỗi khi lấy Posts (Trang {page_count}) cho Page {page_id}: {e}")
                if e.response is not None:
                    logger.error(f"Response: {e.response.json()}")
                break # Break the `while` loop
            except Exception as e:
                logger.error(f"Lỗi không xác định: {e}")
                break
                
        logger.info(f"Hoàn tất! Lấy được tổng cộng {len(all_posts_data)} bài đăng.")
        return all_posts_data

def main():
    try:
        extractor = FacebookAdsExtractor()
        
        logger.info("--- BẮT ĐẦU DEBUG METRIC REACH ---")
        
        # 1. Lấy danh sách tài khoản quảng cáo
        accounts = extractor.get_all_ad_accounts()
        if not accounts:
            logger.error("Không tìm thấy tài khoản quảng cáo nào.")
            return

        # Lấy tài khoản đầu tiên để test (hoặc bạn có thể filter theo tên)
        target_account = accounts[4] 
        account_id = target_account['id']
        account_name = target_account['name']
        
        logger.info(f"Đang test với tài khoản: {account_name} ({account_id})")

        # 2. Thiết lập thời gian test (Ví dụ: 7 ngày qua)
        today = date.today()
        end_date_obj = today - relativedelta(days=1) # Hôm qua (để chắc chắn có data)
        start_date_obj = end_date_obj - relativedelta(days=6) # 7 ngày trước
        
        # 3. TEST 1: Lấy Reach của cả khoảng 7 ngày (Standard Way)
        logger.info(f"TEST 1: Lấy Reach tổng hợp 7 ngày ({start_date_obj} - {end_date_obj})...")
        reach_7d = extractor.get_total_metric(
            account_id=account_id,
            metric_name='reach',
            start_date=start_date_obj,
            end_date=end_date_obj
        )
        
        # 4. TEST 2: Chỉ lấy Reach của ngày cuối cùng (User Hypothesis)
        logger.info(f"TEST 2: Chỉ lấy Reach của ngày cuối ({end_date_obj})...")
        reach_1d = extractor.get_total_metric(
            account_id=account_id,
            metric_name='reach',
            start_date=end_date_obj,
            end_date=end_date_obj
        )

        # 5. Xuất kết quả để so sánh
        result_data = {
            'account': account_name,
            'debug_note': 'So sanh Reach 7 ngay vs Reach ngay cuoi cung',
            'TEST_1_Range_7_Days': {
                'period': f"{start_date_obj} to {end_date_obj}",
                'raw_data': reach_7d,
                'value': reach_7d[0].get('reach') if reach_7d else 0
            },
            'TEST_2_Single_Last_Day': {
                'period': f"{end_date_obj}",
                'raw_data': reach_1d,
                'value': reach_1d[0].get('reach') if reach_1d else 0
            }
        }
        
        filename = "debug_reach_comparison.json"
        extractor.save_to_json(data=result_data, filename=filename)
        
        logger.info(f"--- KẾT QUẢ ---")
        logger.info(f"Reach 7 ngày: {result_data['TEST_1_Range_7_Days']['value']}")
        logger.info(f"Reach ngày cuối: {result_data['TEST_2_Single_Last_Day']['value']}")
        logger.info(f"File chi tiết đã lưu tại: {filename}")
        
        if int(result_data['TEST_1_Range_7_Days']['value']) > int(result_data['TEST_2_Single_Last_Day']['value']):
            logger.info("KẾT LUẬN: Reach 7 ngày LỚN HƠN Reach ngày cuối -> Reach là unique deduplicated, không phải accumulative snapshot.")
        else:
            logger.info("KẾT LUẬN: Reach xấp xỉ nhau (hoặc data ít) -> Cần kiểm tra thêm.")

    except Exception as e:
        logger.error(f"Lỗi không mong muốn trong hàm main: {e}", exc_info=True)

if __name__ == "__main__":
    main()