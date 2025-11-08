#!/usr/bin/env python3

import os
import json
import logging
import shutil
from datetime import datetime, date
from typing import Dict, List, Any
import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FacebookAdsExtractor:
    def __init__(self):
        load_dotenv()
        self.access_token = os.getenv("SECRET_KEY")
        self.base_url = os.getenv("BASE_URL", "https://graph.facebook.com/v24.0/")
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

                accounts_page = data.get('adaccounts', {}).get('data', [])
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
    
def main():
    try:
        extractor = FacebookAdsExtractor()
        
        if not extractor.test_connection():
            logger.warning("Không thể kết nối đến Facebook API. Tạo dữ liệu mẫu...")
            sample_data = extractor.generate_sample_data()
            extractor.save_to_json(sample_data, "ads_data.json")
            return
        
        logger.info("Bắt đầu trích xuất dữ liệu...")
        data = extractor.extract_all_data("2023-01-01")
        
        if not data.get('campaigns') or len(data['campaigns']) == 0:
            logger.warning("Không tìm thấy chiến dịch nào trong tài khoản quảng cáo.")
            logger.info("Có thể do:")
            logger.info("1. Tài khoản chưa có chiến dịch quảng cáo nào")
            logger.info("2. Thiếu quyền 'ads_read' (cần cấp quyền này)")
            logger.info("3. Chiến dịch đã bị xóa hoặc ẩn")
            logger.info("Tạo dữ liệu mẫu để demo dashboard...")
            
            sample_data = extractor.generate_sample_data()
            if extractor.save_to_json(sample_data, "ads_data.json"):
                logger.info("Đã tạo dữ liệu mẫu thành công!")
            else:
                logger.error("Lỗi khi tạo dữ liệu mẫu!")
        else:
            if extractor.save_to_json(data, "ads_data.json"):
                logger.info("Trích xuất dữ liệu hoàn tất thành công!")
            else:
                logger.error("Lỗi khi lưu dữ liệu!")
            
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")

if __name__ == "__main__":
    main()