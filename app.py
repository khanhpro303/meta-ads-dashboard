#!/usr/bin/env python3

import os
import json
import logging
import traceback
from datetime import datetime
import time

from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
import pandas as pd

from fbads_extract import FacebookAdsExtractor

# --- CẤU HÌNH CƠ BẢN ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# --- KHỞI TẠO FLASK APP ---
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-meta-ads-secret-key')

# --- CẤU HÌNH CACHE TRONG BỘ NHỚ (IN-MEMORY) ---
app_cache = {}
CACHE_DURATION_SECONDS = 3600  # Cache trong 1 giờ
logger.info(f"Sử dụng cache trong bộ nhớ với thời gian tồn tại là {CACHE_DURATION_SECONDS} giây.")

# LẤY DỮ LIỆU TÀI KHOẢN KHI KHỞI ĐỘNG APP ===
AD_ACCOUNTS_LIST = []
try:
    logger.info("Đang khởi tạo và lấy danh sách tài khoản quảng cáo khi khởi động...")
    startup_extractor = FacebookAdsExtractor()
    AD_ACCOUNTS_LIST = startup_extractor.get_all_ad_accounts()
    if AD_ACCOUNTS_LIST:
        logger.info(f"Đã lấy thành công {len(AD_ACCOUNTS_LIST)} tài khoản quảng cáo.")
    else:
        logger.warning("Không tìm thấy tài khoản quảng cáo nào khi khởi động.")
except Exception as e:
    logger.error(f"Lỗi nghiêm trọng khi lấy danh sách tài khoản lúc khởi động: {e}")
# ==========================================================

@app.route('/')
def index():
    """
    Render trang dashboard chính.
    """
    return render_template('index.html')

# === BỔ SUNG: ENDPOINT MỚI ĐỂ LẤY DANH SÁCH TÀI KHOẢN ===
@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    """
    Trả về danh sách tài khoản quảng cáo đã được lấy khi khởi động.
    """
    return jsonify(AD_ACCOUNTS_LIST)

@app.route('/api/campaigns', methods=['POST'])
def get_campaigns():
    """
    Lấy danh sách các chiến dịch cho một tài khoản quảng cáo cụ thể.
    """
    try:
        data = request.get_json()
        # === THAY ĐỔI: Nhận account_id từ frontend ===
        account_id = data.get('account_id')
        date_preset = data.get('date_preset')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        force_reload = data.get('force_reload', False)

        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400

        if date_preset:
            CACHE_KEY = f"campaign_list_{account_id}_{date_preset}"
        elif start_date and end_date:
            CACHE_KEY = f"campaign_list_{account_id}_{start_date}_{end_date}"
        else:
            return jsonify({'error': 'Thiếu tham số thời gian.'}), 400

        if not force_reload and CACHE_KEY in app_cache:
            cached_item = app_cache[CACHE_KEY]
            if time.time() - cached_item['timestamp'] < CACHE_DURATION_SECONDS:
                logger.info(f"Trả về danh sách chiến dịch từ cache cho key: {CACHE_KEY}")
                return jsonify(cached_item['data'])

        logger.info(f"Cache không có hoặc đã hết hạn. Đang lấy danh sách chiến dịch từ API cho key: {CACHE_KEY}")
        extractor = FacebookAdsExtractor()

        all_campaigns = extractor.get_campaigns_for_account(
            account_id=account_id,
            date_preset=date_preset,
            start_date=start_date,
            end_date=end_date
        )
        if not all_campaigns:
            all_campaigns = []
        
        all_campaigns.sort(key=lambda x: x.get('name', ''))

        app_cache[CACHE_KEY] = {
            'data': all_campaigns,
            'timestamp': time.time()
        }
        logger.info(f"Đã lưu danh sách chiến dịch vào cache cho key: {CACHE_KEY}")

        return jsonify(all_campaigns)

    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách chiến dịch: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500


@app.route('/api/adsets', methods=['POST'])
def get_adsets():
    """
    Lấy danh sách các nhóm quảng cáo (adsets) cho một hoặc nhiều chiến dịch cụ thể.
    """
    try:
        data = request.get_json()
        account_id = data.get('account_id')
        campaign_ids = data.get('campaign_ids')
        date_preset = data.get('date_preset')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        force_reload = data.get('force_reload', False)

        if not account_id or not campaign_ids:
            return jsonify({'error': 'Thiếu account_id hoặc campaign_ids.'}), 400

        # Tạo một cache key duy nhất dựa trên các bộ lọc
        campaign_key_part = '_'.join(sorted(campaign_ids))
        if date_preset:
            CACHE_KEY = f"adset_list_{account_id}_{campaign_key_part}_{date_preset}"
        elif start_date and end_date:
            CACHE_KEY = f"adset_list_{account_id}_{campaign_key_part}_{start_date}_{end_date}"
        else:
            return jsonify({'error': 'Thiếu tham số thời gian.'}), 400

        # Kiểm tra cache
        if not force_reload and CACHE_KEY in app_cache:
            cached_item = app_cache[CACHE_KEY]
            if time.time() - cached_item['timestamp'] < CACHE_DURATION_SECONDS:
                logger.info(f"Trả về danh sách adset từ cache cho key: {CACHE_KEY}")
                return jsonify(cached_item['data'])

        logger.info(f"Cache không có hoặc đã hết hạn. Đang lấy danh sách adset từ API cho key: {CACHE_KEY}")
        extractor = FacebookAdsExtractor()

        # Gọi hàm để lấy adsets từ các chiến dịch đã chọn
        # Giả sử bạn có một hàm get_adsets_for_campaigns trong fbads_extract.py
        all_adsets = extractor.get_adsets_for_campaigns(
            account_id=account_id,
            campaign_ids=campaign_ids,
            date_preset=date_preset,
            start_date=start_date,
            end_date=end_date
        )
        if not all_adsets:
            all_adsets = []
        
        all_adsets.sort(key=lambda x: x.get('name', ''))

        # Lưu vào cache
        app_cache[CACHE_KEY] = {
            'data': all_adsets,
            'timestamp': time.time()
        }
        logger.info(f"Đã lưu danh sách adset vào cache cho key: {CACHE_KEY}")

        return jsonify(all_adsets)

    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách adset: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500


@app.route('/api/dashboard_data', methods=['POST'])
def get_dashboard_data():
    """
    Lấy dữ liệu dashboard cho một tài khoản và các chiến dịch cụ thể.
    """
    try:
        data = request.get_json()
        # === THAY ĐỔI: Nhận account_id từ frontend ===
        account_id = data.get('account_id')
        date_preset = data.get('date_preset')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        campaign_ids = data.get('campaign_ids')
        force_reload = data.get('force_reload', False)

        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400

        # === THAY ĐỔI: Cập nhật CACHE_KEY để bao gồm account_id ===
        if date_preset:
            CACHE_KEY = f"dashboard_data_{account_id}_{date_preset}"
        elif start_date and end_date:
            CACHE_KEY = f"dashboard_data_{account_id}_{start_date}_{end_date}"
        else:
            return jsonify({'error': 'Thiếu tham số thời gian.'}), 400
        
        if campaign_ids:
            CACHE_KEY += f"_{'_'.join(sorted(campaign_ids))}"

        if not force_reload and CACHE_KEY in app_cache:
            cached_item = app_cache[CACHE_KEY]
            if time.time() - cached_item['timestamp'] < CACHE_DURATION_SECONDS:
                logger.info(f"Trả về dữ liệu dashboard từ cache cho key: {CACHE_KEY}")
                return jsonify(cached_item['data'])

        logger.info(f"Cache không có hoặc đã hết hạn. Đang lấy dữ liệu dashboard từ API cho key: {CACHE_KEY}")
        extractor = FacebookAdsExtractor()
        
        # === THAY ĐỔI: Chỉ gọi API cho một tài khoản duy nhất ===
        all_insights = extractor.get_insights(
            account_id=account_id,
            campaign_id=campaign_ids,
            date_preset=date_preset,
            start_date=start_date,
            end_date=end_date
        )
        
        if not all_insights:
            return jsonify({'scorecards': {}, 'campaign_table': []})

        df = pd.DataFrame(all_insights)
        numeric_cols = ['impressions', 'clicks', 'spend']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=numeric_cols)

        total_spend = df['spend'].sum()
        total_impressions = df['impressions'].sum()
        total_clicks = df['clicks'].sum()
        
        cpm = (total_spend / total_impressions) * 1000 if total_impressions > 0 else 0
        cpc = total_spend / total_clicks if total_clicks > 0 else 0
        ctr = (total_clicks / total_impressions) * 100 if total_impressions > 0 else 0

        dashboard_data = {
            'scorecards': {
                'total_spend': float(total_spend),
                'cpm': float(cpm),
                'cpc': float(cpc),
                'ctr': float(ctr)
            },
            'campaign_table': df.to_dict(orient='records')
        }

        app_cache[CACHE_KEY] = {
            'data': dashboard_data,
            'timestamp': time.time()
        }
        logger.info(f"Đã lưu dữ liệu dashboard vào cache cho key: {CACHE_KEY}")

        return jsonify(dashboard_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu dashboard: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500

# --- CHẠY APP ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)
