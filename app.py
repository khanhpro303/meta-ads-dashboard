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
        
        all_campaigns.sort(key=lambda x: x.get('campaign_name', ''))

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
        
        all_adsets.sort(key=lambda x: x.get('adset_name', ''))

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


@app.route('/api/ads', methods=['POST'])
def get_ads():
    """
    Lấy danh sách các quảng cáo (ads) cho một hoặc nhiều nhóm quảng cáo cụ thể.
    """
    try:
        data = request.get_json()
        account_id = data.get('account_id')
        adset_ids = data.get('adset_ids')
        date_preset = data.get('date_preset')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        force_reload = data.get('force_reload', False)

        if not account_id or not adset_ids:
            return jsonify({'error': 'Thiếu account_id hoặc adset_ids.'}), 400

        # Tạo một cache key duy nhất dựa trên các bộ lọc
        adset_key_part = '_'.join(sorted(adset_ids))
        if date_preset:
            CACHE_KEY = f"ad_list_{account_id}_{adset_key_part}_{date_preset}"
        elif start_date and end_date:
            CACHE_KEY = f"ad_list_{account_id}_{adset_key_part}_{start_date}_{end_date}"
        else:
            return jsonify({'error': 'Thiếu tham số thời gian.'}), 400

        # Kiểm tra cache
        if not force_reload and CACHE_KEY in app_cache:
            cached_item = app_cache[CACHE_KEY]
            if time.time() - cached_item['timestamp'] < CACHE_DURATION_SECONDS:
                logger.info(f"Trả về danh sách quảng cáo từ cache cho key: {CACHE_KEY}")
                return jsonify(cached_item['data'])

        logger.info(f"Cache không có hoặc đã hết hạn. Đang lấy danh sách quảng cáo từ API cho key: {CACHE_KEY}")
        extractor = FacebookAdsExtractor()

        all_ads = extractor.get_ads_for_adsets(
            account_id=account_id,
            adset_ids=adset_ids,
            date_preset=date_preset,
            start_date=start_date,
            end_date=end_date
        )
        if not all_ads:
            all_ads = []
        
        all_ads.sort(key=lambda x: x.get('ad_name', ''))

        # Lưu vào cache
        app_cache[CACHE_KEY] = {
            'data': all_ads,
            'timestamp': time.time()
        }
        logger.info(f"Đã lưu danh sách quảng cáo vào cache cho key: {CACHE_KEY}")

        return jsonify(all_ads)

    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách quảng cáo: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500


@app.route('/api/overview_data', methods=['POST'])
def get_overview_data():
    """
    Lấy và xử lý dữ liệu tổng hợp cho trang Dashboard Tổng quan.
    """
    try:
        data = request.get_json()
        account_id = data.get('account_id')
        campaign_ids = data.get('campaign_ids')
        adset_ids = data.get('adset_ids')
        ad_ids = data.get('ad_ids')
        date_preset = data.get('date_preset')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        force_reload = data.get('force_reload', False)

        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400

        # --- Tạo Cache Key phức tạp hơn để chứa tất cả bộ lọc ---
        key_parts = [account_id]
        if date_preset: key_parts.append(date_preset)
        if start_date and end_date: key_parts.extend([start_date, end_date])
        if campaign_ids: key_parts.append('c_' + '_'.join(sorted(campaign_ids)))
        if adset_ids: key_parts.append('as_' + '_'.join(sorted(adset_ids)))
        if ad_ids: key_parts.append('a_' + '_'.join(sorted(ad_ids)))
        
        CACHE_KEY = "overview_" + "_".join(key_parts)

        # --- Kiểm tra cache ---
        if not force_reload and CACHE_KEY in app_cache:
            cached_item = app_cache[CACHE_KEY]
            if time.time() - cached_item['timestamp'] < CACHE_DURATION_SECONDS:
                logger.info(f"Trả về dữ liệu tổng quan từ cache cho key: {CACHE_KEY}")
                return jsonify(cached_item['data'])

        logger.info(f"Cache không có hoặc đã hết hạn. Đang lấy dữ liệu tổng quan từ API cho key: {CACHE_KEY}")
        extractor = FacebookAdsExtractor()

        # --- Gọi API để lấy dữ liệu thô ---
        raw_insights = extractor.get_insights(
            account_id=account_id,
            campaign_id=campaign_ids,
            adset_id=adset_ids,
            ad_id=ad_ids,
            date_preset=date_preset,
            start_date=start_date,
            end_date=end_date
        )

        # === THAY ĐỔI: Cập nhật logic xử lý và tổng hợp dữ liệu ===
        if not raw_insights:
            # Trả về cấu trúc scorecard rỗng nếu không có dữ liệu
            return jsonify({
                'scorecards': {
                    'total_spend': 0, 'total_impressions': 0, 'ctr': 0, 
                    'total_messages': 0, 'total_reach': 0, 'total_post_engagement': 0,
                    'total_link_click': 0, 'total_purchases': 0, 'total_purchase_value': 0
                },
                'raw_data': []
            })

        # --- Xử lý và tổng hợp dữ liệu ---
        total_spend = 0
        total_impressions = 0
        total_clicks = 0
        total_reach = 0
        total_messages = 0
        total_purchases = 0
        total_purchase_value = 0
        total_post_engagement = 0
        total_link_click = 0

        for item in raw_insights:
            total_spend += float(item.get('spend', 0))
            total_impressions += int(item.get('impressions', 0))
            total_clicks += int(item.get('clicks', 0))
            total_reach += int(item.get('reach', 0))
            
            # Xử lý trường 'actions' để lấy số lượng
            if 'actions' in item:
                for action in item['actions']:
                    action_type = action.get('action_type')
                    action_value = int(action.get('value', 0))
                    
                    if action_type == 'onsite_conversion.messaging_conversation_started_7d':
                        total_messages += action_value
                    elif action_type == 'onsite_conversion.purchase':
                        total_purchases += action_value
                    elif action_type == 'post_engagement':
                        total_post_engagement += action_value
                    elif action_type == 'link_click':
                        total_link_click += action_value

            # Xử lý trường 'action_values' để lấy giá trị chuyển đổi
            if 'action_values' in item:
                for action in item['action_values']:
                    if action.get('action_type') == 'onsite_conversion.purchase':
                        total_purchase_value += float(action.get('value', 0))

        # Tính toán các chỉ số phái sinh
        ctr = (total_clicks / total_impressions) * 100 if total_impressions > 0 else 0
        
        # --- Tạo cấu trúc dữ liệu trả về ---
        response_data = {
            'scorecards': {
                'total_spend': total_spend,
                'total_impressions': total_impressions,
                'ctr': ctr,
                'total_messages': total_messages,
                'total_reach': total_reach,
                'total_post_engagement': total_post_engagement,
                'total_link_click': total_link_click,
                'total_purchases': total_purchases,
                'total_purchase_value': total_purchase_value
            },
            'raw_data': raw_insights
        }

        # --- Lưu vào cache ---
        app_cache[CACHE_KEY] = {
            'data': response_data,
            'timestamp': time.time()
        }
        logger.info(f"Đã lưu dữ liệu tổng quan vào cache cho key: {CACHE_KEY}")

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu tổng quan: {e}")
        traceback.print_exc()
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500