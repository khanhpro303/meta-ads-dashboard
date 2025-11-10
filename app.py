#!/usr/bin/env python3

import os
import logging
from datetime import datetime

from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
from sqlalchemy import func, select, and_

# Import các lớp từ database_manager
from database_manager import DatabaseManager, DimAdAccount, DimCampaign, DimAdset, DimAd, FactPerformance, DimDate

# --- CẤU HÌNH CƠ BẢN ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# --- KHỞI TẠO FLASK APP VÀ DATABASE MANAGER ---
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-meta-ads-secret-key')
db_manager = DatabaseManager()

# ======================================================================
# API ENDPOINTS - TRUY VẤN TRỰC TIẾP TỪ DATABASE
# ======================================================================

@app.route('/')
def index():
    """
    Render trang dashboard chính.
    """
    return render_template('index.html')

@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    """
    Lấy danh sách tài khoản quảng cáo từ bảng DimAdAccount.
    """
    session = db_manager.SessionLocal()
    try:
        accounts = session.query(DimAdAccount.ad_account_id, DimAdAccount.name).order_by(DimAdAccount.name).all()
        # Chuyển đổi kết quả thành định dạng JSON mong muốn
        accounts_list = [{'id': acc.ad_account_id, 'name': acc.name} for acc in accounts]
        return jsonify(accounts_list)
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách tài khoản từ DB: {e}")
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/campaigns', methods=['POST'])
def get_campaigns():
    """
    Lấy danh sách chiến dịch từ bảng DimCampaign dựa trên account_id.
    """
    data = request.get_json()
    account_id = data.get('account_id')
    if not account_id:
        return jsonify({'error': 'Thiếu account_id.'}), 400

    session = db_manager.SessionLocal()
    try:
        campaigns = session.query(DimCampaign.campaign_id, DimCampaign.name)\
            .filter(DimCampaign.ad_account_id == account_id)\
            .order_by(DimCampaign.name).all()
        
        campaigns_list = [{'campaign_id': c.campaign_id, 'name': c.name} for c in campaigns]
        return jsonify(campaigns_list)
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách chiến dịch từ DB: {e}")
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/adsets', methods=['POST'])
def get_adsets():
    """
    Lấy danh sách adset từ bảng DimAdset dựa trên danh sách campaign_ids.
    """
    data = request.get_json()
    campaign_ids = data.get('campaign_ids')
    if not campaign_ids:
        return jsonify([]) # Trả về mảng rỗng nếu không có campaign nào được chọn

    session = db_manager.SessionLocal()
    try:
        # Lấy các adset_id duy nhất từ bảng Fact, sau đó join với DimAdset
        adset_query = select(DimAdset.adset_id, DimAdset.name)\
            .join(FactPerformance, FactPerformance.adset_id == DimAdset.adset_id)\
            .filter(FactPerformance.campaign_id.in_(campaign_ids))\
            .distinct()\
            .order_by(DimAdset.name)
            
        adsets = session.execute(adset_query).all()
        adsets_list = [{'adset_id': a.adset_id, 'name': a.name} for a in adsets]
        return jsonify(adsets_list)
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách adset từ DB: {e}")
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/ads', methods=['POST'])
def get_ads():
    """
    Lấy danh sách ads từ bảng DimAd dựa trên danh sách adset_ids.
    """
    data = request.get_json()
    adset_ids = data.get('adset_ids')
    if not adset_ids:
        return jsonify([])

    session = db_manager.SessionLocal()
    try:
        ad_query = select(DimAd.ad_id, DimAd.name)\
            .join(FactPerformance, FactPerformance.ad_id == DimAd.ad_id)\
            .filter(FactPerformance.adset_id.in_(adset_ids))\
            .distinct()\
            .order_by(DimAd.name)

        ads = session.execute(ad_query).all()
        ads_list = [{'ad_id': a.ad_id, 'name': a.name} for a in ads]
        return jsonify(ads_list)
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách quảng cáo từ DB: {e}")
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/overview_data', methods=['POST'])
def get_overview_data():
    """
    Tổng hợp dữ liệu scorecard và dữ liệu chi tiết bằng cách truy vấn trực tiếp từ DB.
    """
    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        
        # --- Xây dựng câu truy vấn động dựa trên bộ lọc ---
        query = select(
            func.sum(FactPerformance.spend).label('total_spend'),
            func.sum(FactPerformance.impressions).label('total_impressions'),
            func.sum(FactPerformance.clicks).label('total_clicks'),
            func.sum(FactPerformance.reach).label('total_reach'),
            func.sum(FactPerformance.messages_started).label('total_messages'),
            func.sum(FactPerformance.purchases).label('total_purchases'),
            func.sum(FactPerformance.purchase_value).label('total_purchase_value'),
            func.sum(FactPerformance.post_engagement).label('total_post_engagement'),
            func.sum(FactPerformance.link_click).label('total_link_click')
        ).join(DimDate, FactPerformance.date_key == DimDate.date_key)

        # Áp dụng bộ lọc thời gian
        if start_date_str and end_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            query = query.filter(DimDate.full_date.between(start_date, end_date))

        # Áp dụng các bộ lọc ID
        filters = []
        if data.get('campaign_ids'):
            filters.append(FactPerformance.campaign_id.in_(data['campaign_ids']))
        if data.get('adset_ids'):
            filters.append(FactPerformance.adset_id.in_(data['adset_ids']))
        if data.get('ad_ids'):
            filters.append(FactPerformance.ad_id.in_(data['ad_ids']))
        
        if filters:
            query = query.where(and_(*filters))

        # Thực thi câu truy vấn
        result = session.execute(query).first()

        if not result or result.total_impressions is None:
            # Trả về cấu trúc rỗng nếu không có kết quả
            return jsonify({
                'scorecards': {
                    'total_spend': 0, 'total_impressions': 0, 'ctr': 0, 
                    'total_messages': 0, 'total_reach': 0, 'total_post_engagement': 0,
                    'total_link_click': 0, 'total_purchases': 0, 'total_purchase_value': 0
                }
            })

        # Tính toán các chỉ số phái sinh
        total_impressions = result.total_impressions or 0
        total_clicks = result.total_clicks or 0
        ctr = (total_clicks / total_impressions) * 100 if total_impressions > 0 else 0

        # Tạo cấu trúc dữ liệu trả về
        response_data = {
            'scorecards': {
                'total_spend': result.total_spend or 0,
                'total_impressions': total_impressions,
                'ctr': ctr,
                'total_messages': result.total_messages or 0,
                'total_reach': result.total_reach or 0,
                'total_post_engagement': result.total_post_engagement or 0,
                'total_link_click': result.total_link_click or 0,
                'total_purchases': result.total_purchases or 0,
                'total_purchase_value': result.total_purchase_value or 0
            }
            # Bạn có thể thêm một câu truy vấn khác ở đây để lấy 'raw_data' nếu cần
        }

        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu tổng quan từ DB: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

# --- CHẠY APP ---
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)