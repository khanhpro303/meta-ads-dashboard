#!/usr/bin/env python3

import os
import logging
from datetime import datetime, timedelta

from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
from sqlalchemy import func, select, and_

# Import các lớp từ database_manager
from database_manager import DatabaseManager, DimAdAccount, DimCampaign, DimAdset, DimAd, FactPerformance, DimDate, DimPlatform, DimPlacement

# --- CẤU HÌNH CƠ BẢN ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# --- KHỞI TẠO FLASK APP VÀ DATABASE MANAGER ---
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-meta-ads-secret-key')
db_manager = DatabaseManager()
# Tạo tất cả các bảng nếu chưa tồn tại
db_manager.create_all_tables()

# ======================================================================
# API ENDPOINTS - TRUY VẤN TRỰC TIẾP TỪ DATABASE
# ======================================================================

@app.route('/')
def index():
    """
    Render trang dashboard chính.
    """
    return render_template('index.html')

@app.route('/api/refresh', methods=['POST'])
def refresh_data():
    """
    Kích hoạt quy trình làm mới dữ liệu từ Meta Ads API vào database.
    """
    # Lấy tham số từ request nếu có
    data = request.get_json()
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    date_preset = data.get('date_preset')
    try:
        db_manager.refresh_data(start_date=start_date, end_date=end_date, date_preset=date_preset)
        return jsonify({'message': 'Dữ liệu đã được làm mới thành công.'})
    except Exception as e:
        logger.error(f"Lỗi khi làm mới dữ liệu: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500

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

@app.route('/api/chart_data', methods=['POST'])
def get_chart_data():
    """
    Lấy dữ liệu đã được nhóm theo ngày để vẽ biểu đồ đường.
    Trục X: Ngày
    Line 1: Spend
    Line 2: Impressions
    """
    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')

        if not start_date_str or not end_date_str:
            return jsonify({'error': 'Thiếu start_date hoặc end_date.'}), 400

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

        # --- Xây dựng câu truy vấn động ---
        query = select(
            DimDate.full_date,
            func.sum(FactPerformance.spend).label('daily_spend'),
            func.sum(FactPerformance.impressions).label('daily_impressions')
        ).join(
            FactPerformance, FactPerformance.date_key == DimDate.date_key
        ).filter(
            DimDate.full_date.between(start_date, end_date)
        )

        # Áp dụng các bộ lọc ID nếu có
        filters = []
        if data.get('campaign_ids'):
            filters.append(FactPerformance.campaign_id.in_(data['campaign_ids']))
        if data.get('adset_ids'):
            filters.append(FactPerformance.adset_id.in_(data['adset_ids']))
        if data.get('ad_ids'):
            filters.append(FactPerformance.ad_id.in_(data['ad_ids']))
        
        if filters:
            query = query.where(and_(*filters))

        # Nhóm theo ngày và sắp xếp
        query = query.group_by(DimDate.full_date).order_by(DimDate.full_date)

        # Thực thi câu truy vấn
        results = session.execute(query).all()

        # --- Xử lý và định dạng dữ liệu cho Chart.js ---
        
        # Tạo một map để tra cứu kết quả nhanh chóng
        results_map = {res.full_date: res for res in results}
        
        labels = []
        spend_data = []
        impressions_data = []
        
        # Lặp qua tất cả các ngày trong khoảng thời gian để đảm bảo không có ngày nào bị thiếu
        current_date = start_date
        while current_date <= end_date:
            labels.append(current_date.strftime('%Y-%m-%d'))
            
            if current_date in results_map:
                # Nếu có dữ liệu cho ngày này, lấy nó
                spend_data.append(results_map[current_date].daily_spend or 0)
                impressions_data.append(results_map[current_date].daily_impressions or 0)
            else:
                # Nếu không có dữ liệu, điền vào số 0
                spend_data.append(0)
                impressions_data.append(0)
            
            current_date += timedelta(days=1)

        # Tạo cấu trúc dữ liệu cuối cùng cho Chart.js
        chartjs_data = {
            'labels': labels,
            'datasets': [
                {
                    'label': 'Chi phí (Spend)',
                    'data': spend_data,
                    'borderColor': 'rgb(255, 99, 132)',
                    'backgroundColor': 'rgba(255, 99, 132, 0.5)',
                    'yAxisID': 'y', # Gán vào trục y chính (bên trái)
                },
                {
                    'label': 'Lượt hiển thị (Impressions)',
                    'data': impressions_data,
                    'borderColor': 'rgb(54, 162, 235)',
                    'backgroundColor': 'rgba(54, 162, 235, 0.5)',
                    'yAxisID': 'y1', # Gán vào trục y phụ (bên phải)
                }
            ]
        }

        return jsonify(chartjs_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu biểu đồ: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()
