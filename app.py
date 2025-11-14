#!/usr/bin/env python3

import os
import logging
from datetime import datetime, timedelta

from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
from matplotlib.dates import relativedelta
from sqlalchemy import func, select, and_, case

# Import các lớp từ database_manager
from database_manager import DatabaseManager, DimAdAccount, DimCampaign, DimAdset, DimAd, FactPerformancePlatform, DimDate, DimPlatform, DimPlacement, FactPerformanceDemographic

DATE_PRESET = ['today', 'yesterday', 'this_month', 'last_month', 'this_quarter', 'maximum', 'data_maximum', 'last_3d', 'last_7d', 'last_14d', 'last_28d', 'last_30d', 'last_90d', 'last_week_mon_sun', 'last_week_sun_sat', 'last_quarter', 'last_year', 'this_week_mon_today', 'this_week_sun_today', 'this_year']
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
# HELPER FUNCTION - XỬ LÝ DATE PRESET
# ======================================================================
def _calculate_date_range(date_preset: str, today: datetime.date = None) -> tuple[datetime.date, datetime.date]:
    """
    Tính toán start_date và end_date dựa trên một date_preset.
    Trả về một tuple (start_date, end_date).
    """
    if today is None:
        today = datetime.today().date()

    start_date, end_date = None, None

    if date_preset == 'today':
        start_date = end_date = today
    elif date_preset == 'yesterday':
        start_date = end_date = today - relativedelta(days=1)
    elif date_preset in ['last_3d', 'last_7d', 'last_14d', 'last_28d', 'last_30d', 'last_90d']:
        days = int(date_preset.replace('last_', '').replace('d', ''))
        start_date = today - relativedelta(days=days - 1)
        end_date = today
    elif date_preset == 'this_week_mon_today':
        start_date = today - relativedelta(days=today.weekday())
        end_date = today
    elif date_preset == 'this_week_sun_today':
        start_date = today - relativedelta(days=(today.weekday() + 1) % 7)
        end_date = today
    elif date_preset == 'this_month':
        start_date = today.replace(day=1)
        end_date = today
    elif date_preset == 'this_quarter':
        start_month = (today.month - 1) // 3 * 3 + 1
        start_date = today.replace(month=start_month, day=1)
        end_date = today
    elif date_preset == 'this_year':
        start_date = today.replace(month=1, day=1)
        end_date = today
    elif date_preset == 'last_week_mon_sun':
        end_of_last_week = today - relativedelta(days=today.weekday() + 1)
        start_of_last_week = end_of_last_week - relativedelta(days=6)
        start_date, end_date = start_of_last_week, end_of_last_week
    elif date_preset == 'last_week_sun_sat':
        end_of_last_week = today - relativedelta(days=(today.weekday() + 1) % 7 + 1)
        start_of_last_week = end_of_last_week - relativedelta(days=6)
        start_date, end_date = start_of_last_week, end_of_last_week
    elif date_preset == 'last_month':
        first_day_this_month = today.replace(day=1)
        end_date = first_day_this_month - relativedelta(days=1)
        start_date = end_date.replace(day=1)
    elif date_preset == 'last_quarter':
        current_quarter_start_month = (today.month - 1) // 3 * 3 + 1
        first_day_this_quarter = today.replace(month=current_quarter_start_month, day=1)
        end_date = first_day_this_quarter - relativedelta(days=1)
        last_quarter_start_month = (end_date.month - 1) // 3 * 3 + 1
        start_date = end_date.replace(month=last_quarter_start_month, day=1)
    elif date_preset == 'last_year':
        first_day_this_year = today.replace(month=1, day=1)
        end_date = first_day_this_year - relativedelta(days=1)
        start_date = end_date.replace(month=1, day=1)
    elif date_preset in ['maximum', 'data_maximum']:
        # Trả về None để không áp dụng bộ lọc ngày
        return None, None
    
    return start_date, end_date

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
    Kích hoạt quy trình làm mới dữ liệu (ETL) từ Meta Ads API vào database.
    """
    try:
        data = request.get_json()
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')

        # Xác định ngày bắt đầu và kết thúc
        start_date, end_date = None, None
        if date_preset and date_preset in DATE_PRESET:
            logger.info(f"Yêu cầu tải lại dữ liệu theo date_preset: {date_preset}")
            today = datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date()
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=today)
        elif start_date_input and end_date_input:
            logger.info(f"Yêu cầu tải lại dữ liệu theo khoảng thời gian: {start_date_input} - {end_date_input}")
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            # Mặc định an toàn nếu không có gì được gửi
            end_date = datetime.today().date()
            start_date = end_date.replace(day=1)

        db_manager.refresh_data(
            start_date=start_date.strftime('%Y-%m-%d'), 
            end_date=end_date.strftime('%Y-%m-%d'), 
            date_preset=date_preset
        )
        
        return jsonify({'message': 'Dữ liệu đã được làm mới thành công.'})
    
    except Exception as e:
        logger.error(f"Lỗi khi làm mới dữ liệu: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    """
    Lấy danh sách tài khoản quảng cáo từ bảng DimAdAccount.
    """
    session = db_manager.SessionLocal()
    try:
        accounts = session.query(DimAdAccount.ad_account_id, DimAdAccount.name).order_by(DimAdAccount.name).all()
        # Chuyển đổi kết quả thành định dạng JSON mong muốn
        accounts_list = [{'id': acc.ad_account_id, 'name': acc.name} for acc in accounts if acc.name not in ['Nguyen Xuan Trang', 'Lâm Khải']]
        return jsonify(accounts_list)
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách tài khoản từ DB: {e}")
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/campaigns', methods=['POST'])
def get_campaigns():
    """
    Lấy danh sách chiến dịch từ bảng DimCampaign dựa trên account_id và filter theo ngày được chọn.
    created_time trong khoảng date from và date to.
    """
    data = request.get_json()
    account_id = data.get('account_id')
    if not account_id:
        return jsonify({'error': 'Thiếu account_id.'}), 400
    start_date_input = data.get('start_date')
    end_date_input = data.get('end_date')
    date_preset = data.get('date_preset')
    start_date, end_date = None, None
    # Logic xử lý date_preset
    if date_preset and date_preset in DATE_PRESET:
        start_date, end_date = _calculate_date_range(date_preset=date_preset, today=datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else datetime.today().date())
    elif start_date_input and end_date_input:
        start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
    else:
        end_date = datetime.today().date()
        start_date = end_date.replace(day=1)

    session = db_manager.SessionLocal()
    try:
        campaigns = session.query(DimCampaign.campaign_id, DimCampaign.name)\
            .filter(DimCampaign.ad_account_id == account_id)\
            .filter(DimCampaign.start_time <= end_date)\
            .filter(DimCampaign.stop_time >= start_date)\
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
    Lấy danh sách adset từ bảng DimAdset dựa trên danh sách campaign_ids và filter theo ngày được chọn.
    created_time trong khoảng date from và date to.
    """
    data = request.get_json()
    campaign_ids = data.get('campaign_ids')
    if not campaign_ids:
        return jsonify({'error': 'Thiếu campaign_ids.'}), 400
    start_date_input = data.get('start_date')
    end_date_input = data.get('end_date')
    date_preset = data.get('date_preset')
    # Logic xử lý date_preset
    if date_preset and date_preset in DATE_PRESET:
        start_date, end_date = _calculate_date_range(date_preset=date_preset, today=datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date())
    elif start_date_input and end_date_input:
        start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
    else:
        end_date = datetime.today().date()
        start_date = end_date.replace(day=1)
    if not campaign_ids:
        return jsonify([]) # Trả về mảng rỗng nếu không có campaign nào được chọn

    session = db_manager.SessionLocal()
    try:
        # Lấy thông tin adset từ bảng DimAdset
        adset_query = select(DimAdset.adset_id, DimAdset.name)\
            .filter(DimAdset.campaign_id.in_(campaign_ids))\
            .filter(DimAdset.start_time <= end_date)\
            .filter(DimAdset.stop_time >= start_date)\
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
    Lấy danh sách ads từ bảng DimAd dựa trên danh sách adset_ids được chọn và filter theo ngày được chọn.
    created_time trong khoảng date from và date to.
    """
    data = request.get_json()
    adset_ids = data.get('adset_ids')
    start_date_input = data.get('start_date')
    end_date_input = data.get('end_date')
    date_preset = data.get('date_preset')
    # Logic xử lý date_preset
    if date_preset and date_preset in DATE_PRESET:
        start_date, end_date = _calculate_date_range(date_preset=date_preset, today=datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date())
    elif start_date_input and end_date_input:
        start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
    else:
        end_date = datetime.today().date()
        start_date = end_date.replace(day=1)
    if not adset_ids:
        return jsonify([])

    session = db_manager.SessionLocal()
    try:
        ad_query = select(DimAd.ad_id, DimAd.name)\
            .filter(DimAd.adset_id.in_(adset_ids))\
            .filter(DimAd.ad_schedule_start_time <= end_date)\
            .filter(DimAd.ad_schedule_end_time >= start_date)\
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
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        # Logic xử lý date_preset
        start_date, end_date = None, None
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date())
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date() if start_date_input else None
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else None
        else:
            end_date = datetime.today().date()
            start_date = end_date.replace(day=1)

        # --- Xây dựng câu truy vấn động dựa trên bộ lọc ---
        query = select(
            func.sum(FactPerformancePlatform.spend).label('total_spend'),
            func.sum(FactPerformancePlatform.impressions).label('total_impressions'),
            func.sum(FactPerformancePlatform.clicks).label('total_clicks'),
            func.avg(FactPerformancePlatform.ctr).label('avg_ctr'),
            func.avg(FactPerformancePlatform.cpm).label('avg_cpm'),
            func.avg(FactPerformancePlatform.frequency).label('avg_frequency'),
            func.sum(FactPerformancePlatform.reach).label('total_reach'),
            func.sum(FactPerformancePlatform.messages_started).label('total_messages'),
            func.sum(FactPerformancePlatform.purchases).label('total_purchases'),
            func.sum(FactPerformancePlatform.purchase_value).label('total_purchase_value'),
            func.sum(FactPerformancePlatform.post_engagement).label('total_post_engagement'),
            func.sum(FactPerformancePlatform.link_click).label('total_link_click')
        ).join(DimDate, FactPerformancePlatform.date_key == DimDate.date_key)

        # Áp dụng bộ lọc thời gian
        if start_date and end_date:
            query = query.filter(DimDate.full_date.between(start_date, end_date))

        # Áp dụng các bộ lọc ID
        filters = []
        if data.get('campaign_ids'):
            filters.append(FactPerformancePlatform.campaign_id.in_(data['campaign_ids']))
        if data.get('adset_ids'):
            filters.append(FactPerformancePlatform.adset_id.in_(data['adset_ids']))
        if data.get('ad_ids'):
            filters.append(FactPerformancePlatform.ad_id.in_(data['ad_ids']))
        
        if filters:
            query = query.where(and_(*filters))

        # Thực thi câu truy vấn
        result = session.execute(query).first()

        if not result or result.total_impressions is None:
            # Trả về cấu trúc rỗng nếu không có kết quả
            return jsonify({
                'scorecards': {
                    'total_spend': 0, 'total_impressions': 0, 'total_clicks': 0,
                    'avg_ctr': 0, 'avg_cpm': 0, 'avg_frequency': 0,
                    'total_messages': 0, 'total_reach': 0, 'total_post_engagement': 0,
                    'total_link_click': 0, 'total_purchases': 0, 'total_purchase_value': 0
                }
            })

        # Tạo cấu trúc dữ liệu trả về
        response_data = {
            'scorecards': {
                'total_spend': result.total_spend or 0,
                'total_impressions': result.total_impressions or 0,
                'ctr': result.avg_ctr or 0,
                'total_clicks': result.total_clicks or 0,
                'avg_cpm': result.avg_cpm or 0,
                'avg_frequency': result.avg_frequency or 0,
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
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        # Logic xử lý date_preset
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date())
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            end_date = datetime.today().date()
            start_date = end_date.replace(day=1)

        # --- Xây dựng câu truy vấn động ---
        query = select(
            DimDate.full_date,
            func.sum(FactPerformancePlatform.spend).label('daily_spend'),
            func.sum(FactPerformancePlatform.impressions).label('daily_impressions')
        ).join(
            FactPerformancePlatform, FactPerformancePlatform.date_key == DimDate.date_key
        ).filter(
            DimDate.full_date.between(start_date, end_date)
        )

        # Áp dụng các bộ lọc ID nếu có
        filters = []
        if data.get('campaign_ids'):
            filters.append(FactPerformancePlatform.campaign_id.in_(data['campaign_ids']))
        if data.get('adset_ids'):
            filters.append(FactPerformancePlatform.adset_id.in_(data['adset_ids']))
        if data.get('ad_ids'):
            filters.append(FactPerformancePlatform.ad_id.in_(data['ad_ids']))
        
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

@app.route('/api/breakdown_chart', methods=['POST'])
def get_breakdown_chart_data():
    """
    Lấy dữ liệu đã được nhóm theo một chiều (dimension) cụ thể
    để vẽ biểu đồ tròn (pie/doughnut).
    Logic này sẽ tự động chọn bảng Fact (Platform hoặc Demographic)
    dựa trên dimension được yêu cầu.
    """
    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        
        # === 1. Lấy tham số động ===
        metric_name = data.get('metric', 'purchases')
        dimension_name = data.get('dimension', 'placement')

        if not metric_name or not dimension_name:
            return jsonify({'error': 'Vui lòng cung cấp cả "metric" và "dimension".'}), 400

        # === 2. Xử lý bộ lọc ngày ===
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date())
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            end_date = datetime.today().date()
            start_date = end_date.replace(day=1)

        # === 3. Ánh xạ Dimension (Chiều) và chọn Fact Table ===
        # Bước này quyết định cột nào sẽ được GROUP BY và bảng nào (Platform/Demographic) sẽ được truy vấn.
        dimension_mapping = {
            'placement': {
                'model': DimPlacement, 
                'join_on': FactPerformancePlatform.placement_id == DimPlacement.placement_id, 
                'column': DimPlacement.placement_name,
                'fact_table': FactPerformancePlatform # Dùng bảng Platform
            },
            'platform': {
                'model': DimPlatform, 
                'join_on': FactPerformancePlatform.platform_id == DimPlatform.platform_id, 
                'column': DimPlatform.platform_name,
                'fact_table': FactPerformancePlatform # Dùng bảng Platform
            },
            'gender': {
                'model': None, 
                'join_on': None, 
                'column': FactPerformanceDemographic.gender,
                'fact_table': FactPerformanceDemographic # Dùng bảng Demographic
            },
            'age': {
                'model': None, 
                'join_on': None, 
                'column': FactPerformanceDemographic.age,
                'fact_table': FactPerformanceDemographic # Dùng bảng Demographic
            }
        }

        dim_config = dimension_mapping.get(dimension_name)
        if dim_config is None:
            return jsonify({'error': f'Dimension không hợp lệ: {dimension_name}'}), 400
        
        dimension_column = dim_config['column'].label('dimension_label')
        FactTable = dim_config['fact_table'] # Đây là bảng Fact chính (Platform hoặc Demographic)

        # === 4. Ánh xạ Metric (Chỉ số) DỰA TRÊN FactTable ===
        
        # Hàm trợ giúp để ánh xạ metric, quyết định SUM hay AVG
        def get_metric_aggregator(table_model, metric_name_str):
            # Các metric cần SUM
            sum_metrics = {
                'spend': table_model.spend,
                'impressions': table_model.impressions,
                'clicks': table_model.clicks,
                'reach': table_model.reach,
                'purchases': table_model.purchases,
                'purchase_value': table_model.purchase_value,
                'messages_started': table_model.messages_started,
                'post_engagement': table_model.post_engagement,
                'link_click': table_model.link_click,
            }
            # Các metric cần AVG
            avg_metrics = {
                'ctr': table_model.ctr,
                'cpm': table_model.cpm,
                'frequency': table_model.frequency,
            }

            if metric_name_str in sum_metrics:
                return func.sum(sum_metrics[metric_name_str]).label('total_metric')
            elif metric_name_str in avg_metrics:
                return func.avg(avg_metrics[metric_name_str]).label('total_metric')
            
            return None

        # Lấy cột metric đã được tổng hợp (SUM/AVG)
        metric_aggregator = get_metric_aggregator(FactTable, metric_name)
        
        if metric_aggregator is None:
            return jsonify({'error': f'Metric không hợp lệ: {metric_name}'}), 400

        # === 5. Xây dựng câu truy vấn động ===
        query = select(
            dimension_column,
            metric_aggregator # Đây là (e.g., func.sum(FactTable.spend).label('total_metric'))
        ).join(
            DimDate, FactTable.date_key == DimDate.date_key # JOIN với bảng DimDate
        )

        # Thêm JOIN động (cho placement/platform)
        if dim_config['model'] is not None and dim_config['join_on'] is not None:
            query = query.join(dim_config['model'], dim_config['join_on'])
            
        # Áp dụng bộ lọc thời gian
        query = query.filter(DimDate.full_date.between(start_date, end_date))
            
        # Áp dụng các bộ lọc (campaign, adset, ad)
        # Các bộ lọc này sẽ tự động áp dụng trên FactTable chính xác (Platform hoặc Demographic)
        filters = []
        if data.get('campaign_ids'):
            filters.append(FactTable.campaign_id.in_(data['campaign_ids']))
        if data.get('adset_ids'):
            filters.append(FactTable.adset_id.in_(data['adset_ids']))
        if data.get('ad_ids'):
            filters.append(FactTable.ad_id.in_(data['ad_ids']))
        
        if filters:
            query = query.where(and_(*filters))

        # Nhóm theo dimension và sắp xếp theo metric giảm dần
        query = query.group_by(dimension_column).order_by(metric_aggregator.desc())

        # === 6. Thực thi và Định dạng kết quả cho Chart.js ===
        results = session.execute(query).all()

        labels = []
        chart_data = []

        if not results:
            return jsonify({'labels': [], 'datasets': [{'data': []}]})

        for row in results:
            labels.append(row.dimension_label or 'Không xác định') # Xử lý giá trị NULL
            chart_data.append(float(row.total_metric) if row.total_metric is not None else 0)

        # Tự động tạo dải màu cho biểu đồ pie
        num_labels = len(labels)
        colors = [f'hsl({(i * 360 / num_labels) % 360}, 70%, 60%)' for i in range(num_labels)]

        chartjs_data = {
            'labels': labels,
            'datasets': [
                {
                    'data': chart_data,
                    'backgroundColor': colors,
                    'hoverBackgroundColor': colors
                }
            ]
        }

        return jsonify(chartjs_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu biểu đồ breakdown: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/table_data', methods=['POST'])
def get_table_data():
    """
    Lấy dữ liệu hiệu suất đã được nhóm theo chiến dịch để hiển thị trong bảng. Chỉ lấy top 10 chiến dịch theo số lượt mua (purchases).
    Ở đây, chúng ta sẽ tính toán thêm chỉ số CPA (Cost Per Acquisition).
    """
    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        
        # === 1. Lấy Account ID (Bắt buộc) ===
        account_id = data.get('account_id')
        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400

        # === 2. Xử lý bộ lọc ngày (Giống các hàm khác) ===
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date())
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            end_date = datetime.today().date()
            start_date = end_date.replace(day=1)

        # === 3. Xây dựng Metric tính toán (CPA) ===
        # Sử dụng CASE để tránh lỗi chia cho 0
        cpa_case = case(
            (func.sum(FactPerformancePlatform.purchases) == 0, 0),
            else_=(func.sum(FactPerformancePlatform.spend) / func.sum(FactPerformancePlatform.purchases))
        ).label('cpa')

        # === 4. Xây dựng câu truy vấn động ===
        query = select(
            DimCampaign.name.label('campaign_name'),
            DimCampaign.status,
            func.sum(FactPerformancePlatform.spend).label('total_spend'),
            func.sum(FactPerformancePlatform.impressions).label('total_impressions'),
            func.sum(FactPerformancePlatform.purchases).label('total_purchases'),
            cpa_case
        ).join(
            DimCampaign, FactPerformancePlatform.campaign_id == DimCampaign.campaign_id
        ).join(
            DimDate, FactPerformancePlatform.date_key == DimDate.date_key
        )

        # === 5. Áp dụng bộ lọc ===
        
        # Lọc theo ngày (Luôn có)
        query = query.filter(DimDate.full_date.between(start_date, end_date))
        
        # Lọc theo Account (Luôn có)
        query = query.filter(DimCampaign.ad_account_id == account_id)

        # Lọc theo cấp bậc (Campaign, Adset, Ad) - (Tùy chọn)
        filters = []
        if data.get('campaign_ids'):
            filters.append(FactPerformancePlatform.campaign_id.in_(data['campaign_ids']))
        if data.get('adset_ids'):
            filters.append(FactPerformancePlatform.adset_id.in_(data['adset_ids']))
        if data.get('ad_ids'):
            filters.append(FactPerformancePlatform.ad_id.in_(data['ad_ids']))
        
        if filters:
            query = query.where(and_(*filters))

        # === 6. Nhóm và Sắp xếp ===
        query = query.group_by(
            DimCampaign.campaign_id, # Group by ID để đảm bảo tính duy nhất
            DimCampaign.name,
            DimCampaign.status
        ).order_by(
            func.sum(FactPerformancePlatform.purchases).desc(),  # Sắp xếp theo lượt mua giảm dần
            func.sum(FactPerformancePlatform.spend).asc()       # Sau đó tới chi tiêu tăng dần
        ).limit(10)  # Chỉ lấy top 10 chiến dịch

        # === 7. Thực thi và Định dạng kết quả ===
        results = session.execute(query).all()
        
        table_data = []
        for row in results:
            table_data.append({
                'campaign_name': row.campaign_name,
                'status': row.status,
                'spend': row.total_spend or 0,
                'impressions': row.total_impressions or 0,
                'purchases': row.total_purchases or 0,
                'cpa': row.cpa or 0
            })
            
        return jsonify(table_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu bảng: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True)