#!/usr/bin/env python3

import os
import logging
from datetime import datetime, timedelta, date

from flask import Flask, request, jsonify, render_template, redirect, url_for, flash, abort, Response
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

from dotenv import load_dotenv
from matplotlib.dates import relativedelta
import requests
import re
from sqlalchemy import func, select, and_, case
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import branca.colormap as cm
import io
import base64
import matplotlib
matplotlib.use('Agg') # Chuyển backend sang 'Agg' để chạy trên server
import matplotlib.pyplot as plt
from pywaffle import Waffle

import json

import threading
import time

# Import các lớp từ database_manager
from database_manager import (
    DatabaseManager, DimAdAccount, DimCampaign, DimAdset, DimAd, 
    FactPerformancePlatform, DimDate, DimPlatform, DimPlacement, 
    FactPerformanceDemographic, DimFanpage, FactPageMetricsDaily, 
    FactPostPerformance, DimRegion, FactPerformanceRegion,
    User
)
from ai_agent import AIAgent

DATE_PRESET = ['today', 'yesterday', 'this_month', 'last_month', 'this_quarter', 'maximum', 'data_maximum', 'last_3d', 'last_7d', 'last_14d', 'last_28d', 'last_30d', 'last_90d', 'last_week_mon_sun', 'last_week_sun_sat', 'last_quarter', 'last_year', 'this_week_mon_today', 'this_week_sun_today', 'this_year']

# --- BIẾN TOÀN CỤC ĐỂ KIỂM SOÁT TÁC VỤ CHẠY NGẦM ---
task_status = {
    'ads_refreshing': False,
    'fanpage_refreshing': False,
    'ads_start_time': None,
    'fanpage_start_time': None
}

# --- CẤU HÌNH CƠ BẢN ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

# --- KHỞI TẠO FLASK APP VÀ DATABASE MANAGER ---
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-meta-ads-secret-key')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=30) # Tự đăng xuất sau 30 phút

# --- CẤU HÌNH FLASK-LOGIN ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


db_manager = DatabaseManager()
# Tạo tất cả các bảng nếu chưa tồn tại
db_manager.create_all_tables()
# Khởi tạo AI Agent
try:
    ai_analyst = AIAgent()
    logger.info("Khởi tạo AI Analyst thành công.")
except Exception as e:
    logger.error(f"KHÔNG THỂ KHỞI TẠO AI AGENT: {e}")
    ai_analyst = None

# Tên file để lưu trạng thái chung cho tất cả worker
STATUS_FILE = 'system_status.json'

def save_task_status(status_data):
    """Lưu trạng thái vào file JSON để chia sẻ giữa các worker"""
    try:
        with open(STATUS_FILE, 'w') as f:
            json.dump(status_data, f)
    except Exception as e:
        logger.error(f"Không thể lưu trạng thái: {e}")

def load_task_status():
    """Đọc trạng thái từ file JSON"""
    if not os.path.exists(STATUS_FILE):
        # Trạng thái mặc định nếu file chưa tồn tại
        return {
            'ads_refreshing': False,
            'fanpage_refreshing': False,
            'ads_start_time': None,
            'fanpage_start_time': None
        }
    try:
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {
            'ads_refreshing': False,
            'fanpage_refreshing': False,
            'ads_start_time': None,
            'fanpage_start_time': None
        }

# --- HELPER: TẠO USER ADMIN MẶC ĐỊNH KHI CHẠY LẦN ĐẦU ---
def create_default_admin():
    session = db_manager.SessionLocal()
    try:
        # Kiểm tra xem có user nào chưa
        existing_user = session.query(User).first()
        if not existing_user:
            logger.info("Chưa có user. Đang tạo tài khoản Admin mặc định...")
            # Admin / Admin@123 (Bạn nên đổi ngay sau khi login)
            hashed_pw = generate_password_hash("Admin@123", method='pbkdf2:sha256')
            new_admin = User(username="admin", password_hash=hashed_pw, is_admin=True)
            session.add(new_admin)
            session.commit()
            logger.info("Đã tạo user: admin / Admin@123")
    except Exception as e:
        logger.error(f"Lỗi tạo admin mặc định: {e}")
    finally:
        session.close()

# Gọi hàm này ngay khi khởi động app
create_default_admin()

# --- FLASK-LOGIN USER LOADER ---
@login_manager.user_loader
def load_user(user_id):
    session = db_manager.SessionLocal()
    try:
        return session.query(User).get(int(user_id))
    finally:
        session.close()

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
# AUTHENTICATION ROUTES (MỚI)
# ======================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        session = db_manager.SessionLocal()
        user = session.query(User).filter_by(username=username).first()
        session.close()

        if user and check_password_hash(user.password_hash, password):
            session.permanent = True
            login_user(user)
            return redirect(url_for('index'))
        else:
            flash('Sai tên đăng nhập hoặc mật khẩu.', 'danger')
            
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# ======================================================================
# ADMIN ROUTES (MỚI)
# ======================================================================

@app.route('/admin')
@login_required
def admin_panel():
    if not current_user.is_admin:
        return "Access Denied: Bạn không phải Admin", 403
    
    session = db_manager.SessionLocal()
    users = session.query(User).all()
    session.close()
    return render_template('admin.html', users=users)

@app.route('/admin/create_user', methods=['POST'])
@login_required
def create_user():
    if not current_user.is_admin:
        abort(403)
        
    username = request.form.get('username')
    password = request.form.get('password')
    is_admin = True if request.form.get('is_admin') == 'on' else False
    
    session = db_manager.SessionLocal()
    try:
        existing = session.query(User).filter_by(username=username).first()
        if existing:
            flash('Username đã tồn tại!', 'error')
        else:
            hashed_pw = generate_password_hash(password, method='pbkdf2:sha256')
            new_user = User(username=username, password_hash=hashed_pw, is_admin=is_admin)
            session.add(new_user)
            session.commit()
            flash(f'Đã tạo user {username}', 'success')
    except Exception as e:
        logger.error(f"Lỗi tạo user: {e}")
        flash('Lỗi server', 'error')
    finally:
        session.close()
    
    # QUAN TRỌNG: Redirect về index với tham số ?panel=settings để JS tự mở lại tab
    return redirect(url_for('index', panel='settings'))

@app.route('/admin/change_password', methods=['POST'])
@login_required
def change_password():
    if not current_user.is_admin:
        abort(403)
        
    user_id = request.form.get('user_id')
    new_password = request.form.get('new_password')
    
    session = db_manager.SessionLocal()
    try:
        user = session.query(User).get(int(user_id))
        if user:
            user.password_hash = generate_password_hash(new_password, method='pbkdf2:sha256')
            session.commit()
            flash(f'Đã đổi mật khẩu cho {user.username}', 'success')
        else:
            flash('Không tìm thấy user', 'error')
    finally:
        session.close()
        
    return redirect(url_for('index', panel='settings'))

# ======================================================================
# API ENDPOINTS - TRUY VẤN TRỰC TIẾP TỪ DATABASE
# ======================================================================

@app.route('/')
@login_required
def index():
    # Mặc định users là rỗng
    users_list = []
    
    # Nếu là Admin, lấy danh sách User để hiển thị trong panel Cài đặt
    if current_user.is_admin:
        session = db_manager.SessionLocal()
        try:
            users_list = session.query(User).all()
        finally:
            session.close()

    # Truyền users vào template
    return render_template('index.html', user=current_user, users=users_list)

@app.route('/api/status/<task_type>', methods=['GET'])
@login_required
def get_task_status(task_type):
    # [SỬA] Đọc trạng thái từ File thay vì biến toàn cục
    current_status = load_task_status()
    
    if task_type == 'ads':
        is_refreshing = current_status.get('ads_refreshing', False)
        start_time = current_status.get('ads_start_time')
    elif task_type == 'fanpage':
        is_refreshing = current_status.get('fanpage_refreshing', False)
        start_time = current_status.get('fanpage_start_time')
    else:
        return jsonify({'error': 'Loại tác vụ không hợp lệ.'}), 400

    elapsed_time = 0
    if is_refreshing and start_time:
        elapsed_time = int(time.time() - start_time)

    return jsonify({
        'status': f"{task_type}_refreshing" if is_refreshing else 'finished',
        'elapsed_time': f"{elapsed_time:02d}",
        'is_refreshing': is_refreshing
    })

@app.route('/api/refresh', methods=['POST'])
@login_required
def refresh_data():
    """
    [ASYNC] Kích hoạt quy trình làm mới dữ liệu Ads chạy ngầm.
    [ĐÃ SỬA] Thực hiện refresh theo từng ngày (daily loop) để tránh timeout.
    """
    # 1. Đọc trạng thái từ file
    current_status = load_task_status()
    if current_status.get('ads_refreshing'):
        return jsonify({'message': 'Hệ thống đang cập nhật dữ liệu Ads. Vui lòng đợi...'}), 429

    try:
        data = request.get_json()
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')

        # Logic xác định ngày 
        start_date, end_date = None, None
        if date_preset and date_preset in DATE_PRESET:
            today = datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date()
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=today)
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            end_date = datetime.today().date()
            start_date = end_date.replace(day=1)

        # 2. Hàm chạy ngầm (Worker)
        def run_async_job(app_context, s_date_str, e_date_str, preset):
            import time
            from datetime import datetime, timedelta
            
            start_date_worker = datetime.strptime(s_date_str, '%Y-%m-%d').date()
            end_date_worker = datetime.strptime(e_date_str, '%Y-%m-%d').date()
            current_date_worker = start_date_worker
            
            with app_context: 
                logger.info(">>> BẮT ĐẦU THREAD REFRESH ADS <<<")
                
                # [SỬA] Cập nhật trạng thái vào FILE -> True
                status_update = load_task_status()
                status_update['ads_refreshing'] = True
                status_update['ads_start_time'] = time.time()
                save_task_status(status_update)
                
                try:
                    while current_date_worker <= end_date_worker:
                        current_date_str = current_date_worker.strftime('%Y-%m-%d')
                        
                        # D to D+1 logic (Giữ nguyên)
                        until_date_obj = current_date_worker + timedelta(days=1)
                        until_date_str = until_date_obj.strftime('%Y-%m-%d') 
                        
                        logger.info(f"THREAD ADS: Đang nạp {current_date_str}")

                        try:
                            db_manager.refresh_data(
                                start_date=current_date_str, 
                                end_date=until_date_str,
                                date_preset=preset
                            )
                        except Exception as e:
                            logger.error(f"Lỗi nạp ngày {current_date_str}: {e}")
                        
                        current_date_worker += timedelta(days=1)
                        time.sleep(1) 

                finally:
                    # [QUAN TRỌNG] Luôn cập nhật trạng thái về False dù có lỗi hay không
                    logger.info(">>> KẾT THÚC THREAD REFRESH ADS <<<")
                    # Thêm delay nhỏ để frontend kịp nhận trạng thái cuối cùng nếu cần
                    time.sleep(2) 
                    
                    # [SỬA] Cập nhật trạng thái vào FILE -> False
                    final_status = load_task_status()
                    final_status['ads_refreshing'] = False
                    final_status['ads_start_time'] = None
                    save_task_status(final_status)

        # 3. Khởi tạo Thread
        thread = threading.Thread(target=run_async_job, args=(
            app.app_context(),
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            date_preset
        ))
        thread.start()
        
        # 4. Trả về ngay lập tức
        return jsonify({'message': 'Đã tiếp nhận yêu cầu! Dữ liệu đang được cập nhật ngầm (theo từng ngày).'})
    
    except Exception as e:
        logger.error(f"Lỗi khi kích hoạt refresh: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/accounts', methods=['GET'])
@login_required
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
@login_required
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
            .filter((DimCampaign.stop_time >= start_date) | (DimCampaign.stop_time == None))\
            .order_by(DimCampaign.name).all()

        campaigns_list = [{'campaign_id': c.campaign_id, 'name': c.name} for c in campaigns]
        return jsonify(campaigns_list)
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách chiến dịch từ DB: {e}")
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/adsets', methods=['POST'])
@login_required
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
            .filter((DimAdset.end_time >= start_date) | (DimAdset.end_time == None))\
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
@login_required
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
            .filter((DimAd.ad_schedule_start_time <= end_date) | (DimAd.ad_schedule_start_time == None))\
            .filter((DimAd.ad_schedule_end_time >= start_date) | (DimAd.ad_schedule_end_time == None))\
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
@login_required
def get_overview_data():
    """
    Tổng hợp dữ liệu scorecard.
    - Lấy các metric CÓ THỂ CỘNG (spend, clicks, impressions) từ DB.
    - Lấy metric KHÔNG THỂ CỘNG (reach) bằng cách gọi API live.
    - Tính toán CTR (clicks/impressions) và so sánh kỳ trước.
    """
    session = db_manager.SessionLocal()
    from fbads_extract import FacebookAdsExtractor # Import để lấy hàm live extract metrics
    extractor = FacebookAdsExtractor()
    
    # Cấu trúc rỗng trả về nếu có lỗi hoặc không có dữ liệu
    empty_scorecards = {
        'total_spend': 0, 'total_impressions': 0, 'ctr': 0, 'total_purchases': 0,
        'total_clicks': 0, 'avg_cpm': 0, 'avg_frequency': 0, 'total_messages': 0,
        'total_reach': 0, 'total_post_engagement': 0, 'total_link_click': 0, 
        'total_purchase_value': 0, 'avg_ctr': 0, # Thêm avg_ctr (tính từ db)
        'total_spend_previous': None, 'total_spend_absolute': None, 'total_spend_growth': None,
        'total_impressions_previous': None, 'total_impressions_absolute': None, 'total_impressions_growth': None,
        'ctr_previous': None, 'ctr_absolute': None, 'ctr_growth': None,
        'total_purchases_previous': None, 'total_purchases_absolute': None, 'total_purchases_growth': None,
        'total_reach_previous': None, 'total_reach_absolute': None, 'total_reach_growth': None,
    }

    try:
        data = request.get_json()
        
        # === 1. LẤY BỘ LỌC ===
        account_id = data.get('account_id')
        if not account_id:
            logger.error("get_overview_data thiếu account_id. Sẽ không thể lấy 'reach' live.")
            return jsonify({'error': 'Thiếu account_id.'}), 400
        
        campaign_ids = data.get('campaign_ids')
        adset_ids = data.get('adset_ids')
        ad_ids = data.get('ad_ids')
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        
        # === 2. XÁC ĐỊNH KỲ HIỆN TẠI ===
        start_date, end_date = None, None
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date())
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            end_date = datetime.today().date()
            start_date = end_date.replace(day=1)

        # === 3. HÀM HELPER ĐỂ TRUY VẤN DỮ LIỆU (TỪ DATABASE) ===
        def _get_period_data_from_db(period_start, period_end):
            """Hàm nội bộ để lấy dữ liệu TÍNH TỔNG ĐƯỢC từ DB."""
            
            query = select(
                func.sum(FactPerformancePlatform.spend).label('total_spend'),
                func.sum(FactPerformancePlatform.impressions).label('total_impressions'),
                func.sum(FactPerformancePlatform.clicks).label('total_clicks'),
                func.avg(FactPerformancePlatform.ctr).label('avg_ctr'), # Đây là CTR trung bình (tính từ db)
                func.avg(FactPerformancePlatform.cpm).label('avg_cpm'),
                func.avg(FactPerformancePlatform.frequency).label('avg_frequency'),
                func.sum(FactPerformancePlatform.messages_started).label('total_messages'),
                func.sum(FactPerformancePlatform.purchases).label('total_purchases'),
                func.sum(FactPerformancePlatform.purchase_value).label('total_purchase_value'),
                func.sum(FactPerformancePlatform.post_engagement).label('total_post_engagement'),
                func.sum(FactPerformancePlatform.link_click).label('total_link_click')
            ).join(DimDate, FactPerformancePlatform.date_key == DimDate.date_key
            ).join(
                DimCampaign, FactPerformancePlatform.campaign_id == DimCampaign.campaign_id
            ).filter(
                DimCampaign.ad_account_id == account_id
            )

            if period_start and period_end:
                query = query.filter(DimDate.full_date.between(period_start, period_end))
            
            filters = []
            if campaign_ids:
                filters.append(FactPerformancePlatform.campaign_id.in_(campaign_ids))
            if adset_ids:
                filters.append(FactPerformancePlatform.adset_id.in_(adset_ids))
            if ad_ids:
                filters.append(FactPerformancePlatform.ad_id.in_(ad_ids))
            
            if filters:
                query = query.where(and_(*filters))

            return session.execute(query).first()

        # === 4. TRUY VẤN DỮ LIỆU KỲ HIỆN TẠI (TỪ DB) ===
        current_results_db = _get_period_data_from_db(start_date, end_date)

        if not current_results_db or current_results_db.total_impressions is None:
            return jsonify({'scorecards': empty_scorecards})

        # === 5. XÁC ĐỊNH KỲ TRƯỚC ===
        previous_results_db = None
        prev_start_date, prev_end_date = None, None
        
        if start_date and end_date:
            try:
                duration_days = (end_date - start_date).days + 1
                prev_end_date = start_date - relativedelta(days=1)
                prev_start_date = prev_end_date - relativedelta(days=duration_days - 1)
                
                # Truy vấn dữ liệu kỳ trước (từ DB)
                previous_results_db = _get_period_data_from_db(prev_start_date, prev_end_date)
            except Exception as e:
                logger.warning(f"Không thể tính toán hoặc truy vấn kỳ trước: {e}")
                previous_results_db = None

        # === 6. TÍNH TOÁN METRIC (CTR) VÀ GỌI API (REACH) ===

        # Hàm helper
        def calculate_growth(current, previous):
            current_val = float(current or 0)
            previous_val = float(previous or 0)
            if previous_val == 0: return None
            return (current_val - previous_val) / previous_val

        def calculate_absolute(current, previous, previous_exists):
            if not previous_exists: return None
            current_val = float(current or 0)
            previous_val = float(previous or 0)
            return current_val - previous_val

        # --- Kỳ hiện tại ---
        current_spend = current_results_db.total_spend or 0
        current_impressions = current_results_db.total_impressions or 0
        current_clicks = current_results_db.total_clicks or 0
        current_purchases = current_results_db.total_purchases or 0
        current_post_engagement = current_results_db.total_post_engagement or 0
        current_link_click = current_results_db.total_link_click or 0
        current_messages = current_results_db.total_messages or 0
        current_purchase_value = current_results_db.total_purchase_value or 0
        
        # TÍNH TOÁN CTR THỰC TẾ (Kỳ hiện tại)
        current_ctr_true = (current_clicks / current_impressions) * 100 if current_impressions else 0
        
        # GỌI API LẤY REACH (Kỳ hiện tại)
        current_reach = 0.0
        if account_id:
            current_reach = extractor.get_total_metric(
                account_id=account_id,
                metric_name='reach',
                campaign_ids=campaign_ids,
                adset_ids=adset_ids,
                ad_ids=ad_ids,
                date_preset=date_preset if not (start_date and end_date) else None,
                start_date=start_date,
                end_date=end_date
            )
            if current_reach:
                metric_value_str = current_reach[0].get('reach', '0')
                current_reach = float(metric_value_str)
        else:
            logger.warning("Không có account_id, current_reach được đặt là 0")

        # --- Kỳ trước ---
        previous_exists = previous_results_db is not None
        
        prev_spend = (previous_results_db.total_spend or 0) if previous_exists else 0
        prev_impressions = (previous_results_db.total_impressions or 0) if previous_exists else 0
        prev_clicks = (previous_results_db.total_clicks or 0) if previous_exists else 0
        prev_purchases = (previous_results_db.total_purchases or 0) if previous_exists else 0
        prev_post_engagement = (previous_results_db.total_post_engagement or 0) if previous_exists else 0
        prev_link_click = (previous_results_db.total_link_click or 0) if previous_exists else 0
        prev_messages = (previous_results_db.total_messages or 0) if previous_exists else 0
        prev_purchase_value = (previous_results_db.total_purchase_value or 0) if previous_exists else 0

        # TÍNH TOÁN CTR THỰC TẾ (Kỳ trước)
        prev_ctr_true = (prev_clicks / prev_impressions) * 100 if prev_impressions else 0

        # GỌI API LẤY REACH (Kỳ trước)
        prev_reach = 0.0
        if account_id and previous_exists:
            prev_reach = extractor.get_total_metric(
                account_id=account_id,
                metric_name='reach',
                campaign_ids=campaign_ids,
                adset_ids=adset_ids,
                ad_ids=ad_ids,
                date_preset=None, # Luôn dùng ngày tuyệt đối cho kỳ trước
                start_date=prev_start_date,
                end_date=prev_end_date
            )
            if prev_reach:
                metric_value_str = prev_reach[0].get('reach', '0')
                prev_reach = float(metric_value_str)
        elif previous_exists:
            logger.warning("Không có account_id, prev_reach được đặt là 0")

        # === 7. TẠO SCORECARDS ===
        scorecards = {
            # Chỉ số chính kỳ hiện tại
            'total_spend': current_spend,
            'total_impressions': current_impressions,
            'ctr': current_ctr_true, # <-- CTR đúng tính thủ công
            'total_purchases': current_purchases,
            'total_reach': current_reach,

            # Các chỉ số phụ kỳ hiện tại
            'total_clicks': current_clicks,
            'avg_cpm': current_results_db.avg_cpm or 0,
            'avg_frequency': current_results_db.avg_frequency or 0,
            'total_messages': current_results_db.total_messages or 0,
            'total_post_engagement': current_results_db.total_post_engagement or 0,
            'total_link_click': current_results_db.total_link_click or 0,
            'total_purchase_value': current_results_db.total_purchase_value or 0,
            'avg_ctr': current_results_db.avg_ctr or 0, # <-- CTR TRUNG BÌNH (tính bằng db)

            # 1. Chi phí (Spend)
            'total_spend_previous': prev_spend if previous_exists else None,
            'total_spend_absolute': calculate_absolute(current_spend, prev_spend, previous_exists),
            'total_spend_growth': calculate_growth(current_spend, prev_spend),

            # 2. Hiển thị (Impressions)
            'total_impressions_previous': prev_impressions if previous_exists else None,
            'total_impressions_absolute': calculate_absolute(current_impressions, prev_impressions, previous_exists),
            'total_impressions_growth': calculate_growth(current_impressions, prev_impressions),
            
            # 3. CTR (Đã sửa)
            'ctr_previous': prev_ctr_true if previous_exists else None,
            'ctr_absolute': calculate_absolute(current_ctr_true, prev_ctr_true, previous_exists),
            'ctr_growth': calculate_growth(current_ctr_true, prev_ctr_true),

            # 4. Lượt mua (Purchases)
            'total_purchases_previous': prev_purchases if previous_exists else None,
            'total_purchases_absolute': calculate_absolute(current_purchases, prev_purchases, previous_exists),
            'total_purchases_growth': calculate_growth(current_purchases, prev_purchases),
            
            # 5. Reach (Đã sửa)
            'total_reach_previous': prev_reach if previous_exists else None,
            'total_reach_absolute': calculate_absolute(current_reach, prev_reach, previous_exists),
            'total_reach_growth': calculate_growth(current_reach, prev_reach),

            # 6. Lượt nhắn tin
            'total_messages_previous': prev_messages if previous_exists else None,
            'total_messages_absolute': calculate_absolute(current_messages, prev_messages, previous_exists),
            'total_messages_growth': calculate_growth(current_messages, prev_messages),

            # 7. Tương tác bài viết
            'total_post_engagement_previous': prev_post_engagement if previous_exists else None,
            'total_post_engagement_absolute': calculate_absolute(current_post_engagement, prev_post_engagement, previous_exists),
            'total_post_engagement_growth': calculate_growth(current_post_engagement, prev_post_engagement),

            # 8. Click vào links
            'total_link_click_previous': prev_link_click if previous_exists else None,
            'total_link_click_absolute': calculate_absolute(current_link_click, prev_link_click, previous_exists),
            'total_link_click_growth': calculate_growth(current_link_click, prev_link_click),

            # 9. Giá trị mua hàng
            'total_purchase_value_previous': prev_purchase_value if previous_exists else None,
            'total_purchase_value_absolute': calculate_absolute(current_purchase_value, prev_purchase_value, previous_exists),
            'total_purchase_value_growth': calculate_growth(current_purchase_value, prev_purchase_value)
        }

        response_data = {'scorecards': scorecards}
        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu tổng quan từ DB: {e}", exc_info=True)
        return jsonify({'scorecards': empty_scorecards}), 500
    finally:
        session.close()

@app.route('/api/chart_data', methods=['POST'])
@login_required
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
        account_id = data.get('account_id')
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
        ).join(
            DimCampaign, FactPerformancePlatform.campaign_id == DimCampaign.campaign_id
        ).filter(
            DimCampaign.ad_account_id == account_id
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
@login_required
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

        account_id = data.get('account_id')
        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400
        
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
        ).join(
            DimCampaign, FactTable.campaign_id == DimCampaign.campaign_id
        ).filter(
            DimCampaign.ad_account_id == account_id
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
@login_required
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

@app.route('/api/chat', methods=['POST'])
@login_required
def handle_chat():
    """
    Nhận tin nhắn từ người dùng và trả về câu trả lời của AI.
    """
    if not ai_analyst:
        return jsonify({'error': 'AI Agent chưa được khởi tạo đúng cách. Vui lòng kiểm tra GOOGLE_API_KEY.'}), 500
        
    data = request.get_json()
    user_message = data.get('message')

    if not user_message:
        return jsonify({'error': 'Không có tin nhắn nào được gửi.'}), 400

    def generate_stream():
        """Generator stream dữ liệu SSE an toàn với Heroku Timeout"""
        try:
            # 1. Gửi ngay một comment SSE để thiết lập kết nối với Heroku
            # Heroku thấy byte này sẽ reset bộ đếm timeout 30s
            yield ": start-stream\n\n"

            # 2. Duyệt qua generator của AI
            for chunk in ai_analyst.ask(user_message):
                
                # chunk bây giờ là dict: {'type': '...', 'content': '...'}
                
                if chunk['type'] == 'status':
                    # TRICK QUAN TRỌNG:
                    # Gửi trạng thái dưới dạng comment SSE (bắt đầu bằng dấu :) 
                    # hoặc event riêng để Frontend hiển thị "Loading..."
                    # Ở đây gửi về data status để có thể hiện lên UI nếu muốn
                    payload = json.dumps({'status': chunk['content']})
                    yield f"data: {payload}\n\n"
                    
                elif chunk['type'] == 'text':
                    # Dữ liệu văn bản trả lời thực sự
                    payload = json.dumps({'text': chunk['content']})
                    yield f"data: {payload}\n\n"
            
            yield "data: [DONE]\n\n"
            
        except Exception as e:
            logger.error(f"Lỗi streaming: {e}", exc_info=True)
            yield f"data: {json.dumps({'error': 'Lỗi xử lý AI.'})}\n\n"

    return Response(
        generate_stream(), 
        mimetype='text/event-stream',
        headers={'X-Accel-Buffering': 'no'} 
    )
    
@app.route('/api/refresh_fanpage', methods=['POST'])
@login_required
def refresh_data_fanpage():
    """
    [ASYNC] Kích hoạt quy trình làm mới dữ liệu FANPAGE chạy ngầm.
    """
    # [SỬA 1] Đọc trạng thái từ File thay vì biến toàn cục RAM
    current_status = load_task_status()
    if current_status.get('fanpage_refreshing'):
        return jsonify({'message': 'Hệ thống đang cập nhật Fanpage. Vui lòng đợi...'}), 429

    try:
        data = request.get_json()
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')

        # Logic xác định ngày (Giữ nguyên)
        start_date, end_date = None, None
        if date_preset and date_preset in DATE_PRESET:
            today = datetime.strptime(end_date_input, '%Y-%m-%d').date() if end_date_input else datetime.today().date()
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=today)
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            end_date = datetime.today().date()
            start_date = end_date.replace(day=1)
        
        # 2. Hàm chạy ngầm (Worker)
        def run_async_job(app_context, s_date_str, e_date_str):
            import time
            from datetime import datetime, timedelta
            
            start_date_worker = datetime.strptime(s_date_str, '%Y-%m-%d').date()
            end_date_worker = datetime.strptime(e_date_str, '%Y-%m-%d').date()
            current_date_worker = start_date_worker

            with app_context:
                logger.info(">>> BẮT ĐẦU THREAD REFRESH FANPAGE <<<")
                
                # [SỬA 2] Ghi trạng thái 'Đang chạy' vào FILE JSON
                status_update = load_task_status()
                status_update['fanpage_refreshing'] = True
                status_update['fanpage_start_time'] = time.time()
                save_task_status(status_update)
                
                try:
                    while current_date_worker <= end_date_worker:
                        current_date_str = current_date_worker.strftime('%Y-%m-%d')
                        
                        # D to D+1 logic
                        until_date_obj = current_date_worker + timedelta(days=1)
                        until_date_str = until_date_obj.strftime('%Y-%m-%d') 
                        
                        logger.info(f"THREAD FANPAGE: Nạp {current_date_str}")

                        try:
                            db_manager.refresh_data_fanpage(
                                start_date=current_date_str, 
                                end_date=until_date_str 
                            )
                        except Exception as e:
                            logger.error(f"Lỗi nạp Fanpage ngày {current_date_str}: {e}")
                        
                        current_date_worker += timedelta(days=1)
                        time.sleep(1) # Throttling

                finally:
                    logger.info(">>> KẾT THÚC THREAD REFRESH FANPAGE <<<")
                    # Thêm delay nhỏ để Frontend kịp nhìn thấy trạng thái
                    time.sleep(2)

                    # [SỬA 3] Ghi trạng thái 'Đã xong' vào FILE JSON
                    final_status = load_task_status()
                    final_status['fanpage_refreshing'] = False
                    final_status['fanpage_start_time'] = None
                    save_task_status(final_status)

        # 3. Khởi tạo Thread
        thread = threading.Thread(target=run_async_job, args=(
            app.app_context(),
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))
        thread.start()
        
        return jsonify({'message': 'Đã tiếp nhận! Dữ liệu Fanpage đang cập nhật ngầm.'})
    
    except Exception as e:
        logger.error(f"Lỗi refresh Fanpage: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ======================================================================
# API ENDPOINTS - FANPAGE OVERVIEW
# ======================================================================

@app.route('/api/fanpage/list', methods=['GET'])
@login_required
def get_fanpage_list():
    """
    Lấy danh sách Fanpage (ID, Name) từ dim_fanpage để điền vào
    dropdown filter (id="fp-filter-fanpage").
    """
    session = db_manager.SessionLocal()
    try:
        pages = session.query(DimFanpage.page_id, DimFanpage.name)\
            .order_by(DimFanpage.name)\
            .all()
        
        page_list = [{'id': page.page_id, 'name': page.name} for page in pages]
        return jsonify(page_list)
        
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách Fanpage: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/fanpage/overview_data', methods=['POST'])
@login_required
def get_fanpage_overview_data():
    """
    Lấy TOÀN BỘ dữ liệu cho Panel Fanpage Overview, bao gồm:
    1. Scorecards (KPIs) (từ 2 bảng Fact)
    2. Main Chart (từ FactPageMetricsDaily)
    3. Interactions Table (từ FactPageMetricsDaily)
    4. Content Type Table (từ FactPostPerformance)
    5. Top 5 Content Tables (từ FactPostPerformance)
    """
    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        page_id = data.get('page_id')
        
        date_preset = data.get('date_preset')
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')

        if not page_id:
            return jsonify({'error': 'Thiếu "page_id".'}), 400

        # === 1. TÍNH TOÁN KHOẢNG THỜI GIAN (LOGIC MỚI) ===
        start_date, end_date = None, None
        today = datetime.today().date() 

        if date_preset and date_preset != 'custom':
            # Case 1: Dùng preset (e.g., 'last_7d')
            start_date, end_date = _calculate_date_range(date_preset, today)
        elif start_date_input and end_date_input:
            # Case 2: Dùng ngày tùy chỉnh (khi preset là 'custom')
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            # Case 3: Fallback (Mặc định là last_month nếu không có gì được gửi)
            logger.warning(f"Không có date_preset ({date_preset}) hoặc ngày tùy chỉnh, mặc định 'last_month'.")
            date_preset = 'last_month' # Gán lại để logic bên dưới chạy đúng
            start_date, end_date = _calculate_date_range(date_preset, today)
        
        # Tính kỳ trước để so sánh
        duration_days = (end_date - start_date).days + 1
        prev_end_date = start_date - timedelta(days=1)
        prev_start_date = prev_end_date - timedelta(days=duration_days - 1)
        
        # Chuyển đổi sang datetime cho FactPostPerformance (Logic này giữ nguyên)
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        prev_start_datetime = datetime.combine(prev_start_date, datetime.min.time())
        prev_end_datetime = datetime.combine(prev_end_date, datetime.max.time())

        # === 2. HÀM HELPER TÍNH TOÁN (Giữ nguyên) ===
        
        def _calculate_growth(current, previous):
            current_val = float(current or 0)
            previous_val = float(previous or 0)
            if previous_val == 0:
                return None # Tránh chia cho 0
            return (current_val - previous_val) / previous_val

        # === 3. TRUY VẤN DỮ LIỆU (TỔ HỢP NHIỀU TRUY VẤN) (Giữ nguyên) ===
        
        # --- Q1 & Q2: Dữ liệu cho Scorecard (KPIs) ---
        
        # Helper để lấy metrics TỔNG HỢP (từ FactPageMetricsDaily)
        def _get_daily_kpis(p_id, s_date, e_date):
            return session.query(
                func.sum(FactPageMetricsDaily.page_fan_adds_unique).label('new_likes'),
                func.sum(FactPageMetricsDaily.page_impressions).label('impressions'),
                func.sum(FactPageMetricsDaily.page_post_engagements).label('engagement'),
                func.sum(FactPageMetricsDaily.page_video_views).label('video_views')
            ).join(DimDate, FactPageMetricsDaily.date_key == DimDate.date_key)\
             .filter(FactPageMetricsDaily.page_id == p_id)\
             .filter(DimDate.full_date.between(s_date, e_date))\
             .first()

        # Helper để lấy metrics POST (từ FactPostPerformance)
        def _get_post_kpis(p_id, s_datetime, e_datetime):
            return session.query(
                func.sum(FactPostPerformance.lt_post_clicks).label('clicks'),
                func.sum(FactPostPerformance.comments_total_count).label('comments'),
                func.sum(FactPostPerformance.lt_post_reactions_like_total).label('post_likes'),
                func.sum(FactPostPerformance.lt_post_impressions_organic_unique).label('organic_impressions')
            ).filter(FactPostPerformance.page_id == p_id)\
             .filter(FactPostPerformance.created_time.between(s_datetime, e_datetime))\
             .first()

        # Helper để lấy Total Likes (metric tích lũy)
        def _get_total_likes(p_id, at_date):
            # Lấy bản ghi MỚI NHẤT (latest record)
            # vào hoặc trước ngày được chọn (at_date)
            result = session.query(FactPageMetricsDaily.page_fans)\
                .join(DimDate, FactPageMetricsDaily.date_key == DimDate.date_key)\
                .filter(FactPageMetricsDaily.page_id == p_id)\
                .filter(DimDate.full_date <= at_date)\
                .order_by(DimDate.full_date.desc())\
                .first() # <-- Lấy bản ghi đầu tiên (là ngày mới nhất)
            return result.page_fans if result else 0

        # Lấy dữ liệu 2 kỳ
        current_daily_kpis = _get_daily_kpis(page_id, start_date, end_date)
        prev_daily_kpis = _get_daily_kpis(page_id, prev_start_date, prev_end_date)
        
        current_post_kpis = _get_post_kpis(page_id, start_datetime, end_datetime)
        prev_post_kpis = _get_post_kpis(page_id, prev_start_datetime, prev_end_datetime)
        
        current_total_likes = _get_total_likes(page_id, end_date)

        # Gộp thành 1 dict `scorecards`
        scorecards = {
            'total_likes': current_total_likes,
            'new_likes': current_daily_kpis.new_likes or 0,
            'new_likes_growth': _calculate_growth(current_daily_kpis.new_likes, prev_daily_kpis.new_likes),
            'impressions': current_daily_kpis.impressions or 0,
            'impressions_growth': _calculate_growth(current_daily_kpis.impressions, prev_daily_kpis.impressions),
            'engagement': current_daily_kpis.engagement or 0,
            'engagement_growth': _calculate_growth(current_daily_kpis.engagement, prev_daily_kpis.engagement),
            'video_views': current_daily_kpis.video_views or 0,
            'video_views_growth': _calculate_growth(current_daily_kpis.video_views, prev_daily_kpis.video_views),
            'clicks': current_post_kpis.clicks or 0,
            'clicks_growth': _calculate_growth(current_post_kpis.clicks, prev_post_kpis.clicks),
            'comments': current_post_kpis.comments or 0,
            'comments_growth': _calculate_growth(current_post_kpis.comments, prev_post_kpis.comments),
            'post_likes': current_post_kpis.post_likes or 0,
            'post_likes_growth': _calculate_growth(current_post_kpis.post_likes, prev_post_kpis.post_likes),
            'organic_impressions': current_post_kpis.organic_impressions or 0,
            'organic_impressions_growth': _calculate_growth(current_post_kpis.organic_impressions, prev_post_kpis.organic_impressions),
        }

        # --- Q3: Dữ liệu cho Main Chart (fp-main-chart) ---
        chart_query = session.query(
            DimDate.full_date,
            func.sum(FactPageMetricsDaily.page_fan_adds_unique).label('new_likes'),
            func.sum(FactPageMetricsDaily.page_impressions).label('impressions'),
            func.sum(FactPageMetricsDaily.page_post_engagements).label('engagement')
        ).join(DimDate, FactPageMetricsDaily.date_key == DimDate.date_key)\
         .filter(FactPageMetricsDaily.page_id == page_id)\
         .filter(DimDate.full_date.between(start_date, end_date))\
         .group_by(DimDate.full_date)\
         .order_by(DimDate.full_date)\
         .all()

        # Tạo map để điền ngày thiếu
        chart_results_map = {res.full_date: res for res in chart_query}
        chart_labels = []
        chart_new_likes_data = []
        chart_impressions_data = []
        chart_engagement_data = []
        
        current_chart_date = start_date
        while current_chart_date <= end_date:
            date_str = current_chart_date.strftime('%Y-%m-%d')
            chart_labels.append(date_str)
            
            if current_chart_date in chart_results_map:
                res = chart_results_map[current_chart_date]
                chart_new_likes_data.append(res.new_likes or 0)
                chart_impressions_data.append(res.impressions or 0)
                chart_engagement_data.append(res.engagement or 0)
            else:
                chart_new_likes_data.append(0)
                chart_impressions_data.append(0)
                chart_engagement_data.append(0)
            
            current_chart_date += timedelta(days=1)

        main_chart_data = {
            'labels': chart_labels,
            'datasets': [
                {'label': 'New likes', 'data': chart_new_likes_data, 'borderColor': '#3b82f6'},
                {'label': 'Page Impressions', 'data': chart_impressions_data, 'borderColor': '#9ca3af'},
                {'label': 'Page Post Engagements', 'data': chart_engagement_data, 'borderColor': '#f472b6'}
            ]
        }

        # --- Q4: Dữ liệu Bảng Tương Tác (fp-interactions-table-body) ---
        
        table_query = session.query(
            DimDate.full_date,
            func.sum(FactPageMetricsDaily.page_impressions).label('impressions'),
            func.sum(FactPageMetricsDaily.page_post_engagements).label('engagement'),
            func.sum(FactPageMetricsDaily.page_impressions_unique).label('impressions_unique'), 
            func.sum(FactPageMetricsDaily.page_fan_removes).label('fan_removes'),                 
            func.sum(FactPageMetricsDaily.page_video_views).label('video_views'),
            func.sum(FactPageMetricsDaily.page_posts_impressions_organic_unique).label('organic_unique')
        ).join(DimDate, FactPageMetricsDaily.date_key == DimDate.date_key)\
         .filter(FactPageMetricsDaily.page_id == page_id)\
         .filter(DimDate.full_date.between(start_date, end_date))\
         .group_by(DimDate.full_date)\
         .order_by(DimDate.full_date.desc())\
         .all()
        
        interactions_table = [
            {
                'date': row.full_date.strftime('%Y-%m-%d'),
                'impressions': row.impressions or 0,
                'engagement': row.engagement or 0,
                'impressions_unique': row.impressions_unique or 0,
                'fan_removes': row.fan_removes or 0,            
                'video_views': row.video_views or 0,
                'organic_unique': row.organic_unique or 0
            } for row in table_query
        ]

        # --- Q5: Bảng Sơ Lượng Content (fp-content-type-body) ---
        # Dùng CASE để phân loại 'post_type'
        
        # 1. Định nghĩa cột "phân loại"
        #    Nếu post_type chứa dấu ':' (ví dụ: "00:32"), coi là Video/Reels
        #    Ngược lại (bao gồm cả NULL), coi là Static
        categorized_post_type = case(
            (FactPostPerformance.post_type.like('%:%'), 'Video/Reels'),
            else_='Static'
        ).label('categorized_type')

        # 2. Truy vấn và nhóm theo cột phân loại MỚI
        content_type_query = session.query(
            categorized_post_type,
            func.count(FactPostPerformance.post_id).label('count')
        ).filter(FactPostPerformance.page_id == page_id)\
         .filter(FactPostPerformance.created_time.between(start_datetime, end_datetime))\
         .group_by(categorized_post_type)\
         .order_by(func.count(FactPostPerformance.post_id).desc())\
         .all()
        
        # 3. Render kết quả
        content_type_table = [
            {'type': row.categorized_type, 'count': row.count}
            for row in content_type_query
        ]

        # --- Q6: Bảng Top 5 Content (3 bảng) (Giữ nguyên) ---
        def _get_top_posts(metric_column, limit=5):
            return session.query(
                FactPostPerformance.message,
                FactPostPerformance.full_picture_url,
                metric_column.label('metric_value')
            ).filter(FactPostPerformance.page_id == page_id)\
             .filter(FactPostPerformance.created_time.between(start_datetime, end_datetime))\
             .order_by(metric_column.desc().nullslast())\
             .limit(limit)\
             .all()

        top_impressions = _get_top_posts(FactPostPerformance.lt_post_impressions)
        top_likes = _get_top_posts(FactPostPerformance.lt_post_reactions_like_total)
        top_clicks = _get_top_posts(FactPostPerformance.lt_post_clicks)

        top_content_data = {
            'impressions': [{'message': row.message, 'image': row.full_picture_url, 'value': row.metric_value or 0} for row in top_impressions],
            'likes': [{'message': row.message, 'image': row.full_picture_url, 'value': row.metric_value or 0} for row in top_likes],
            'clicks': [{'message': row.message, 'image': row.full_picture_url, 'value': row.metric_value or 0} for row in top_clicks]
        }
        
        # === 7. TỔNG HỢP VÀ TRẢ VỀ (Giữ nguyên) ===
        response_data = {
            'scorecards': scorecards,
            'mainChartData': main_chart_data,
            'interactionsTable': interactions_table,
            'contentTypeTable': content_type_table,
            'topContentData': top_content_data
        }
        
        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu Fanpage Overview: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

@app.route('/api/fanpage/cover', methods=['GET'])
@login_required
def get_fanpage_cover():
    """
    Lấy URL ảnh bìa (cover source) cho một page_id cụ thể.
    Tự động refresh Page Token nếu gặp lỗi 190 (Token hết hạn).
    Thêm HARDCODE FALLBACK nếu API gọi thất bại.
    """
    session = db_manager.SessionLocal()
    
    # 1. Lấy page_id từ query param
    page_id = request.args.get('page_id')
    if not page_id:
        return jsonify({'error': 'Thiếu "page_id" query parameter.'}), 400

    # Dữ liệu Hardcode Fallback
    HARDCODE_COVER_URLS = {
        '111944661954575': '../static/images/gara2_cover.png',
        '631209596914838': '../static/images/spid_cover.png',
        '248822825627335': '../static/images/bbi_cover.png',
        '168476889690455': '../static/images/ego_cover.png',
        '937224289677041': '../static/images/yohe_cover.png',
        '273719346452016': '../static/images/ls2_cover.png'
    }

    try:
        # 2. Lấy Page Access Token đã lưu trong CSDL
        page = session.query(DimFanpage.page_access_token)\
            .filter(DimFanpage.page_id == page_id)\
            .first()
        
        if not page or not page.page_access_token:
            logger.error(f"Không tìm thấy token cho page_id {page_id} trong dim_fanpage.")
            # Chuyển qua Fallback
            if page_id in HARDCODE_COVER_URLS:
                logger.info(f"Sử dụng HARDCODE fallback cho page_id {page_id}")
                return jsonify({'cover_url': HARDCODE_COVER_URLS[page_id]})
            return jsonify({'error': 'Không tìm thấy Fanpage hoặc Page Access Token trong CSDL.'}), 404
        
        page_access_token = page.page_access_token
        base_url = os.getenv("BASE_URL", "https://graph.facebook.com/v24.0")
        
        # 3. Định nghĩa hàm gọi API (để có thể retry)
        def call_api(token_to_use: str) -> dict:
            api_url = f"{base_url}/{page_id}"
            params = {
                'fields': 'cover{source}',
                'access_token': token_to_use
            }
            response = requests.get(api_url, params=params)
            response.raise_for_status() # Ném lỗi (ví dụ: 190) nếu thất bại
            return response.json()

        # 4. Thử gọi API lần 1
        data = None
        try:
            data = call_api(page_access_token)
        
        except requests.exceptions.RequestException as e:
            # Kiểm tra xem có phải lỗi token hết hạn (190) không
            is_token_error = False
            if e.response is not None:
                try:
                    is_token_error = e.response.json().get('error', {}).get('code') == 190
                except Exception:
                    pass # Không phải lỗi JSON
            
            if is_token_error:
                logger.warning(f"Token ảnh bìa cho {page_id} đã hết hạn. Đang làm mới...")
                
                # Import extractor (cần thiết cho logic này)
                from fbads_extract import FacebookAdsExtractor
                extractor = FacebookAdsExtractor()
                
                # B1: Lấy token mới (dùng User Token)
                new_fanpages = extractor.get_all_fanpages()
                
                # B2: Cập nhật CSDL (dùng db_manager đã có)
                db_manager.upsert_fanpages(new_fanpages) # upsert_fanpages tự quản lý session
                
                # B3: Tìm token mới cho page này
                new_token = None
                for new_page in new_fanpages:
                    if new_page.get('id') == page_id:
                        new_token = new_page.get('access_token')
                        break
                        
                if not new_token:
                    logger.error(f"Không tìm thấy token mới cho {page_id} sau khi làm mới.")
                    raise e # Ném lại lỗi gốc để except bên ngoài bắt

                # B4: Thử lại (Lần 2) với token mới
                logger.info(f"Thử lại API ảnh bìa cho {page_id} với token mới...")
                data = call_api(new_token) # Nếu thất bại lần 2, nó sẽ ném lỗi ra ngoài
                
            else: 
                # Không phải lỗi 190, ném lại lỗi gốc để except bên ngoài bắt
                raise e
                
        # 5. Bóc tách và trả về URL (từ 'data' của Lần 1 hoặc Lần 2 thành công)
        cover_url = data.get('cover', {}).get('source')
        
        if not cover_url:
            # Chuyển qua Fallback
            if page_id in HARDCODE_COVER_URLS:
                logger.info(f"Sử dụng HARDCODE fallback cho page_id {page_id} (API trả về không có ảnh bìa).")
                return jsonify({'cover_url': HARDCODE_COVER_URLS[page_id]})
            return jsonify({'error': 'Page này không có ảnh bìa.'}), 404
            
        return jsonify({'cover_url': cover_url})

    except requests.exceptions.RequestException as e:
        # Lỗi này bị bắt nếu:
        # 1. Lần 1 thất bại (với lỗi không phải 190)
        # 2. Lần 2 (retry) cũng thất bại
        logger.error(f"Lỗi API (cuối cùng) khi lấy ảnh bìa cho {page_id}: {e}")
        
        # === HARDCODE FALLBACK NẾU THẤT BẠI HOÀN TOÀN SAU KHI THỬ LẠI ===
        if page_id in HARDCODE_COVER_URLS:
            logger.info(f"Sử dụng HARDCODE fallback cho page_id {page_id} (sau khi API gọi thất bại).")
            return jsonify({'cover_url': HARDCODE_COVER_URLS[page_id]})
            
        if e.response is not None:
            try:
                logger.error(f"Response: {e.response.json()}")
            except:
                 logger.error(f"Response (raw): {e.response.text}")
        return jsonify({'error': 'Lỗi khi gọi Meta API (đã thử lại nếu token hết hạn).'}), 500
    
    except Exception as e:
        logger.error(f"Lỗi nội bộ khi lấy ảnh bìa: {e}", exc_info=True)
        # === HARDCODE FALLBACK NẾU LỖI NỘI BỘ ===
        if page_id in HARDCODE_COVER_URLS:
            logger.info(f"Sử dụng HARDCODE fallback cho page_id {page_id} (sau khi lỗi nội bộ).")
            return jsonify({'cover_url': HARDCODE_COVER_URLS[page_id]})
            
        return jsonify({'error': 'Lỗi server nội bộ.'}), 500
    finally:
        session.close()

# ======================================================================
# API ENDPOINTS - CAMPAIGN ANALYSIS (GEO MAP)
# ======================================================================

@app.route('/api/geo_map_data', methods=['POST'])
@login_required
def get_geo_map_data():
    """
    Tạo và trả về HTML cho một bản đồ Folium dựa trên bộ lọc.
    - Kích thước (radius) = impressions
    - Màu sắc (color) = purchase_value
    """
    if not folium:
        return jsonify({'error': 'Thư viện Folium chưa được cài đặt trên server.'}), 500

    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        
        # === 1. LẤY BỘ LỌC (Giống hệt các endpoint khác) ===
        account_id = data.get('account_id')
        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400
        
        campaign_ids = data.get('campaign_ids')
        adset_ids = data.get('adset_ids')
        ad_ids = data.get('ad_ids')
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        
        # === 2. XÁC ĐỊNH KỲ HIỆN TẠI ===
        start_date, end_date = None, None
        today = datetime.today().date()
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=today)
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            date_preset = 'last_month' # Fallback
            start_date, end_date = _calculate_date_range(date_preset, today)

        # === 3. TRUY VẤN DỮ LIỆU ===
        logger.info(f"Đang truy vấn dữ liệu bản đồ từ {start_date} đến {end_date}...")
        
        query = select(
            DimRegion.region_name,
            DimRegion.latitude,
            DimRegion.longitude,
            func.sum(FactPerformanceRegion.impressions).label('impressions'),
            func.sum(FactPerformanceRegion.purchase_value).label('purchase_value')
        ).join(
            FactPerformanceRegion, FactPerformanceRegion.region_id == DimRegion.region_id
        ).join(
            DimDate, FactPerformanceRegion.date_key == DimDate.date_key
        ).join(
            DimCampaign, FactPerformanceRegion.campaign_id == DimCampaign.campaign_id # Cần join để lọc account_id
        ).filter(
            DimCampaign.ad_account_id == account_id # Lọc theo tài khoản
        ).filter(
            DimDate.full_date.between(start_date, end_date) # Lọc theo ngày
        ).filter(
            DimRegion.latitude != None # Chỉ lấy region có tọa độ
        )

        # Áp dụng các bộ lọc ID nếu có
        filters = []
        if campaign_ids:
            filters.append(FactPerformanceRegion.campaign_id.in_(campaign_ids))
        if adset_ids:
            filters.append(FactPerformanceRegion.adset_id.in_(adset_ids))
        if ad_ids:
            filters.append(FactPerformanceRegion.ad_id.in_(ad_ids))
        
        if filters:
            query = query.where(and_(*filters))

        query = query.group_by(
            DimRegion.region_id, 
            DimRegion.region_name, 
            DimRegion.latitude, 
            DimRegion.longitude
        )

        results = session.execute(query).all()

        if not results:
            logger.warning("Không tìm thấy dữ liệu Geo để vẽ bản đồ.")
            # Trả về bản đồ trống
            m = folium.Map(location=[16.0, 108.0], zoom_start=6, tiles='CartoDB positron')
            return jsonify({'map_html': m._repr_html_()})

        # === 4. CHUẨN BỊ DỮ LIỆU VỚI PANDAS ===
        df = pd.DataFrame(results, columns=['region_name', 'latitude', 'longitude', 'impressions', 'purchase_value'])
        df['impressions'] = pd.to_numeric(df['impressions'])
        df['purchase_value'] = pd.to_numeric(df['purchase_value'])

        # Chuẩn hóa (Normalize) impressions để làm bán kính (radius)
        # Chuyển đổi sang thang đo từ 0-1
        imp_min, imp_max = df['impressions'].min(), df['impressions'].max()
        if imp_max == imp_min:
            df['radius_normalized'] = 1.0 # Nếu tất cả giá trị bằng nhau
        else:
            df['radius_normalized'] = (df['impressions'] - imp_min) / (imp_max - imp_min)
        
        # Ánh xạ thang 0-1 sang thang pixel (ví dụ: 8px đến 40px)
        min_radius, max_radius = 13, 52
        df['radius'] = df['radius_normalized'].apply(lambda x: min_radius + (x * (max_radius - min_radius)))

        # === 5. TẠO THANG MÀU (COLORMAP) CHO PURCHASE VALUE ===
        pv_min, pv_max = df['purchase_value'].min(), df['purchase_value'].max()
        
        # Tạo thang màu (Vàng -> Cam -> Đỏ)
        # Nếu max = min (chỉ có 1 giá trị), đặt 1 màu cố định
        if pv_max == pv_min:
             colormap = cm.linear.YlOrRd_09.scale(pv_min - 1, pv_max + 1)
        else:
             colormap = cm.linear.YlOrRd_09.scale(pv_min, pv_max)
        
        colormap.caption = 'Giá trị Mua hàng (VND)'

        # === 6. TẠO BẢN ĐỒ FOLIUM ===
        logger.info("Đang tạo bản đồ Folium...")
        m = folium.Map(location=[16.0, 108.0], zoom_start=6, tiles='CartoDB positron')
        
        # Tạo một nhóm gom cụm (MarkerCluster)
        marker_cluster = MarkerCluster().add_to(m)

        for _, row in df.iterrows():
            # Tạo popup HTML (giữ nguyên)
            popup_html = f"""
            <div style="font-family: Inter, sans-serif; width: 200px;">
                <h4 style="font-weight: 600; margin: 0 0 5px 0;">{row['region_name']}</h4>
                <p style="margin: 2px 0;">
                    <strong>Impressions:</strong> {row['impressions']:,.0f}
                </p>
                <p style="margin: 2px 0;">
                    <strong>Purchase Value:</strong> {row['purchase_value']:,.0f} VND
                </p>
            </div>
            """
            
            # [SỬA 3] Dùng CircleMarker thay vì Circle, và bỏ * 100
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                popup=popup_html,
                radius=row['radius'], # <-- Đã bỏ * 100 (vì CircleMarker dùng pixel)
                fill=True,
                fill_color=colormap(row['purchase_value']), # Màu dựa trên Purchase Value
                color=colormap(row['purchase_value']), # Viền cùng màu
                fill_opacity=0.6,
                weight=1
            ).add_to(marker_cluster) # [SỬA 4] Thêm vào cluster, không phải 'm'

        m.add_child(colormap) # Thêm thang màu (legend) vào bản đồ

        # === 7. TRẢ VỀ HTML CỦA BẢN ĐỒ ===
        map_html = m._repr_html_()
        return jsonify({'map_html': map_html})

    except Exception as e:
        logger.error(f"Lỗi khi tạo bản đồ Geo: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ khi tạo bản đồ.'}), 500
    finally:
        session.close()

@app.route('/api/camp_performance', methods=['POST'])
@login_required
def get_campaign_performance_data():
    """
    Lấy dữ liệu tổng hợp cho Panel Chiến dịch, bao gồm:
    1. Bảng Giới tính (từ FactPerformanceDemographic)
    2. Bảng Độ tuổi (từ FactPerformanceDemographic)
    3. Bảng Địa lý (từ FactPerformanceRegion)
    4. Biểu đồ tròn Địa lý (từ FactPerformanceRegion)
    """
    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        
        # === 1. LẤY BỘ LỌC ===
        account_id = data.get('account_id')
        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400
        
        # Lấy danh sách campaign_ids (có thể rỗng)
        campaign_ids = data.get('campaign_ids') 
        
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        
        # === 2. XÁC ĐỊNH KỲ HIỆN TẠI (Copy từ endpoint trước) ===
        start_date, end_date = None, None
        today = datetime.today().date()
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=today)
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            date_preset = 'last_month' # Fallback
            start_date, end_date = _calculate_date_range(date_preset, today)

        # === 3. XÂY DỰNG BỘ LỌC CHUNG ===
        
        # Bộ lọc cho bảng FactPerformanceDemographic
        demo_filters = [
            DimCampaign.ad_account_id == account_id,
            DimDate.full_date.between(start_date, end_date)
        ]
        if campaign_ids:
            demo_filters.append(FactPerformanceDemographic.campaign_id.in_(campaign_ids))

        # Bộ lọc cho bảng FactPerformanceRegion
        region_filters = [
            DimCampaign.ad_account_id == account_id,
            DimDate.full_date.between(start_date, end_date)
        ]
        if campaign_ids:
            region_filters.append(FactPerformanceRegion.campaign_id.in_(campaign_ids))

        # === 4. TRUY VẤN DỮ LIỆU ===
        
        # --- Query 1: Bảng Giới tính (gender-table-body) ---
        gender_query = select(
            FactPerformanceDemographic.gender,
            func.sum(FactPerformanceDemographic.impressions).label('impressions'),
            func.sum(FactPerformanceDemographic.clicks).label('clicks'),
            func.sum(FactPerformanceDemographic.purchase_value).label('purchase_value')
        ).join(
            DimDate, FactPerformanceDemographic.date_key == DimDate.date_key
        ).join(
            DimCampaign, FactPerformanceDemographic.campaign_id == DimCampaign.campaign_id
        ).where(
            and_(*demo_filters)
        ).group_by(
            FactPerformanceDemographic.gender
        ).order_by(
            func.sum(FactPerformanceDemographic.purchase_value).desc()
        )
        
        gender_results = session.execute(gender_query).all()
        gender_table = [
            {
                'gender': row.gender or 'Unknown',
                'impressions': row.impressions or 0,
                'clicks': row.clicks or 0,
                'purchase_value': row.purchase_value or 0
            } for row in gender_results
        ]

        # --- Query 2: Bảng Độ tuổi (age-table-body) ---
        age_query = select(
            FactPerformanceDemographic.age,
            func.sum(FactPerformanceDemographic.impressions).label('impressions'),
            func.sum(FactPerformanceDemographic.clicks).label('clicks'),
            func.sum(FactPerformanceDemographic.purchase_value).label('purchase_value')
        ).join(
            DimDate, FactPerformanceDemographic.date_key == DimDate.date_key
        ).join(
            DimCampaign, FactPerformanceDemographic.campaign_id == DimCampaign.campaign_id
        ).where(
            and_(*demo_filters)
        ).group_by(
            FactPerformanceDemographic.age
        ).order_by(
            FactPerformanceDemographic.age.asc()
        )
        
        age_results = session.execute(age_query).all()
        age_table = [
            {
                'age': row.age or 'Unknown',
                'impressions': row.impressions or 0,
                'clicks': row.clicks or 0,
                'purchase_value': row.purchase_value or 0
            } for row in age_results
        ]

        # --- Query 3: Bảng Địa lý (geo-table-body) ---
        cpa_case = case(
            (func.sum(FactPerformanceRegion.purchases) == 0, 0),
            else_=(func.sum(FactPerformanceRegion.spend) / func.sum(FactPerformanceRegion.purchases))
        ).label('cpa')

        geo_query = select(
            DimRegion.region_name,
            func.sum(FactPerformanceRegion.spend).label('spend'),
            func.sum(FactPerformanceRegion.purchases).label('purchases'),
            cpa_case
        ).join(
            DimRegion, FactPerformanceRegion.region_id == DimRegion.region_id
        ).join(
            DimDate, FactPerformanceRegion.date_key == DimDate.date_key
        ).join(
            DimCampaign, FactPerformanceRegion.campaign_id == DimCampaign.campaign_id
        ).where(
            and_(*region_filters)
        ).group_by(
            DimRegion.region_name
        ).order_by(
            func.sum(FactPerformanceRegion.spend).desc()
        )
        
        geo_results = session.execute(geo_query).all()
        geo_table = [
            {
                'region_name': row.region_name or 'Unknown',
                'spend': row.spend or 0,
                'purchases': row.purchases or 0,
                'cpa': row.cpa or 0
            } for row in geo_results
        ]

        # --- Query 4: Biểu đồ tròn Địa lý (region-pie-chart) ---
        # (Sử dụng lại kết quả từ geo_table)
        
        labels = []
        data = []
        
        if geo_table:
            total_spend = sum(row['spend'] for row in geo_table)
            others_spend = 0
            
            if total_spend > 0:
                for row in geo_table:
                    percentage = row['spend'] / total_spend
                    if percentage <= 0.03:
                        others_spend += row['spend']
                    else:
                        labels.append(row['region_name'])
                        data.append(row['spend'])
                
                if others_spend > 0:
                    labels.append('Khác (<= 3%)')
                    data.append(others_spend)

        # Tự động tạo dải màu (giống hàm get_breakdown_chart_data)
        num_labels = len(labels)
        colors = [f'hsl({(i * 360 / num_labels) % 360}, 70%, 60%)' for i in range(num_labels)]

        geo_pie_chart = {
            'labels': labels,
            'datasets': [
                {
                    'data': data,
                    'backgroundColor': colors,
                    'hoverBackgroundColor': colors
                }
            ]
        }

        # === 5. TỔNG HỢP VÀ TRẢ VỀ ===
        response_data = {
            'gender_table': gender_table,
            'age_table': age_table,
            'geo_table': geo_table,
            'geo_pie_chart': geo_pie_chart
        }
        
        return jsonify(response_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu Panel Chiến dịch: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ khi tạo dữ liệu chiến dịch.'}), 500
    finally:
        session.close()

@app.route('/api/age_gender_chart', methods=['POST'])
@login_required
def get_age_gender_chart_data():
    """
    Lấy dữ liệu cho biểu đồ cột đôi (grouped bar):
    - Trục X: Độ tuổi (Age)
    - Trục Y: Impressions
    - Nhóm (Series): Giới tính (Gender)
    """
    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        
        # === 1. LẤY BỘ LỌC (Giống các hàm khác) ===
        account_id = data.get('account_id')
        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400
        
        campaign_ids = data.get('campaign_ids') 
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        
        # === 2. XÁC ĐỊNH KỲ HIỆN TẠI ===
        start_date, end_date = None, None
        today = datetime.today().date()
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=today)
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            date_preset = 'last_month' # Fallback
            start_date, end_date = _calculate_date_range(date_preset, today)

        # === 3. XÂY DỰNG BỘ LỌC CHUNG ===
        demo_filters = [
            DimCampaign.ad_account_id == account_id,
            DimDate.full_date.between(start_date, end_date)
        ]
        if campaign_ids:
            demo_filters.append(FactPerformanceDemographic.campaign_id.in_(campaign_ids))

        # === 4. TRUY VẤN DỮ LIỆU GỐC (Dạng "Long") ===
        # Lấy (Age, Gender, Impressions)
        query = select(
            FactPerformanceDemographic.age,
            FactPerformanceDemographic.gender,
            func.sum(FactPerformanceDemographic.impressions).label('impressions')
        ).join(
            DimDate, FactPerformanceDemographic.date_key == DimDate.date_key
        ).join(
            DimCampaign, FactPerformanceDemographic.campaign_id == DimCampaign.campaign_id
        ).where(
            and_(*demo_filters)
        ).group_by(
            FactPerformanceDemographic.age,
            FactPerformanceDemographic.gender
        ).order_by(
            FactPerformanceDemographic.age.asc(),
            FactPerformanceDemographic.gender.asc()
        )
        
        results = session.execute(query).all()

        if not results:
            return jsonify({'labels': [], 'datasets': []}) # Trả về rỗng

        # === 5. "PIVOT" DỮ LIỆU (Chuyển từ "Long" sang "Wide" cho Chart.js) ===
        
        data_pivot = {} # Cấu trúc: { "18-24": {"female": 100, "male": 110}, "25-34": ... }
        all_ages_set = set()
        all_genders_set = set()

        for row in results:
            age = row.age or 'Unknown'
            gender = row.gender or 'Unknown'
            impressions = row.impressions or 0
            
            all_ages_set.add(age)
            all_genders_set.add(gender)
            
            if age not in data_pivot:
                data_pivot[age] = {}
            
            data_pivot[age][gender] = impressions

        # Sắp xếp các nhãn (Age)
        def age_sort_key(age_str):
            if age_str == 'Unknown':
                return 99
            match = re.match(r'(\d+)', age_str) # Lấy số đầu tiên
            if match:
                return int(match.group(1))
            return 98
            
        sorted_labels = sorted(list(all_ages_set), key=age_sort_key)
        
        # Sắp xếp các nhóm (Gender)
        sorted_genders = sorted(list(all_genders_set)) 
        
        # Dải màu cho các thanh bar (lam, hồng, xám, v.v.)
        colors = ['#3b82f6', '#ec4899', '#6b7280', '#eab308', '#14b8a6']

        # Xây dựng các dataset
        datasets = []
        for i, gender in enumerate(sorted_genders):
            dataset_data = []
            for age_label in sorted_labels:
                # Lấy giá trị impression, nếu không có thì mặc định là 0
                value = data_pivot.get(age_label, {}).get(gender, 0)
                dataset_data.append(value)
            
            datasets.append({
                'label': gender.capitalize(),
                'data': dataset_data,
                'backgroundColor': colors[i % len(colors)] # Chọn màu xoay vòng
            })
            
        chart_data = {
            'labels': sorted_labels,
            'datasets': datasets
        }
        
        return jsonify(chart_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu Age/Gender Chart: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ khi tạo biểu đồ.'}), 500
    finally:
        session.close()

@app.route('/api/drilldown_chart', methods=['POST'])
@login_required
def get_drilldown_chart_data():
    """
    Endpoint cho các biểu đồ drill-down (Top 5).
    
    Endpoint này sẽ trả về Top 5 của một 'dimension' (ví dụ: 'campaign'),
    được xếp hạng bởi một 'primary_metric' (ví dụ: 'impressions'),
    và hiển thị một 'secondary_metric' (ví dụ: 'purchase_value') dưới dạng line.
    
    Tất cả được lọc bởi các filter chính VÀ các filter drill-down (ví dụ: age, gender).
    """
    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        
        # === 1. LẤY BỘ LỌC CHÍNH ===
        account_id = data.get('account_id')
        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400
        
        campaign_ids = data.get('campaign_ids') 
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')

        # === 2. LẤY THAM SỐ DRILL-DOWN ===
        # (Ví dụ: 'campaign', 'adset', hoặc 'ad')
        group_by_dimension = data.get('group_by_dimension', 'campaign') 
        
        # (Ví dụ: 'impressions')
        primary_metric = data.get('primary_metric', 'impressions')
        
        # (Ví dụ: 'purchase_value')
        secondary_metric = data.get('secondary_metric', 'purchase_value') 
        
        # (Ví dụ: {'age': '18-24', 'gender': 'male'})
        drilldown_filters = data.get('drilldown_filters', {}) 

        # === 3. XÁC ĐỊNH KỲ HIỆN TẠI ===
        start_date, end_date = None, None
        today = datetime.today().date()
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=today)
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            date_preset = 'last_month' # Fallback
            start_date, end_date = _calculate_date_range(date_preset, today)

        # === 4. XÁC ĐỊNH BẢNG VÀ CỘT ĐỂ TRUY VẤN ===
        # Logic này xác định xem nên dùng bảng Fact nào
        
        target_fact_table = None
        dim_joins = []
        base_filters = [
            DimDate.full_date.between(start_date, end_date)
        ]

        # Xác định Bảng Fact dựa trên các filter drill-down
        if 'age' in drilldown_filters or 'gender' in drilldown_filters:
            target_fact_table = FactPerformanceDemographic
            dim_joins.append((DimCampaign, FactPerformanceDemographic.campaign_id == DimCampaign.campaign_id))
            base_filters.append(DimCampaign.ad_account_id == account_id)
            if 'age' in drilldown_filters:
                base_filters.append(FactPerformanceDemographic.age == drilldown_filters['age'])
            if 'gender' in drilldown_filters:
                base_filters.append(FactPerformanceDemographic.gender == drilldown_filters['gender'])
                
        elif 'region' in drilldown_filters:
            target_fact_table = FactPerformanceRegion
            dim_joins.append((DimCampaign, FactPerformanceRegion.campaign_id == DimCampaign.campaign_id))
            dim_joins.append((DimRegion, FactPerformanceRegion.region_id == DimRegion.region_id))
            base_filters.append(DimCampaign.ad_account_id == account_id)
            base_filters.append(DimRegion.region_name == drilldown_filters['region'])
        
        else:
            # Fallback về bảng Platform (hoặc bảng mặc định của bạn)
            target_fact_table = FactPerformancePlatform
            dim_joins.append((DimCampaign, FactPerformancePlatform.campaign_id == DimCampaign.campaign_id))
            base_filters.append(DimCampaign.ad_account_id == account_id)

        # Áp dụng bộ lọc campaign (nếu có)
        if campaign_ids:
            base_filters.append(target_fact_table.campaign_id.in_(campaign_ids))

        # === 5. XÁC ĐỊNH DIMENSION VÀ METRICS ĐỂ QUERY ===
        
        # Mapping tên metric sang cột SQLAlchemy
        metric_column_map = {
            'impressions': func.sum(target_fact_table.impressions),
            'purchase_value': func.sum(target_fact_table.purchase_value),
            'spend': func.sum(target_fact_table.spend),
            'clicks': func.sum(target_fact_table.clicks),
            'purchases': func.sum(target_fact_table.purchases)
        }

        # Mapping tên dimension sang cột SQLAlchemy
        dimension_column_map = {
            'campaign': (DimCampaign.name, DimCampaign.campaign_id, DimCampaign),
            'adset': (DimAdset.name, DimAdset.adset_id, DimAdset),
            'ad': (DimAd.name, DimAd.ad_id, DimAd),
            'region': (DimRegion.region_name, DimRegion.region_id, DimRegion)
        }
        
        if group_by_dimension not in dimension_column_map:
            return jsonify({'error': f"Dimension '{group_by_dimension}' không được hỗ trợ."}), 400
        if primary_metric not in metric_column_map:
            return jsonify({'error': f"Metric chính '{primary_metric}' không được hỗ trợ."}), 400
        if secondary_metric not in metric_column_map:
            return jsonify({'error': f"Metric phụ '{secondary_metric}' không được hỗ trợ."}), 400
            
        # Lấy cột dimension
        dim_name_col, dim_id_col, dim_model = dimension_column_map[group_by_dimension]
        
        # Lấy cột metric
        primary_metric_agg = metric_column_map[primary_metric].label('primary_metric')
        secondary_metric_agg = metric_column_map[secondary_metric].label('secondary_metric')

        # === 6. XÂY DỰNG VÀ THỰC THI TRUY VẤN ===
        
        query = select(
            dim_name_col.label('dimension_label'),
            primary_metric_agg,
            secondary_metric_agg
        ).join(
            DimDate, target_fact_table.date_key == DimDate.date_key
        )
        
        # Thêm các join cần thiết (ví dụ: DimCampaign)
        for join_model, join_condition in dim_joins:
            query = query.join(join_model, join_condition)
            
        # Join với bảng Dimension mà chúng ta đang group by (nếu nó chưa được join)
        # (Ví dụ: nếu group by 'adset', chúng ta cần join Fact -> Adset)
        if dim_model not in [j[0] for j in dim_joins]:
            fk_on_fact = getattr(target_fact_table, f"{group_by_dimension}_id")
            query = query.join(dim_model, fk_on_fact == dim_id_col)

        # Áp dụng filter, group by, order, và limit
        query = query.where(
            and_(*base_filters)
        ).group_by(
            dim_id_col,
            dim_name_col
        ).order_by(
            primary_metric_agg.desc().nullslast() # Sắp xếp theo metric chính
        ).limit(5) # Top 5

        logger.info(f"Đang thực thi truy vấn drill-down: {str(query)}")
        results = session.execute(query).all()

        if not results:
            return jsonify({'labels': [], 'datasets': []})

        # === 7. ĐỊNH DẠNG KẾT QUẢ (COMBO CHART) ===
        labels = []
        bar_data = []  # Metric chính (ví dụ: Impressions)
        line_data = [] # Metric phụ (ví dụ: Purchase Value)
        
        for row in results:
            labels.append(row.dimension_label or 'Không xác định')
            bar_data.append(float(row.primary_metric) if row.primary_metric is not None else 0)
            line_data.append(float(row.secondary_metric) if row.secondary_metric is not None else 0)

        chart_data = {
            'labels': labels,
            'datasets': [
                {
                    'type': 'bar',
                    'label': primary_metric.replace('_', ' ').capitalize(),
                    'data': bar_data,
                    'backgroundColor': '#3b82f6', # Màu lam
                    'yAxisID': 'y' # Trục Y chính
                },
                {
                    'type': 'line',
                    'label': secondary_metric.replace('_', ' ').capitalize(),
                    'data': line_data,
                    'borderColor': '#ec4899', # Màu hồng
                    'backgroundColor': 'rgba(236, 72, 153, 0.1)',
                    'fill': True,
                    'tension': 0.4,
                    'yAxisID': 'y1' # Trục Y phụ
                }
            ]
        }
        
        return jsonify(chart_data)

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu drill-down chart: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ khi tạo biểu đồ drill-down.'}), 500
    finally:
        session.close()

@app.route('/api/waffle_chart', methods=['POST'])
@login_required
def get_waffle_chart_data():
    """
    [MỚI] Lấy dữ liệu và tạo biểu đồ Waffle cho Purchase Value theo Campaign.
    Label được xử lý theo logic string splitting.
    """
    if not plt or not Waffle:
        return jsonify({'error': 'Thư viện Matplotlib/PyWaffle chưa được cài đặt trên server.'}), 500

    session = db_manager.SessionLocal()
    try:
        data = request.get_json()
        
        # === 1. LẤY BỘ LỌC ===
        account_id = data.get('account_id')
        if not account_id:
            return jsonify({'error': 'Thiếu account_id.'}), 400
        
        campaign_ids = data.get('campaign_ids') 
        start_date_input = data.get('start_date')
        end_date_input = data.get('end_date')
        date_preset = data.get('date_preset')
        
        # === 2. XÁC ĐỊNH KỲ HIỆN TẠI ===
        start_date, end_date = None, None
        today = datetime.today().date()
        if date_preset and date_preset in DATE_PRESET:
            start_date, end_date = _calculate_date_range(date_preset=date_preset, today=today)
        elif start_date_input and end_date_input:
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d').date()
        else:
            date_preset = 'last_month' # Fallback
            start_date, end_date = _calculate_date_range(date_preset, today)

        # === 3. XÂY DỰNG BỘ LỌC CHUNG ===
        # Dùng bảng Demo
        base_filters = [
            DimCampaign.ad_account_id == account_id,
            DimDate.full_date.between(start_date, end_date)
        ]
        if campaign_ids:
            base_filters.append(FactPerformanceDemographic.campaign_id.in_(campaign_ids))
        
        # === 4. LOGIC XỬ LÝ LABEL (SQLAlchemy CASE) ===
        # Đếm số lượng dấu gạch dưới '_'
        num_underscores = (
            func.length(DimCampaign.name) - 
            func.length(func.replace(DimCampaign.name, '_', ''))
        )
        
        # Tạo câu lệnh CASE
        # func.split_part
        label_case_statement = case(
            (num_underscores >= 3, func.split_part(DimCampaign.name, '_', 4)), # 4+ yếu tố, lấy cái thứ 4
            (num_underscores == 2, func.split_part(DimCampaign.name, '_', 3)), # 3 yếu tố, lấy cái thứ 3 (cuối)
            else_=DimCampaign.name # Fallback
        ).label('campaign_label')

        # === 5. TRUY VẤN DỮ LIỆU ===
        query = select(
            label_case_statement,
            func.sum(FactPerformanceDemographic.purchase_value).label('total_value')
        ).join(
            DimDate, FactPerformanceDemographic.date_key == DimDate.date_key
        ).join(
            DimCampaign, FactPerformanceDemographic.campaign_id == DimCampaign.campaign_id
        ).where(
            and_(*base_filters)
        ).group_by(
            label_case_statement
        ).order_by(
            func.sum(FactPerformanceDemographic.purchase_value).desc()
        )
        
        results = session.execute(query).all()

        if not results:
            return jsonify({'error': 'Không có dữ liệu Purchase Value cho Waffle Chart.'}), 404

        # === 6. TẠO BIỂU ĐỒ BẰNG PYWAFFLE ===
        
        # Chuyển đổi dữ liệu (và loại bỏ giá trị 0 hoặc âm)
        data_dict = {
            row.campaign_label: float(row.total_value) 
            for row in results if row.total_value and row.total_value > 0
        }

        if not data_dict:
             return jsonify({'error': 'Không có dữ liệu Purchase Value > 0.'}), 404

        # Tính tổng để Waffle biết 1 ô vuông = bao nhiêu
        total_value = sum(data_dict.values())

        fig = plt.figure(
            FigureClass=Waffle,
            rows=7,
            columns=25,
            values=data_dict,
            block_arranging_style='snake',
            legend={
                'loc': 'upper left',
                'bbox_to_anchor': (1, 1), # Vẫn giữ legend bên ngoài
                'ncol': 1, # Legend 1 hàng
                'framealpha': 0,
                'fontsize': 9
            },
            figsize=(12, 5)
        )
        
        # === 7. CHUYỂN BIỂU ĐỒ SANG BASE64 ===
        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        plt.close(fig) # Đóng figure để giải phóng bộ nhớ
        
        return jsonify({'image_base64': f'data:image/png;base64,{img_base64}'})

    except Exception as e:
        logger.error(f"Lỗi khi tạo Waffle chart: {e}", exc_info=True)
        return jsonify({'error': 'Lỗi server nội bộ khi tạo biểu đồ waffle.'}), 500
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True)