/**
 * main.js
 * File JavaScript chính cho Meta Ads Dashboard (ĐÃ HOÀN THIỆN)
 *
 * Chức năng:
 * 1. Khởi tạo Tom Select cho tất cả các dropdown.
 * 2. Tải danh sách tài khoản, chiến dịch, adset, ad (dropdowns phụ thuộc).
 * 3. Xử lý logic của bộ lọc (filters), bao gồm ẩn/hiện ngày tùy chỉnh.
 * 4. Gắn hành động cho nút "Tải Dữ liệu" (gọi /api/refresh).
 * 5. Gắn hành động cho nút "Áp dụng" (gọi /api/overview_data và /api/chart_data).
 * 6. Render (vẽ) dữ liệu trả về lên các thẻ KPI, bảng, và biểu đồ.
 */

// --- BIẾN TOÀN CỤC CHO BIỂU ĐỒ ---
let spendTrendChartInstance = null;
let platformChartInstance = null;

// --- BIẾN TOÀN CỤC CHO TOM SELECT ---
// Chúng ta cần lưu trữ các instance của Tom Select để điều khiển chúng
let tsAccount, tsTime, tsBrand, tsCampaigns, tsAdsets, tsAds;

// --- HÀM KHỞI TẠO (CHẠY KHI TẢI TRANG) ---
document.addEventListener('DOMContentLoaded', () => {
    // Kích hoạt icon
    feather.replace();

    // Khởi tạo các biểu đồ (với dữ liệu rỗng)
    initializeCharts();
    
    // Khởi tạo Tom Select cho các dropdown
    initializeTomSelect();

    // Gắn các trình xử lý sự kiện (event listeners)
    setupEventListeners();

    // Tải dữ liệu ban đầu cho dropdown tài khoản
    loadAccountDropdown();
});

// --- KHỞI TẠO BIỂU ĐỒ ---
function initializeCharts() {
    // 1. Biểu đồ đường (Spend & Impressions)
    const ctxSpend = document.getElementById('spendTrendChart').getContext('2d');
    spendTrendChartInstance = new Chart(ctxSpend, {
        type: 'line',
        data: { labels: [], datasets: [] }, // Dữ liệu rỗng
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            scales: {
                y: {
                    type: 'linear', display: true, position: 'left',
                    title: { display: true, text: 'Chi tiêu (Spend)' }
                },
                y1: {
                    type: 'linear', display: true, position: 'right',
                    title: { display: true, text: 'Hiển thị (Impressions)' },
                    grid: { drawOnChartArea: false },
                },
            }
        }
    });

    // 2. Biểu đồ tròn (Platform)
    const ctxPlatform = document.getElementById('platformChart').getContext('2d');
    platformChartInstance = new Chart(ctxPlatform, {
        type: 'doughnut',
        data: {
            labels: ['Chưa có dữ liệu'],
            datasets: [{
                label: 'Phân bổ',
                data: [1],
                backgroundColor: ['#E5E7EB'], // Màu xám
                hoverOffset: 4
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'bottom' },
                title: { display: true, text: 'Vui lòng áp dụng bộ lọc' }
            }
        }
    });
}

// --- KHỞI TẠO TOM SELECT ---
function initializeTomSelect() {
    const tsSettings = {
        create: false,
        sortField: {
            field: "text",
            direction: "asc"
        }
    };

    tsAccount = new TomSelect('#filter-ad-account', {...tsSettings, placeholder: 'Chọn tài khoản...'});
    tsTime = new TomSelect('#filter-time', { ...tsSettings, allowEmptyOption: false }); // Không cần placeholder
    
    // Brand (tạm thời, vì chưa có API)
    tsBrand = new TomSelect('#filter-brand', {...tsSettings, placeholder: 'Tất cả Brand'});
    tsBrand.disable(); // Vô hiệu hóa vì chưa dùng

    // Các dropdown phụ thuộc, khởi tạo là "disabled"
    tsCampaigns = new TomSelect('#filter-campaigns', {...tsSettings, placeholder: 'Tất cả Campaigns'});
    tsCampaigns.disable();
    
    tsAdsets = new TomSelect('#filter-adsets', {...tsSettings, placeholder: 'Tất cả Adsets'});
    tsAdsets.disable();
    
    tsAds = new TomSelect('#filter-ads', {...tsSettings, placeholder: 'Tất cả Ads'});
    tsAds.disable();
}


// --- GẮN SỰ KIỆN ---
function setupEventListeners() {
    // 1. Nút "Tải Dữ liệu" (Task 1)
    document.getElementById('btn-download-data').addEventListener('click', handleRefreshData);

    // 2. Nút "Áp dụng" (Task 3)
    document.getElementById('btn-apply-filters').addEventListener('click', handleApplyFilters);

    // 3. Logic Ẩn/Hiện Ngày Tùy Chỉnh (dùng Tom Select event)
    tsTime.on('change', (value) => {
        const customDateRange = document.getElementById('custom-date-range');
        if (value === 'custom') {
            customDateRange.classList.remove('hidden');
        } else {
            customDateRange.classList.add('hidden');
        }
    });

    // 4. Logic Dropdown Phụ Thuộc (dùng Tom Select event)
    tsAccount.on('change', (value) => {
        if (value) {
            loadCampaignDropdown(value);
            // Kích hoạt dropdown
            tsCampaigns.enable();
        } else {
            // Vô hiệu hóa các dropdown con
            resetDropdown(tsCampaigns, 'Tất cả Campaigns');
            resetDropdown(tsAdsets, 'Tất cả Adsets');
            resetDropdown(tsAds, 'Tất cả Ads');
        }
    });
    
    tsCampaigns.on('change', (value) => {
        if (value && value !== 'all') {
            loadAdsetDropdown([value]); // app.py mong đợi một danh sách
            tsAdsets.enable();
        } else {
            resetDropdown(tsAdsets, 'Tất cả Adsets');
            resetDropdown(tsAds, 'Tất cả Ads');
        }
    });
    
    tsAdsets.on('change', (value) => {
        if (value && value !== 'all') {
            loadAdDropdown([value]); // app.py mong đợi một danh sách
            tsAds.enable();
        } else {
            resetDropdown(tsAds, 'Tất cả Ads');
        }
    });
}

// --- LOGIC CHÍNH ---

/**
 * Task 1: Xử lý nút "Tải Dữ liệu"
 * Gọi API /api/refresh để yêu cầu backend chạy ETL
 */
async function handleRefreshData() {
    const button = document.getElementById('btn-download-data');
    const originalText = button.querySelector('span').innerText;
    
    // Hiển thị trạng thái loading
    button.disabled = true;
    button.querySelector('span').innerText = 'Đang tải...';
    button.classList.add('button-loading');

    try {
        // Lấy ngày từ bộ lọc để gửi cho backend
        const dateParams = getDateFilterParams(); 
        
        // **SỬA LỖI:** Kiểm tra nếu dateParams là null (do lỗi nhập ngày tùy chỉnh)
        if (!dateParams) {
             throw new Error('Ngày tùy chỉnh không hợp lệ.');
        }
        
        const response = await fetch('/api/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(dateParams) // Gửi ngày tháng đi
        });

        if (!response.ok) {
            const errData = await response.json();
            throw new Error(errData.error || 'Lỗi khi làm mới dữ liệu từ server.');
        }

        const result = await response.json();
        alert(result.message || 'Làm mới dữ liệu thành công!'); // Tạm thời dùng alert

        // Sau khi tải xong, tự động "Áp dụng" bộ lọc
        handleApplyFilters(); 

    } catch (error) {
        console.error('Lỗi khi tải dữ liệu:', error);
        alert(`Lỗi khi tải dữ liệu: ${error.message}`);
    } finally {
        // Trả lại trạng thái bình thường
        button.disabled = false;
        button.querySelector('span').innerText = originalText;
        button.classList.remove('button-loading');
    }
}

/**
 * Task 3: Xử lý nút "Áp dụng"
 * Tập hợp tất cả bộ lọc và gọi các API để render dashboard
 */
async function handleApplyFilters() {
    const button = document.getElementById('btn-apply-filters');
    const originalText = button.querySelector('span').innerText;
    
    // Hiển thị trạng thái loading
    button.disabled = true;
    button.querySelector('span').innerText = 'Đang tải...';
    button.classList.add('button-loading');

    try {
        // 1. Lấy tất cả giá trị filter
        const filters = getFilterPayload();
        
        // **SỬA LỖI:** Kiểm tra nếu filters là null (do lỗi ngày tháng)
        if (!filters) {
            throw new Error('Vui lòng kiểm tra lại bộ lọc ngày.');
        }

        if (!filters.account_id) {
            alert('Vui lòng chọn một Tài khoản Quảng cáo.');
            throw new Error('Chưa chọn tài khoản QC.');
        }

        // 2. Gọi song song API overview và API chart
        const [overviewRes, chartRes] = await Promise.all([
            fetch('/api/overview_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters)
            }),
            fetch('/api/chart_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters)
            })
        ]);

        if (!overviewRes.ok || !chartRes.ok) {
            throw new Error('Lỗi khi lấy dữ liệu dashboard.');
        }

        const overviewData = await overviewRes.json();
        const chartData = await chartRes.json();

        // 3. Render dữ liệu
        renderOverviewData(overviewData.scorecards); // Render các thẻ KPI
        renderChartData(chartData); // Task 4: Vẽ biểu đồ

    } catch (error) {
        console.error('Lỗi khi áp dụng bộ lọc:', error);
        if (error.message !== 'Chưa chọn tài khoản QC.' && error.message !== 'Vui lòng kiểm tra lại bộ lọc ngày.') {
             alert('Lỗi khi lấy dữ liệu. Vui lòng kiểm tra console.');
        }
    } finally {
        // Trả lại trạng thái bình thường
        button.disabled = false;
        button.querySelector('span').innerText = originalText;
        button.classList.remove('button-loading');
    }
}

// --- CÁC HÀM TẢI DROPDOWN (Đã sửa cho Tom Select) ---

async function loadAccountDropdown() {
    try {
        const response = await fetch('/api/accounts');
        if (!response.ok) throw new Error('Lỗi tải tài khoản');
        
        const accounts = await response.json();
        
        // Dùng Tom Select API
        populateDropdown(tsAccount, accounts, 'Chọn tài khoản...', 'id', 'name', false); // false = không có "Tất cả"

    } catch (error) {
        console.error('Lỗi tải Account dropdown:', error);
    }
}

async function loadCampaignDropdown(accountId) {
    try {
        const dateParams = getDateFilterParams();
        if (!dateParams) return; // Dừng nếu ngày không hợp lệ
        
        const response = await fetch('/api/campaigns', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                'account_id': accountId,
                'start_date': dateParams.start_date,
                'end_date': dateParams.end_date
            })
        });
        if (!response.ok) throw new Error('Lỗi tải chiến dịch');
        
        const campaigns = await response.json();
        populateDropdown(tsCampaigns, campaigns, 'Tất cả Campaigns', 'campaign_id', 'name', true);

    } catch (error) {
        console.error('Lỗi tải Campaign dropdown:', error);
    }
}

async function loadAdsetDropdown(campaignIds) {
    try {
        const dateParams = getDateFilterParams();
        if (!dateParams) return; 
        
        const response = await fetch('/api/adsets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                'campaign_ids': campaignIds,
                'start_date': dateParams.start_date,
                'end_date': dateParams.end_date
            })
        });
        if (!response.ok) throw new Error('Lỗi tải Adset');
        
        const adsets = await response.json();
        populateDropdown(tsAdsets, adsets, 'Tất cả Adsets', 'adset_id', 'name', true);

    } catch (error) {
        console.error('Lỗi tải Adset dropdown:', error);
    }
}

async function loadAdDropdown(adsetIds) {
    try {
        const dateParams = getDateFilterParams();
        if (!dateParams) return; 
        
        const response = await fetch('/api/ads', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                'adset_ids': adsetIds,
                'start_date': dateParams.start_date,
                'end_date': dateParams.end_date
            })
        });
        if (!response.ok) throw new Error('Lỗi tải Ads');
        
        const ads = await response.json();
        populateDropdown(tsAds, ads, 'Tất cả Ads', 'ad_id', 'name', true);

    } catch (error) {
        console.error('Lỗi tải Ad dropdown:', error);
    }
}


// --- CÁC HÀM RENDER DỮ LIỆU ---

/**
 * Task 3 (con): Render dữ liệu vào các thẻ KPI
 * @param {object} data - Đối tượng scorecards từ /api/overview_data
 */
function renderOverviewData(data) {
    // Helper để định dạng số
    const formatNumber = (num) => new Intl.NumberFormat('vi-VN').format(Math.round(num));
    const formatCurrency = (num) => new Intl.NumberFormat('vi-VN', { style: 'currency', currency: 'VND' }).format(Math.round(num));
    const formatPercent = (num) => `${parseFloat(num).toFixed(2)}%`;

    // Hàng 1: Thẻ KPI (Lấy ID từ HTML của bạn)
    document.getElementById('kpi-total-spend').innerText = formatCurrency(data.total_spend);
    document.getElementById('kpi-total-impressions').innerText = formatNumber(data.total_impressions);
    document.getElementById('kpi-avg-ctr').innerText = formatPercent(data.ctr);
    document.getElementById('kpi-total-purchases').innerText = formatNumber(data.total_purchases);
    
    // Hàng 3, Cột 1: Phễu
    document.getElementById('funnel-total-cost').innerText = formatCurrency(data.total_spend);
    document.getElementById('funnel-impressions').innerText = formatNumber(data.total_impressions);
    document.getElementById('funnel-post-engagement').innerText = formatNumber(data.total_post_engagement);
    document.getElementById('funnel-clicks').innerText = formatNumber(data.total_clicks); // `app.py` trả về `total_clicks`
    document.getElementById('funnel-messaging').innerText = formatNumber(data.total_messages);
    
    // Tính toán và điền KPI phụ của phễu
    document.getElementById('funnel-cpm').innerText = `CPM: ${formatCurrency(data.avg_cpm)}`;
    const cpe = data.total_post_engagement > 0 ? (data.total_spend / data.total_post_engagement) : 0;
    document.getElementById('funnel-cpe').innerText = `Cost/Eng: ${formatCurrency(cpe)}`;
    const cpc = data.total_clicks > 0 ? (data.total_spend / data.total_clicks) : 0;
    document.getElementById('funnel-cpc').innerText = `CPC: ${formatCurrency(cpc)}`;
    const cpmc = data.total_messages > 0 ? (data.total_spend / data.total_messages) : 0;
    document.getElementById('funnel-cpmc').innerText = `Cost/Msg: ${formatCurrency(cpmc)}`;

    // Hàng 3, Cột 2: Bảng KPI chi tiết
    document.getElementById('kpi-detail-impressions').innerText = formatNumber(data.total_impressions);
    document.getElementById('kpi-detail-reach').innerText = formatNumber(data.total_reach);
    document.getElementById('kpi-detail-ctr').innerText = formatPercent(data.ctr);
    document.getElementById('kpi-detail-post-engagement').innerText = formatNumber(data.total_post_engagement);
    document.getElementById('kpi-detail-link-click').innerText = formatNumber(data.total_link_click);
    document.getElementById('kpi-detail-messages').innerText = formatNumber(data.total_messages);
    document.getElementById('kpi-detail-purchases').innerText = formatNumber(data.total_purchases);
    document.getElementById('kpi-detail-purchase-value').innerText = formatCurrency(data.total_purchase_value);
    
    // (Bỏ qua phần growth rate % vì app.py chưa tính)
}

/**
 * Task 4: Render dữ liệu vào biểu đồ đường
 * @param {object} chartData - Dữ liệu từ /api/chart_data
 */
function renderChartData(chartData) {
    if (spendTrendChartInstance) {
        spendTrendChartInstance.data.labels = chartData.labels;
        spendTrendChartInstance.data.datasets = chartData.datasets;
        spendTrendChartInstance.update();
    }
}


// --- CÁC HÀM TRỢ GIÚP (HELPERS) ---

/**
 * Lấy payload bộ lọc để gửi cho API
 * @returns {object}
 */
function getFilterPayload() {
    const filters = {};
    
    // 1. Lấy Account ID (dùng Tom Select API)
    filters.account_id = tsAccount.getValue();

    // 2. Lấy Ngày (Task 2)
    const dateParams = getDateFilterParams();
    if (!dateParams) {
        return null; // Dừng lại nếu ngày không hợp lệ
    }
    filters.date_preset = dateParams.date_preset;
    filters.start_date = dateParams.start_date;
    filters.end_date = dateParams.end_date;

    // 3. Lấy IDs (Campaigns, Adsets, Ads) (dùng Tom Select API)
    // app.py mong đợi danh sách (list)
    const campaignId = tsCampaigns.getValue();
    if (campaignId && campaignId !== 'all') {
        filters.campaign_ids = [campaignId];
    }
    
    const adsetId = tsAdsets.getValue();
    if (adsetId && adsetId !== 'all') {
        filters.adset_ids = [adsetId];
    }
    
    const adId = tsAds.getValue();
    if (adId && adId !== 'all') {
        filters.ad_ids = [adId];
    }

    return filters;
}

/**
 * Task 2 (con): Lấy ngày tháng dựa trên bộ lọc
 * @returns {object} - { date_preset, start_date, end_date }
 */
function getDateFilterParams() {
    let date_preset = tsTime.getValue();
    let start_date = document.getElementById('date-from').value;
    let end_date = document.getElementById('date-to').value;

    // **SỬA LỖI:** Dịch giá trị từ HTML (last_30_days) sang giá trị backend (last_30d)
    if (date_preset === 'last_30_days') {
        date_preset = 'last_30d';
    } else if (date_preset === 'last_90_days') {
        date_preset = 'last_90d';
    }
    // (Bạn có thể thêm các else if khác nếu cần, ví dụ 'today' đã khớp)

    if (date_preset !== 'custom') {
        // Nếu không phải 'custom', xóa ngày tùy chỉnh
        start_date = null;
        // end_date = null; // Giữ lại end_date cho backend tính toán
    } else {
        // Nếu là 'custom'
        if (!start_date || !end_date) {
            alert('Vui lòng chọn Từ ngày và Đến ngày!');
            return null; // Dừng lại
        }
        date_preset = null; // Xóa preset
    }

    // Backend `app.py` cần end_date (là hôm nay) để tính toán preset
    if (!end_date) {
        end_date = new Date().toISOString().split('T')[0];
    }
    
    // app.py của bạn mong đợi 'end_date' là ngày kết thúc, 
    // và `_calculate_date_range` dùng `end_date_input` cho 'today'
    // nên chúng ta gửi `end_date` trong cả hai trường hợp
    return { date_preset, start_date, end_date };
}

/**
 * Helper để điền dữ liệu vào dropdown (dùng Tom Select)
 */
function populateDropdown(tsInstance, data, defaultOptionText, valueKey, nameKey, includeAllOption = true) {
    tsInstance.clear(); // Xóa giá trị đang chọn
    tsInstance.clearOptions(); // Xóa tất cả option cũ

    const options = [];
    
    if (includeAllOption) {
        options.push({ [valueKey]: 'all', [nameKey]: defaultOptionText });
    }

    data.forEach(item => {
        options.push({
            [valueKey]: item[valueKey],
            [nameKey]: item[nameKey]
        });
    });

    tsInstance.addOptions(options);
    if (includeAllOption) {
        tsInstance.setValue('all'); // Chọn "Tất cả" làm mặc định
    } else if (data.length === 0 && !includeAllOption) {
         // Nếu là dropdown tài khoản và không có dữ liệu
        tsInstance.addOption({ [valueKey]: '', [nameKey]: 'Không tìm thấy tài khoản' });
        tsInstance.disable();
    }
    
    // Nếu là dropdown tài khoản, không vô hiệu hóa
    if (includeAllOption) {
        tsInstance.enable();
    }
}

/**
 * Helper để reset và vô hiệu hóa dropdown (dùng Tom Select)
 */
function resetDropdown(tsInstance, defaultOptionText) {
    tsInstance.clear();
    tsInstance.clearOptions();
    tsInstance.addOption({ value: 'all', text: defaultOptionText });
    tsInstance.setValue('all');
    tsInstance.disable();
}