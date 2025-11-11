/**
 * main.js (Phiên bản Multi-select)
 * File JavaScript chính cho Meta Ads Dashboard
 * * * Chức năng:
 * 1. Khởi tạo biểu đồ (với dữ liệu rỗng).
 * 2. Khởi tạo Tom Select cho multi-select dropdowns.
 * 3. Tải danh sách tài khoản, chiến dịch, adset, ad (dropdowns phụ thuộc).
 * 4. Xử lý logic của bộ lọc (filters), bao gồm ẩn/hiện ngày tùy chỉnh.
 * 5. Gắn hành động cho nút "Làm mới" (gọi /api/refresh).
 * 6. Gắn hành động cho nút "Áp dụng" (gọi /api/overview_data và /api/chart_data).
 * 7. Render (vẽ) dữ liệu trả về lên các thẻ KPI, bảng, và biểu đồ.
 */

// --- BIẾN TOÀN CỤC CHO BIỂU ĐỒ & DROPDOWNS ---
let spendTrendChartInstance = null;
let platformChartInstance = null;

// Biến lưu trữ các instance của Tom Select
let tsAccount = null;
// let tsBrand = null; // Tạm thời vô hiệu hóa Brand
let tsTime = null;
let tsCampaigns = null;
let tsAdsets = null;
let tsAds = null;

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

/**
 * Khởi tạo Tom Select cho các dropdown
 */
function initializeTomSelect() {
    const singleSelectSettings = (placeholder) => ({
        create: false,
        placeholder: placeholder
    });
    
    const multiSelectSettings = (placeholder) => ({
        plugins: ['remove_button'],
        maxItems: 20, // Cho phép chọn tối đa 20 mục
        create: false,
        placeholder: placeholder
    });

    // Account (Single select)
    tsAccount = new TomSelect('#filter-ad-account', singleSelectSettings('Chọn tài khoản...'));
    
    // Time (Single select)
    tsTime = new TomSelect('#filter-time', {
        ...singleSelectSettings('30 ngày qua'),
        allowEmptyOption: false
    });
    
    // Brand (Tạm thời vô hiệu hóa)
    // tsBrand = new TomSelect('#filter-brand', multiSelectSettings('Tất cả Brand...'));
    // tsBrand.disable(); 

    // Campaigns (Multi-select)
    tsCampaigns = new TomSelect('#filter-campaigns', multiSelectSettings('Tất cả Campaigns...'));

    // Adsets (Multi-select)
    tsAdsets = new TomSelect('#filter-adsets', multiSelectSettings('Tất cả Adsets...'));

    // Ads (Multi-select)
    tsAds = new TomSelect('#filter-ads', multiSelectSettings('Tất cả Ads...'));

    // Vô hiệu hóa các dropdown phụ thuộc ban đầu
    tsCampaigns.disable();
    tsAdsets.disable();
    tsAds.disable();
}


// --- GẮN SỰ KIỆN ---
function setupEventListeners() {
    // 1. Nút "Làm mới" (đổi ID từ file HTML của bạn)
    document.getElementById('btn-refresh-data').addEventListener('click', handleRefreshData);

    // 2. Nút "Áp dụng"
    document.getElementById('btn-apply-filters').addEventListener('click', handleApplyFilters);

    // 3. Logic Ẩn/Hiện Ngày Tùy Chỉnh
    tsTime.on('change', (value) => {
        const customDateRange = document.getElementById('custom-date-range');
        if (value === 'custom') {
            customDateRange.classList.remove('hidden');
        } else {
            customDateRange.classList.add('hidden');
        }
        
        // Khi thời gian thay đổi, kích hoạt tải lại Campaigns
        triggerCampaignLoad();
    });

    // 4. Logic Dropdown Phụ Thuộc (dùng Tom Select 'change' event)
    tsAccount.on('change', (value) => {
        const accountId = value;
        // Reset các cấp con trước khi tải
        resetDropdown(tsCampaigns, 'Đang tải Campaigns...');
        resetDropdown(tsAdsets, 'Chọn Chiến dịch trước');
        resetDropdown(tsAds, 'Chọn Adset trước');

        if (accountId) {
            loadCampaignDropdown(accountId);
            tsCampaigns.enable();
        }
    });
    
    tsCampaigns.on('change', (value) => {
        // 'value' bây giờ là một MẢNG (array), ví dụ: ['123', '456']
        resetDropdown(tsAdsets, 'Đang tải Adsets...');
        resetDropdown(tsAds, 'Chọn Adset trước');

        if (value && value.length > 0) {
            loadAdsetDropdown(value); // Gửi mảng IDs
            tsAdsets.enable();
        } else {
            resetDropdown(tsAdsets, 'Chọn Chiến dịch trước');
        }
    });
    
    tsAdsets.on('change', (value) => {
        // 'value' cũng là một mảng
        resetDropdown(tsAds, 'Đang tải Ads...');

        if (value && value.length > 0) {
            loadAdDropdown(value); // Gửi mảng IDs
            tsAds.enable();
        } else {
            resetDropdown(tsAds, 'Chọn Adset trước');
        }
    });
    
    // (Tùy chọn) Kích hoạt tải lại Campaigns khi ngày tùy chỉnh thay đổi
    document.getElementById('date-from').addEventListener('change', triggerCampaignLoad);
    document.getElementById('date-to').addEventListener('change', triggerCampaignLoad);
}

// --- LOGIC CHÍNH ---

/**
 * Task 1: Xử lý nút "Làm mới"
 */
async function handleRefreshData() {
    const button = document.getElementById('btn-refresh-data');
    const originalText = button.querySelector('span').innerText;
    setButtonLoading(button, 'Đang tải...');

    try {
        const dateParams = getDateFilterParams(true); // Lấy ngày cho việc "Tải"
        if (dateParams === null) throw new Error("Ngày không hợp lệ.");
        
        const response = await fetch('/api/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(dateParams)
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Lỗi khi làm mới dữ liệu từ server.');
        }

        const result = await response.json();
        showNotification(result.message || 'Làm mới dữ liệu thành công!', 'success');
        
        // Tải lại các dropdown (vì dữ liệu Dimensions có thể đã thay đổi)
        triggerCampaignLoad();
        
        // Tự động "Áp dụng" bộ lọc
        handleApplyFilters(); 

    } catch (error) {
        console.error('Lỗi khi tải dữ liệu:', error);
        showNotification(`Lỗi khi tải dữ liệu: ${error.message}`, 'error');
    } finally {
        setButtonIdle(button, originalText);
    }
}

/**
 * Task 3: Xử lý nút "Áp dụng"
 */
async function handleApplyFilters() {
    const button = document.getElementById('btn-apply-filters');
    const originalText = button.querySelector('span').innerText;
    setButtonLoading(button, 'Đang tải...');

    // 1. Lấy tất cả giá trị filter
    const filters = getFilterPayload();
    
    if (!filters) {
        // Lỗi đã được hiển thị bởi getFilterPayload
        setButtonIdle(button, originalText);
        return;
    }

    try {
        // 2. Gọi song song API
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

        if (!overviewRes.ok) {
            const err = await overviewRes.json();
            throw new Error(`Lỗi overview: ${err.error}`);
        }
        if (!chartRes.ok) {
            const err = await chartRes.json();
            throw new Error(`Lỗi chart: ${err.error}`);
        }

        const overviewData = await overviewRes.json();
        const chartData = await chartRes.json();

        // 3. Render dữ liệu
        renderOverviewData(overviewData.scorecards);
        renderChartData(chartData);
        
        // (Tùy chọn: Tải bảng dữ liệu chi tiết, hiện chưa có API)
        document.getElementById('campaign-table-body').innerHTML = `
            <tr>
                <td colspan="6" class="py-4 px-4 text-center text-gray-500">Đã tải xong dữ liệu tổng quan.</td>
            </tr>
        `;

    } catch (error) {
        console.error('Lỗi khi áp dụng bộ lọc:', error);
        showNotification(`Lỗi khi lấy dữ liệu: ${error.message}`, 'error');
    } finally {
        setButtonIdle(button, originalText);
    }
}

// --- CÁC HÀM TẢI DROPDOWN (ĐÃ CẬP NHẬT CHO TOM SELECT) ---

async function loadAccountDropdown() {
    try {
        const response = await fetch('/api/accounts');
        if (!response.ok) throw new Error('Lỗi tải tài khoản');
        const accounts = await response.json();
        
        populateDropdown(tsAccount, accounts, 'Chọn tài khoản...', 'id', 'name', false); // false = không có "Tất cả"

    } catch (error) {
        console.error('Lỗi tải Account dropdown:', error);
        resetDropdown(tsAccount, 'Lỗi tải tài khoản', false);
    }
}

async function loadCampaignDropdown(accountId, dateParams) {
    try {
        const response = await fetch('/api/campaigns', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                'account_id': accountId,
                ...dateParams
            })
        });
        if (!response.ok) throw new Error('Lỗi tải chiến dịch');
        const campaigns = await response.json();
        
        populateDropdown(tsCampaigns, campaigns, 'Tất cả Campaigns...', 'campaign_id', 'name', true);

    } catch (error) {
        console.error('Lỗi tải Campaign dropdown:', error);
        resetDropdown(tsCampaigns, 'Lỗi tải chiến dịch', true);
    }
}

async function loadAdsetDropdown(campaignIds) {
    try {
        const dateParams = getDateFilterParams(true);
        if (!dateParams) return; 

        const response = await fetch('/api/adsets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                'campaign_ids': campaignIds, // Gửi mảng
                ...dateParams
            })
        });
        if (!response.ok) throw new Error('Lỗi tải Adset');
        const adsets = await response.json();
        
        populateDropdown(tsAdsets, adsets, 'Tất cả Adsets...', 'adset_id', 'name', true);

    } catch (error) {
        console.error('Lỗi tải Adset dropdown:', error);
        resetDropdown(tsAdsets, 'Lỗi tải Adset', true);
    }
}

async function loadAdDropdown(adsetIds) {
    try {
        const dateParams = getDateFilterParams(true);
        if (!dateParams) return;

        const response = await fetch('/api/ads', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                'adset_ids': adsetIds, // Gửi mảng
                ...dateParams
            })
        });
        if (!response.ok) throw new Error('Lỗi tải Ads');
        const ads = await response.json();
        
        populateDropdown(tsAds, ads, 'Tất cả Ads...', 'ad_id', 'name', true);

    } catch (error) {
        console.error('Lỗi tải Ad dropdown:', error);
        resetDropdown(tsAds, 'Lỗi tải Ads', true);
    }
}


// --- CÁC HÀM RENDER DỮ LIỆU ---

/**
 * Render dữ liệu vào các thẻ KPI và Bảng
 */
function renderOverviewData(data) {
    // Helper để định dạng số
    const formatNumber = (num) => new Intl.NumberFormat('vi-VN').format(Math.round(num || 0));
    const formatCurrency = (num) => new Intl.NumberFormat('vi-VN', { style: 'currency', currency: 'VND' }).format(Math.round(num || 0));
    const formatPercent = (num) => `${parseFloat(num || 0).toFixed(2)}%`;

    // Hàng 1: Thẻ KPI (Sử dụng ID từ file HTML của bạn)
    const kpiSpend = document.getElementById('kpi-total-spend');
    const kpiImpressions = document.getElementById('kpi-total-impressions');
    const kpiCtr = document.getElementById('kpi-avg-ctr');
    const kpiPurchases = document.getElementById('kpi-total-purchases');

    if (kpiSpend) kpiSpend.innerText = formatCurrency(data.total_spend);
    if (kpiImpressions) kpiImpressions.innerText = formatNumber(data.total_impressions);
    if (kpiCtr) kpiCtr.innerText = formatPercent(data.ctr);
    if (kpiPurchases) kpiPurchases.innerText = formatNumber(data.total_purchases);
    
    // Hàng 3, Cột 1: Phễu
    document.getElementById('funnel-total-cost').innerText = formatCurrency(data.total_spend);
    document.getElementById('funnel-impressions').innerText = formatNumber(data.total_impressions);
    document.getElementById('funnel-post-engagement').innerText = formatNumber(data.total_post_engagement);
    document.getElementById('funnel-clicks').innerText = formatNumber(data.total_clicks);
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
    
    // Xóa các text (n/a)
    document.querySelectorAll('p[id$="-growth"]').forEach(el => el.innerHTML = '');
}

/**
 * Task 4: Render dữ liệu vào biểu đồ đường
 */
function renderChartData(chartData) {
    if (spendTrendChartInstance) {
        spendTrendChartInstance.data.labels = chartData.labels;
        spendTrendChartInstance.data.datasets = chartData.datasets;
        spendTrendChartInstance.update();
    }
    // (Code render biểu đồ tròn (platform) sẽ cần API riêng)
}


// --- CÁC HÀM TRỢ GIÚP (HELPERS) ---

/**
 * Lấy payload bộ lọc cuối cùng để gửi cho API
 */
function getFilterPayload() {
    const filters = {};
    
    // 1. Lấy Account ID
    filters.account_id = tsAccount.getValue();
    if (!filters.account_id) {
        showNotification('Vui lòng chọn một Tài khoản Quảng cáo.', 'error');
        return null;
    }

    // 2. Lấy Ngày
    const dateParams = getDateFilterParams();
    if (dateParams === null) {
        return null; // Lỗi (ví dụ: ngày tùy chỉnh không hợp lệ)
    }
    filters.date_preset = dateParams.date_preset;
    filters.start_date = dateParams.start_date;
    filters.end_date = dateParams.end_date;

    // 3. Lấy IDs (Campaigns, Adsets, Ads)
    const campaignIds = tsCampaigns.getValue();
    if (campaignIds.length > 0) {
        filters.campaign_ids = campaignIds;
    }
    
    const adsetIds = tsAdsets.getValue();
    if (adsetIds.length > 0) {
        filters.adset_ids = adsetIds;
    }
    
    const adIds = tsAds.getValue();
    if (adIds.length > 0) {
        filters.ad_ids = adIds;
    }

    return filters;
}

/**
 * Lấy ngày tháng dựa trên bộ lọc
 * @param {boolean} forRefresh - Nếu là 'true', dùng ngày hôm nay làm end_date.
 * @returns {object} - { date_preset, start_date, end_date }
 */
function getDateFilterParams(forRefresh = false) {
    const timeFilter = document.getElementById('filter-time');
    let date_preset = timeFilter.value; // (VD: "last_30d")
    let start_date = document.getElementById('date-from').value;
    let end_date = document.getElementById('date-to').value;
    const today = new Date().toISOString().split('T')[0];
    
    if (forRefresh && date_preset !== 'custom') {
        end_date = today;
    }

    if (date_preset !== 'custom') {
        start_date = null; 
    } else {
        if (!start_date || !end_date) {
            showNotification('Tùy chỉnh: Vui lòng chọn Từ ngày và Đến ngày!', 'error');
            return null; // Dừng lại
        }
        date_preset = null; // Gửi ngày tùy chỉnh, không gửi preset
    }

    if (!end_date) {
        end_date = today;
    }
    
    // Logic của app.py sẽ xử lý (date_preset, start_date, end_date)
    if (date_preset) {
        // Gửi end_date để app.py tính toán preset
        return { date_preset: date_preset, end_date: end_date };
    } else {
        // Gửi ngày tùy chỉnh
        return { date_preset: null, start_date: start_date, end_date: end_date };
    }
}

/**
 * Helper để điền dữ liệu vào Tom Select
 */
function populateDropdown(tomSelectInstance, data, placeholder, valueKey, nameKey, includeAllOption) {
    tomSelectInstance.clearOptions();
    
    // Chuyển đổi {campaign_id: "1", name: "A"} -> {value: "1", text: "A"}
    let options = data.map(item => ({
        value: item[valueKey],
        text: item[nameKey]
    }));
    
    // Tom Select không dùng 'all'. Placeholder sẽ xử lý việc "Tất cả"
    tomSelectInstance.setPlaceholder(placeholder);

    tomSelectInstance.addOptions(options);
    tomSelectInstance.refreshOptions(false);
}

/**
 * Helper để reset và vô hiệu hóa Tom Select
 */
function resetDropdown(tomSelectInstance, placeholder) {
    tomSelectInstance.clearOptions();
    tomSelectInstance.clear(); // Xóa các giá trị đã chọn
    tomSelectInstance.setPlaceholder(placeholder);
    tomSelectInstance.disable();
}

/**
 * Helper để hiển thị trạng thái loading cho nút
 */
function setButtonLoading(button, loadingText) {
    button.disabled = true;
    const span = button.querySelector('span');
    if (span) span.innerText = loadingText;
    button.classList.add('button-loading');
}

/**
 * Helper để trả lại trạng thái bình thường cho nút
 */
function setButtonIdle(button, originalText) {
    button.disabled = false;
    const span = button.querySelector('span');
    if (span) span.innerText = originalText;
    button.classList.remove('button-loading');
}

/**
 * Helper để hiển thị thông báo (tạm thời dùng alert)
 */
function showNotification(message, type = 'info') {
    // Tạm thời dùng alert, bạn có thể thay bằng thư viện "Toast"
    if (type === 'error') {
        console.error(message);
        alert(`LỖI: ${message}`);
    } else {
        console.log(message);
        alert(message);
    }
}