/**
 * main.js
 * File JavaScript chính cho Meta Ads Dashboard
 * Chức năng:
 * 1. Khởi tạo các dropdown (Tom Select) và biểu đồ.
 * 2. Tải dữ liệu động cho các dropdown phụ thuộc (cascading).
 * 3. Xử lý logic bộ lọc và các nút bấm ("Làm mới", "Áp dụng").
 * 4. Render dữ liệu lên giao diện (KPIs, biểu đồ, bảng).
 */

// --- BIẾN TOÀN CỤC ---
let spendTrendChartInstance = null;
let platformChartInstance = null;
let tsAccount, tsTime, tsCampaigns, tsAdsets, tsAds;

// --- HÀM KHỞI CHẠY KHI TRANG ĐƯỢC TẢI ---
document.addEventListener('DOMContentLoaded', () => {
    feather.replace();
    initializeCharts();
    initializeTomSelect();
    setupEventListeners();
    loadAccountDropdown(); // Tải tài khoản ngay khi bắt đầu
});

// --- KHỞI TẠO GIAO DIỆN ---

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
    const tomSelectSettings = {
        create: false,
        valueField: 'id',    // Báo TomSelect dùng trường 'id' làm giá trị
        labelField: 'text',  // Báo TomSelect dùng trường 'text' làm nhãn
        searchField: 'text'  // Cho phép tìm kiếm bằng trường 'text'
    };
    // Cài đặt cho dropdown chọn MỘT
    const singleSelectSettings = (placeholder) => ({
        ...tomSelectSettings,
        placeholder: placeholder,
    });

    // Cài đặt cho dropdown chọn NHIỀU
    const multiSelectSettings = (placeholder) => ({
        plugins: ['remove_button', 'checkbox_options'], // Thêm checkbox cho dễ nhìn
        ...tomSelectSettings,
        placeholder: placeholder,
    });

    // 1. Dropdown Tài khoản (chọn một)
    tsAccount = new TomSelect('#filter-ad-account', singleSelectSettings('Chọn tài khoản...'));

    // 2. Dropdown Thời gian (chọn một)
    tsTime = new TomSelect('#filter-time', { ...singleSelectSettings(), allowEmptyOption: false });

    // 3. Dropdown Chiến dịch (chọn nhiều)
    tsCampaigns = new TomSelect('#filter-campaigns', multiSelectSettings('Chọn chiến dịch...'));

    // Các dropdown con khác
    tsAdsets = new TomSelect('#filter-adsets', multiSelectSettings('Chọn nhóm quảng cáo...'));
    tsAds = new TomSelect('#filter-ads', multiSelectSettings('Chọn quảng cáo...'));

    // Vô hiệu hóa các dropdown con ban đầu
    tsCampaigns.disable();
    tsAdsets.disable();
    tsAds.disable();
}

// --- GẮN CÁC BỘ LẮNG NGHE SỰ KIỆN ---

function setupEventListeners() {
    document.getElementById('btn-refresh-data').addEventListener('click', handleRefreshData);
    document.getElementById('btn-apply-filters').addEventListener('click', handleApplyFilters);

    /**
     * Yêu cầu 2: Khi chọn thời gian là "Tùy chỉnh", hiện ra bảng chọn ngày
     */
    tsTime.on('change', (value) => {
        const customDateRange = document.getElementById('custom-date-range');
        if (value === 'custom') {
            customDateRange.classList.remove('hidden');
        } else {
            customDateRange.classList.add('hidden');
        }
        // Khi thời gian thay đổi, tải lại danh sách chiến dịch
        triggerCampaignLoad();
    });

    /**
     * Yêu cầu 3: Khi chọn tài khoản, tải lại danh sách chiến dịch
     */
    tsAccount.on('change', () => {
        triggerCampaignLoad();
    });

    // Logic dropdown phụ thuộc (cascading)
    tsCampaigns.on('change', (selectedCampaigns) => {
        resetDropdown(tsAdsets, 'Chọn nhóm quảng cáo...');
        resetDropdown(tsAds, 'Chọn quảng cáo...');
        if (selectedCampaigns && selectedCampaigns.length > 0) {
            loadAdsetDropdown(selectedCampaigns);
            tsAdsets.enable();
        }
    });

    tsAdsets.on('change', (selectedAdsets) => {
        resetDropdown(tsAds, 'Chọn quảng cáo...');
        if (selectedAdsets && selectedAdsets.length > 0) {
            loadAdDropdown(selectedAdsets);
            tsAds.enable();
        }
    });
}

// --- CÁC HÀM XỬ LÝ SỰ KIỆN ---

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
    tsAccount.clear(true);
    tsAccount.clearOptions();
    tsAccount.disable();
    tsAccount.control_input.placeholder = 'Đang tải tài khoản...';
    try {
        // 1. Tải dữ liệu trực tiếp (KHÔNG dùng ts.load())
        const response = await fetch('/api/accounts');
        if (!response.ok) throw new Error('Lỗi mạng khi tải tài khoản');
        const accounts = await response.json();
        
        // 2. Map dữ liệu (Code này của bạn đã đúng)
        const options = accounts.map(c => ({ id: c.id, text: c.name }));

        // 3. Thêm dữ liệu vào TomSelect
        tsAccount.addOptions(options);
        tsAccount.enable(); // Bật lại
        tsAccount.control_input.placeholder = 'Đang tải tài khoản...';

        // 4. Tự động chọn tài khoản đầu tiên và tải campaign
        if (options.length > 0) {
            // Đặt giá trị một cách "im lặng" (silent = true) để tránh kích hoạt sự kiện 'change'
            // Chúng ta sẽ gọi triggerCampaignLoad() thủ công ngay sau đây.
            tsAccount.setValue(options[0].id, true); 
            
            // Kích hoạt tải campaign thủ công sau khi đã chọn xong
            triggerCampaignLoad();
        }
    } catch (error) {
        console.error('Lỗi tải tài khoản:', error);
        tsAccount.enable();
        tsAccount.control_input.placeholder = 'Lỗi tải tài khoản...';
    }
}

async function loadCampaignDropdown(accountId, dateParams) {
    resetDropdown(tsCampaigns, 'Đang tải chiến dịch...');
    try {
        const response = await fetch('/api/campaigns', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ account_id: accountId, ...dateParams })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const campaigns = await response.json();
        // Đổi key 'campaign_id' thành 'id' và 'name' thành 'text' cho Tom-Select
        const options = campaigns.map(c => ({ id: c.campaign_id, text: c.name }));
        // Thêm dữ liệu vào Tom Select
        tsCampaigns.addOptions(options);
        tsCampaigns.enable();
        tsCampaigns.control_input.placeholder = 'Chọn chiến dịch...';
    } catch (error) {
        console.error('Lỗi tải chiến dịch:', error);
        tsCampaigns.enable();
        tsCampaigns.control_input.placeholder = 'Lỗi tải chiến dịch...';
    }
}


async function loadAdsetDropdown(campaignIds) {
    resetDropdown(tsAdsets, 'Đang tải nhóm QC...');
    try {
        const response = await fetch('/api/adsets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ campaign_ids: campaignIds, ...getDateFilterParams() })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const adsets = await response.json();
        const options = adsets.map(a => ({ id: a.adset_id, text: a.name }));
        tsAdsets.addOptions(options);
        tsAdsets.enable();
        tsAdsets.control_input.placeholder = 'Chọn nhóm quảng cáo...';
    } catch (error) {
        console.error('Lỗi tải nhóm QC:', error);
        tsAdsets.enable();
        tsAdsets.control_input.placeholder = 'Lỗi tải nhóm QC...';
    }
}

async function loadAdDropdown(adsetIds) {
    resetDropdown(tsAds, 'Đang tải quảng cáo...');
    try {
        const response = await fetch('/api/ads', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ adset_ids: adsetIds, ...getDateFilterParams() })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const ads = await response.json();
        const options = ads.map(ad => ({ id: ad.ad_id, text: ad.name }));
        tsAds.addOptions(options);
        tsAds.enable();
        tsAds.control_input.placeholder = 'Chọn quảng cáo...';
    } catch (error) {
        console.error('Lỗi tải quảng cáo:', error);
        tsAds.enable();
        tsAds.control_input.placeholder = 'Lỗi tải quảng cáo...';
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
    tomSelectInstance.control_input.placeholder = placeholder;

    tomSelectInstance.addOptions(options);
    tomSelectInstance.refreshOptions(false);
}

/**
 * Helper để reset và vô hiệu hóa Tom Select
 */
function resetDropdown(tomSelectInstance, placeholder) {
    if (tomSelectInstance) {
        tomSelectInstance.clear(true);
        tomSelectInstance.clearOptions();
        tomSelectInstance.control_input.placeholder = placeholder;
        tomSelectInstance.disable();
    }
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

/**
 * Kích hoạt việc tải lại dropdown Campaigns.
 * Đây là hàm trung gian để gom logic lấy account_id và dateParams.
 */
function triggerCampaignLoad() {
    const accountId = tsAccount.getValue();
    const dateParams = getDateFilterParams();
    console.log("triggerCampaignLoad được gọi. Giá trị dateParams:", dateParams);
    // Reset các dropdown con
    resetDropdown(tsCampaigns, 'Chọn chiến dịch...');
    resetDropdown(tsAdsets, 'Chọn nhóm quảng cáo...');
    resetDropdown(tsAds, 'Chọn quảng cáo...');

    if (accountId && dateParams) {
        loadCampaignDropdown(accountId, dateParams);
        tsCampaigns.enable();
    }
}