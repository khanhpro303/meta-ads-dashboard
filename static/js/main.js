/**
 * main.js
 * File JavaScript chính cho Meta Ads Dashboard
 * CẤU TRÚC KẾT HỢP:
 * - Sử dụng Select2 cho single-select (Account, Time).
 * - Sử dụng MultiselectDropdown.js cho multi-select (Campaigns, Adsets, Ads).
 */

// --- BIẾN TOÀN CỤC ---
let spendTrendChartInstance = null;
let platformChartInstance = null;

// Biến cho Select2
let s2Account, s2Time;

// Biến cho các <select> DOM element (cho multi-select)
let elCampaigns, elAdsets, elAds;

// --- HÀM KHỞI CHẠY KHI TRANG ĐƯỢC TẢI ---
document.addEventListener('DOMContentLoaded', () => {
    
    // [QUAN TRỌNG]
    // Khởi tạo UI của thư viện multiselect-dropdown TRƯỚC TIÊN
    // để sửa lỗi race condition.
    MultiselectDropdown(window.MultiselectDropdownOptions);

    // Giờ mới chạy các hàm logic của dashboard
    feather.replace();
    initializeCharts();
    initializeSelects(); 
    setupEventListeners();
    loadAccountDropdown(); // Tải tài khoản ngay khi bắt đầu
});

// --- KHỞI TẠO GIAO DIỆN ---

function initializeCharts() {
    // (Giữ nguyên, không thay đổi)
    const ctxSpend = document.getElementById('spendTrendChart').getContext('2d');
    spendTrendChartInstance = new Chart(ctxSpend, {
        type: 'line',
        data: { labels: [], datasets: [] },
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            elements: { line: { fill: true } },
            scales: {
                y: { type: 'linear', display: true, position: 'left', title: { display: true, text: 'Chi tiêu (Spend)' } },
                y1: { type: 'linear', display: true, position: 'right', title: { display: true, text: 'Hiển thị (Impressions)' }, grid: { drawOnChartArea: false } },
            }
        }
    });
    const ctxPlatform = document.getElementById('platformChart').getContext('2d');
    platformChartInstance = new Chart(ctxPlatform, {
        type: 'doughnut',
        data: {
            labels: ['Chưa có dữ liệu'],
            datasets: [{ label: 'Phân bổ', data: [1], backgroundColor: ['#E5E7EB'], hoverOffset: 4 }]
        },
        options: {
            responsive: true,
            plugins: { legend: { position: 'bottom' }, title: { display: true, text: 'Vui lòng áp dụng bộ lọc' } }
        }
    });
}

/**
 * Khởi tạo các dropdown
 * - Select2 cho single-select
 * - Lấy DOM element cho multi-select (thư viện đã tự khởi tạo UI)
 */
function initializeSelects() {
    // Cài đặt chung cho Select2
    const singleSelectSettings = (placeholder) => ({
        placeholder: placeholder,
        width: '100%',
    });

    // 1. Dropdown Tài khoản (Dùng Select2)
    s2Account = $('#filter-ad-account').select2(singleSelectSettings('Chọn tài khoản...'));

    // 2. Dropdown Thời gian (Dùng Select2)
    s2Time = $('#filter-time').select2({
        minimumResultsForSearch: Infinity
    });

    // 3. Lấy DOM elements cho các multi-select
    elCampaigns = document.getElementById('filter-campaigns');
    elAdsets = document.getElementById('filter-adsets');
    elAds = document.getElementById('filter-ads');

    // Vô hiệu hóa các dropdown con ban đầu
    elCampaigns.disabled = true;
    elAdsets.disabled = true;
    elAds.disabled = true;
    
    // Yêu cầu thư viện cập nhật UI theo trạng thái "disabled"
    // Hàm .loadOptions() đã tồn tại vì MultiselectDropdown() đã chạy
    elCampaigns.loadOptions(); //
    elAdsets.loadOptions();
    elAds.loadOptions();
}

// --- GẮN CÁC BỘ LẮNG NGHE SỰ KIỆN ---

function setupEventListeners() {
    $('#btn-refresh-data').on('click', handleRefreshData);
    $('#btn-apply-filters').on('click', handleApplyFilters);

    // Sự kiện cho Select2
    s2Time.on('change', (e) => {
        const value = $(e.currentTarget).val();
        const customDateRange = document.getElementById('custom-date-range');
        if (value === 'custom') {
            customDateRange.classList.remove('hidden');
        } else {
            customDateRange.classList.add('hidden');
        }
        triggerCampaignLoad();
    });

    s2Account.on('change', () => {
        triggerCampaignLoad();
    });

    // Sự kiện cho multi-select (dùng DOM addEventListener)
    // Thư viện multiselect-dropdown sẽ tự kích hoạt sự kiện 'change'
    // trên thẻ <select> thật khi người dùng click UI giả
    
    elCampaigns.addEventListener('change', () => {
        const selectedCampaigns = Array.from(elCampaigns.selectedOptions).map(o => o.value);
        
        resetDropdown(elAdsets);
        resetDropdown(elAds);
        
        if (selectedCampaigns && selectedCampaigns.length > 0) {
            loadAdsetDropdown(selectedCampaigns);
            elAdsets.disabled = false;
            elAdsets.loadOptions(); // Cập nhật UI sau khi enabled
        }
    });

    elAdsets.addEventListener('change', () => {
        const selectedAdsets = Array.from(elAdsets.selectedOptions).map(o => o.value);
        
        resetDropdown(elAds);
        
        if (selectedAdsets && selectedAdsets.length > 0) {
            loadAdDropdown(selectedAdsets);
            elAds.disabled = false;
            elAds.loadOptions(); // Cập nhật UI sau khi enabled
        }
    });

    elAds.addEventListener('change', () => {
        // Không cần làm gì khi chọn Ads
    });
}

// --- CÁC HÀM XỬ LÝ SỰ KIỆN ---

async function handleRefreshData() {
    // (Giữ nguyên, không thay đổi)
    const button = document.getElementById('btn-refresh-data');
    const originalText = button.querySelector('span').innerText;
    setButtonLoading(button, 'Đang tải...');

    try {
        const dateParams = getDateFilterParams(true);
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
        
        triggerCampaignLoad();
        handleApplyFilters(); 

    } catch (error) {
        console.error('Lỗi khi tải dữ liệu:', error);
        showNotification(`Lỗi khi tải dữ liệu: ${error.message}`, 'error');
    } finally {
        setButtonIdle(button, originalText);
    }
}

async function handleApplyFilters() {
    // (Giữ nguyên, không thay đổi)
    const button = document.getElementById('btn-apply-filters');
    const originalText = button.querySelector('span').innerText;
    setButtonLoading(button, 'Đang tải...');

    const filters = getFilterPayload();
    
    if (!filters) {
        setButtonIdle(button, originalText);
        return;
    }

    try {
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

        renderOverviewData(overviewData.scorecards);
        renderChartData(chartData);
        
        const tableBody = document.getElementById('campaign-table-body');
        if (tableBody) {
            tableBody.innerHTML = `<tr><td colspan="6" class="py-4 px-4 text-center text-gray-500">Đã tải xong dữ liệu tổng quan.</td></tr>`;
        }

    } catch (error) {
        console.error('Lỗi khi áp dụng bộ lọc:', error);
        showNotification(`Lỗi khi lấy dữ liệu: ${error.message}`, 'error');
    } finally {
        setButtonIdle(button, originalText);
    }
}

// --- CÁC HÀM TẢI DROPDOWN ---

async function loadAccountDropdown() {
    // (Giữ nguyên) - Dùng Select2
    s2Account.empty().append(new Option('Đang tải tài khoản...', '')).prop('disabled', true).trigger('change');
    
    try {
        const response = await fetch('/api/accounts');
        if (!response.ok) throw new Error('Lỗi mạng khi tải tài khoản');
        const accounts = await response.json();
        
        s2Account.empty();
        
        accounts.forEach(c => {
            const idString = String(c.id);
            const lastFourDigits = idString.slice(-4);
            const newText = `${c.name} (${lastFourDigits})`;
            const option = new Option(newText, c.id);
            s2Account.append(option);
        });

        s2Account.prop('disabled', false).trigger('change');

        if (accounts.length > 0) {
            s2Account.val(accounts[0].id).trigger('change');
        }
    } catch (error) {
        console.error('Lỗi tải tài khoản:', error);
        s2Account.empty().append(new Option('Lỗi tải tài khoản', '')).prop('disabled', true).trigger('change');
    }
}

async function loadCampaignDropdown(accountId, dateParams) {
    // Dùng cho DOM element
    setDropdownLoading(elCampaigns, 'Đang tải chiến dịch...');
    
    try {
        const response = await fetch('/api/campaigns', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ account_id: accountId, ...dateParams })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const campaigns = await response.json();

        // 1. Cập nhật thẻ <select> THẬT (đang bị ẩn)
        elCampaigns.innerHTML = '';
        campaigns.forEach(c => {
            const option = new Option(c.name, c.campaign_id);
            elCampaigns.appendChild(option);
        });
        
        elCampaigns.disabled = false;
        
        // 2. Ra lệnh cho thư viện "vẽ lại" UI GIẢ
        elCampaigns.loadOptions();

    } catch (error) {
        console.error('Lỗi tải chiến dịch:', error);
        setDropdownLoading(elCampaigns, 'Lỗi tải chiến dịch...');
    }
}


async function loadAdsetDropdown(campaignIds) {
    // Dùng cho DOM element
    setDropdownLoading(elAdsets, 'Đang tải nhóm QC...');
    
    try {
        const response = await fetch('/api/adsets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ campaign_ids: campaignIds, ...getDateFilterParams() })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const adsets = await response.json();
        
        // 1. Cập nhật thẻ <select> THẬT
        elAdsets.innerHTML = '';
        adsets.forEach(a => {
            const option = new Option(a.name, a.adset_id);
            elAdsets.appendChild(option);
        });
        
        elAdsets.disabled = false;
        
        // 2. Ra lệnh "vẽ lại" UI GIẢ
        elAdsets.loadOptions();
        
    } catch (error) {
        console.error('Lỗi tải nhóm QC:', error);
        setDropdownLoading(elAdsets, 'Lỗi tải nhóm QC...');
    }
}

async function loadAdDropdown(adsetIds) {
    // Dùng cho DOM element
    setDropdownLoading(elAds, 'Đang tải quảng cáo...');
    
    try {
        const response = await fetch('/api/ads', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ adset_ids: adsetIds, ...getDateFilterParams() })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const ads = await response.json();
        
        // 1. Cập nhật thẻ <select> THẬT
        elAds.innerHTML = '';
        ads.forEach(ad => {
            const option = new Option(ad.name, ad.ad_id);
            elAds.appendChild(option);
        });

        elAds.disabled = false;
        
        // 2. Ra lệnh "vẽ lại" UI GIẢ
        elAds.loadOptions();
        
    } catch (error) {
        console.error('Lỗi tải quảng cáo:', error);
        setDropdownLoading(elAds, 'Lỗi tải quảng cáo...');
    }
}


// --- CÁC HÀM RENDER DỮ LIỆU (Giữ nguyên, không thay đổi) ---

function renderOverviewData(data) {
    const formatNumber = (num) => new Intl.NumberFormat('vi-VN').format(Math.round(num || 0));
    const formatCurrency = (num) => new Intl.NumberFormat('vi-VN', { style: 'currency', currency: 'VND' }).format(Math.round(num || 0));
    const formatPercent = (num) => `${parseFloat(num || 0).toFixed(2)}%`;
    const kpiSpend = document.getElementById('kpi-total-spend');
    const kpiImpressions = document.getElementById('kpi-total-impressions');
    const kpiCtr = document.getElementById('kpi-avg-ctr');
    const kpiPurchases = document.getElementById('kpi-total-purchases');
    if (kpiSpend) kpiSpend.innerText = formatCurrency(data.total_spend);
    if (kpiImpressions) kpiImpressions.innerText = formatNumber(data.total_impressions);
    if (kpiCtr) kpiCtr.innerText = formatPercent(data.ctr);
    if (kpiPurchases) kpiPurchases.innerText = formatNumber(data.total_purchases);
    const elFunnelCost = document.getElementById('funnel-total-cost');
    if (elFunnelCost) elFunnelCost.innerText = formatCurrency(data.total_spend);
    const elFunnelImp = document.getElementById('funnel-impressions');
    if (elFunnelImp) elFunnelImp.innerText = formatNumber(data.total_impressions);
    const elFunnelEng = document.getElementById('funnel-post-engagement');
    if (elFunnelEng) elFunnelEng.innerText = formatNumber(data.total_post_engagement);
    const elFunnelClicks = document.getElementById('funnel-clicks');
    if (elFunnelClicks) elFunnelClicks.innerText = formatNumber(data.total_clicks);
    const elFunnelMsg = document.getElementById('funnel-messaging');
    if (elFunnelMsg) elFunnelMsg.innerText = formatNumber(data.total_messages);
    const elFunnelCpm = document.getElementById('funnel-cpm');
    if (elFunnelCpm) elFunnelCpm.innerText = `CPM: ${formatCurrency(data.avg_cpm)}`;
    const cpe = data.total_post_engagement > 0 ? (data.total_spend / data.total_post_engagement) : 0;
    const elFunnelCpe = document.getElementById('funnel-cpe');
    if (elFunnelCpe) elFunnelCpe.innerText = `Cost/Eng: ${formatCurrency(cpe)}`;
    const cpc = data.total_clicks > 0 ? (data.total_spend / data.total_clicks) : 0;
    const elFunnelCpc = document.getElementById('funnel-cpc');
    if (elFunnelCpc) elFunnelCpc.innerText = `CPC: ${formatCurrency(cpc)}`;
    const cpmc = data.total_messages > 0 ? (data.total_spend / data.total_messages) : 0;
    const elFunnelCpmc = document.getElementById('funnel-cpmc');
    if (elFunnelCpmc) elFunnelCpmc.innerText = `Cost/Msg: ${formatCurrency(cpmc)}`;
    const elDetailImp = document.getElementById('kpi-detail-impressions');
    if (elDetailImp) elDetailImp.innerText = formatNumber(data.total_impressions);
    const elDetailReach = document.getElementById('kpi-detail-reach');
    if (elDetailReach) elDetailReach.innerText = formatNumber(data.total_reach);
    const elDetailCtr = document.getElementById('kpi-detail-ctr');
    if (elDetailCtr) elDetailCtr.innerText = formatPercent(data.ctr);
    const elDetailEng = document.getElementById('kpi-detail-post-engagement');
    if (elDetailEng) elDetailEng.innerText = formatNumber(data.total_post_engagement);
    const elDetailLinkClick = document.getElementById('kpi-detail-link-click');
    if (elDetailLinkClick) elDetailLinkClick.innerText = formatNumber(data.total_link_click);
    const elDetailMsg = document.getElementById('kpi-detail-messages');
    if (elDetailMsg) elDetailMsg.innerText = formatNumber(data.total_messages);
    const elDetailPurch = document.getElementById('kpi-detail-purchases');
    if (elDetailPurch) elDetailPurch.innerText = formatNumber(data.total_purchases);
    const elDetailPurchVal = document.getElementById('kpi-detail-purchase-value');
    if (elDetailPurchVal) elDetailPurchVal.innerText = formatCurrency(data.total_purchase_value);
    document.querySelectorAll('p[id$="-growth"]').forEach(el => {
        if (el) el.innerHTML = '';
    });
}

function renderChartData(chartData) {
    // (Giữ nguyên, không thay đổi)
    if (spendTrendChartInstance) {
        spendTrendChartInstance.data.labels = chartData.labels;
        spendTrendChartInstance.data.datasets = chartData.datasets;
        spendTrendChartInstance.update();
    }
}


// --- CÁC HÀM TRỢ GIÚP (HELPERS) ---

function getFilterPayload() {
    const filters = {};
    
    // Lấy giá trị từ Select2
    filters.account_id = s2Account.val();
    if (!filters.account_id) {
        showNotification('Vui lòng chọn một Tài khoản Quảng cáo.', 'error');
        return null;
    }

    const dateParams = getDateFilterParams();
    if (dateParams === null) {
        return null;
    }
    filters.date_preset = dateParams.date_preset;
    filters.start_date = dateParams.start_date;
    filters.end_date = dateParams.end_date;

    // Lấy giá trị từ multi-select DOM
    const campaignIds = Array.from(elCampaigns.selectedOptions).map(o => o.value);
    if (campaignIds.length > 0) {
        filters.campaign_ids = campaignIds;
    }
    
    const adsetIds = Array.from(elAdsets.selectedOptions).map(o => o.value);
    if (adsetIds.length > 0) {
        filters.adset_ids = adsetIds;
    }
    
    const adIds = Array.from(elAds.selectedOptions).map(o => o.value);
    if (adIds.length > 0) {
        filters.ad_ids = adIds;
    }

    return filters;
}

function getDateFilterParams(forRefresh = false) {
    // (Giữ nguyên, không thay đổi)
    const timeFilter = document.getElementById('filter-time');
    let date_preset = timeFilter.value;
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
            return null;
        }
        date_preset = null;
    }
    if (!end_date) {
        end_date = today;
    }
    if (date_preset) {
        return { date_preset: date_preset, end_date: end_date };
    } else {
        return { date_preset: null, start_date: start_date, end_date: end_date };
    }
}

/**
 * Reset một dropdown (xử lý cả Select2 và DOM Element)
 * @param {object} instance - jQuery object (Select2) hoặc DOM Element
 */
function resetDropdown(instance) {
    if (!instance) return;

    if (instance.jquery) { //
        // Đây là Select2 instance
        instance.empty();
        instance.prop('disabled', true).trigger('change');
    } else {
        // Đây là DOM element
        instance.innerHTML = ''; //
        instance.disabled = true; //
        // Gọi loadOptions để cập nhật UI của thư viện mới
        if (instance.loadOptions) {
            instance.loadOptions();
        }
    }
}

/**
 * Set trạng thái "Loading..." (xử lý cả Select2 và DOM Element)
 * @param {object} instance - jQuery object (Select2) hoặc DOM Element
 * @param {string} loadingText - Text hiển thị
 */
function setDropdownLoading(instance, loadingText) {
    if (!instance) return;

    if (instance.jquery) { //
        // Đây là Select2 instance
        instance.empty();
        instance.next('.select2-container')
                     .find('.select2-selection__placeholder')
                     .text(loadingText);
        instance.prop('disabled', true).trigger('change');
    } else {
        // Đây là DOM element
        instance.innerHTML = '';
        instance.disabled = true;
        
        // Thư viện mới đọc placeholder từ thuộc tính
        instance.setAttribute('placeholder', loadingText);

        // Gọi loadOptions để cập nhật UI
        if (instance.loadOptions) {
            instance.loadOptions();
        }
    }
}

function setButtonLoading(button, loadingText) {
    // (Giữ nguyên, không thay đổi)
    button.disabled = true;
    const span = button.querySelector('span');
    if (span) span.innerText = loadingText;
    button.classList.add('button-loading');
}

function setButtonIdle(button, originalText) {
    // (Giữ nguyên, không thay đổi)
    button.disabled = false;
    const span = button.querySelector('span');
    if (span) span.innerText = originalText;
    button.classList.remove('button-loading');
}

function showNotification(message, type = 'info') {
    // (Giữ nguyên, không thay đổi)
    if (type === 'error') {
        console.error(message);
        alert(`LỖI: ${message}`);
    } else {
        console.log(message);
        alert(message);
    }
}

function triggerCampaignLoad() {
    const accountId = s2Account.val();
    const dateParams = getDateFilterParams();
    console.log("triggerCampaignLoad được gọi. Giá trị dateParams:", dateParams);
    
    // Reset các multi-select
    resetDropdown(elCampaigns);
    resetDropdown(elAdsets);
    resetDropdown(elAds);

    if (accountId && dateParams) {
        loadCampaignDropdown(accountId, dateParams);
    }
}

// [ĐÃ XÓA] Hàm updateSelect2Counter và setupSelectAll
// vì thư viện multiselect-dropdown.js đã tự xử lý.