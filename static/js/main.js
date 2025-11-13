/**
 * main.js
 * File JavaScript chính cho Meta Ads Dashboard
 * Đã chuyển đổi hoàn toàn sang jQuery + Select2.
 */

// --- BIẾN TOÀN CỤC (đã đổi tên s2 = Select2) ---
let spendTrendChartInstance = null;
let platformChartInstance = null;
let s2Account, s2Time, s2Campaigns, s2Adsets, s2Ads;

// --- HÀM KHỞI CHẠY KHI TRANG ĐƯỢC TẢI ---
document.addEventListener('DOMContentLoaded', () => {
    // Phải đợi DOM load xong mới chạy code jQuery
    // nên toàn bộ code sẽ nằm trong này
    
    feather.replace();
    initializeCharts();
    initializeSelect2();
    setupEventListeners();
    loadAccountDropdown(); // Tải tài khoản ngay khi bắt đầu
});

// --- KHỞI TẠO GIAO DIỆN ---

function initializeCharts() {
    const ctxSpend = document.getElementById('spendTrendChart').getContext('2d');
    spendTrendChartInstance = new Chart(ctxSpend, {
        type: 'line',
        data: { labels: [], datasets: [] },
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            elements: {
                line: {
                    fill: true // Bật tính năng "fill" (lấp đầy)
                }
            },
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
 * Khởi tạo Select2 cho các dropdown
 */
function initializeSelect2() {
    // Cài đặt chung
    const singleSelectSettings = (placeholder) => ({
        placeholder: placeholder,
    });

    const multiSelectSettings = (placeholder) => ({
        placeholder: placeholder,
        closeOnSelect: false, // Giữ dropdown mở khi chọn
        maximumSelectionLength: 1 //
    });

    // 1. Dropdown Tài khoản (chọn một)
    s2Account = $('#filter-ad-account').select2(singleSelectSettings('Chọn tài khoản...'));

    // 2. Dropdown Thời gian (chọn một)
    s2Time = $('#filter-time').select2({
        minimumResultsForSearch: Infinity // Ẩn ô tìm kiếm
    });

    // 3. Dropdown Chiến dịch (chọn nhiều)
    const campaignPlaceholder = 'Chọn chiến dịch...';
    s2Campaigns = $('#filter-campaigns').select2(multiSelectSettings(campaignPlaceholder));
    setupSelectAll(s2Campaigns, 'campaigns-all'); // Thêm logic "Chọn tất cả"

    // Các dropdown con khác
    const adsetPlaceholder = 'Chọn nhóm quảng cáo...';
    s2Adsets = $('#filter-adsets').select2(multiSelectSettings(adsetPlaceholder));
    setupSelectAll(s2Adsets, 'adsets-all');

    const adPlaceholder = 'Chọn quảng cáo...';
    s2Ads = $('#filter-ads').select2(multiSelectSettings(adPlaceholder));
    setupSelectAll(s2Ads, 'ads-all');

    // Vô hiệu hóa các dropdown con ban đầu
    s2Campaigns.prop('disabled', true).trigger('change');
    s2Adsets.prop('disabled', true).trigger('change');
    s2Ads.prop('disabled', true).trigger('change');
}

// --- GẮN CÁC BỘ LẮNG NGHE SỰ KIỆN ---

function setupEventListeners() {
    $('#btn-refresh-data').on('click', handleRefreshData);
    $('#btn-apply-filters').on('click', handleApplyFilters);

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

    // Logic dropdown phụ thuộc (cascading)
    s2Campaigns.on('change', () => {
        const selectedCampaigns = s2Campaigns.val() ? s2Campaigns.val().filter(id => id !== 'all') : [];
        
        resetDropdown(s2Adsets, 'Chọn nhóm quảng cáo...');
        resetDropdown(s2Ads, 'Chọn quảng cáo...');
        
        if (selectedCampaigns && selectedCampaigns.length > 0) {
            loadAdsetDropdown(selectedCampaigns);
            s2Adsets.prop('disabled', false).trigger('change');
        }
    });

    s2Adsets.on('change', () => {
        const selectedAdsets = s2Adsets.val() ? s2Adsets.val().filter(id => id !== 'all') : [];
        
        resetDropdown(s2Ads, 'Chọn quảng cáo...');
        
        if (selectedAdsets && selectedAdsets.length > 0) {
            loadAdDropdown(selectedAdsets);
            s2Ads.prop('disabled', false).trigger('change');
        }
    });
}

// --- CÁC HÀM XỬ LÝ SỰ KIỆN ---

async function handleRefreshData() {
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

// --- CÁC HÀM TẢI DROPDOWN (VIẾT LẠI CHO SELECT2) ---

async function loadAccountDropdown() {
    s2Account.empty().append(new Option('Đang tải tài khoản...', '')).prop('disabled', true).trigger('change');
    
    try {
        const response = await fetch('/api/accounts');
        if (!response.ok) throw new Error('Lỗi mạng khi tải tài khoản');
        const accounts = await response.json();
        
        s2Account.empty(); // Xóa "Đang tải"

        // [QUAN TRỌNG] Thêm option rỗng cho placeholder
        s2Account.append(new Option('', '', false, false));
        
        // Thêm option vào
        accounts.forEach(c => {
            // Lấy 4 số cuối của ID
            const idString = String(c.id);
            const lastFourDigits = idString.slice(-4);
            
            // Tạo text mới, ví dụ: "BBI2025 (1234)"
            const newText = `${c.name} (${lastFourDigits})`;
            
            // Tạo option với text mới, nhưng value vẫn là ID đầy đủ
            const option = new Option(newText, c.id);
            s2Account.append(option);
        });

        s2Account.prop('disabled', false).trigger('change');

        // Tự động chọn tài khoản đầu tiên
        if (accounts.length > 0) {
            s2Account.val(accounts[0].id).trigger('change'); // .trigger('change') sẽ tự động gọi triggerCampaignLoad
        }
    } catch (error) {
        console.error('Lỗi tải tài khoản:', error);
        s2Account.empty().append(new Option('Lỗi tải tài khoản', '')).prop('disabled', true).trigger('change');
    }
}

async function loadCampaignDropdown(accountId, dateParams) {
    const placeholder = 'Chọn chiến dịch...';
    resetDropdown(s2Campaigns, 'Đang tải chiến dịch...');
    
    try {
        const response = await fetch('/api/campaigns', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ account_id: accountId, ...dateParams })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const campaigns = await response.json();

        s2Campaigns.empty(); // Xóa "Đang tải"

        // THÊM: Thêm tùy chọn "Tất cả" LÊN ĐẦU
        if (campaigns.length > 0) {
            const allOption = new Option('TẤT CẢ (Chọn / Bỏ chọn)', 'all');
            allOption.id = 'campaigns-all'; // ID cho CSS
            s2Campaigns.append(allOption);
        }
        
        // Thêm các option còn lại
        campaigns.forEach(c => {
            const option = new Option(c.name, c.campaign_id);
            s2Campaigns.append(option);
        });
        
        s2Campaigns.prop('disabled', false).trigger('change');
        // Đặt lại placeholder sau khi tải xong
        s2Campaigns.select2({ placeholder: placeholder, closeOnSelect: false, allowClear: true });

    } catch (error) {
        console.error('Lỗi tải chiến dịch:', error);
        resetDropdown(s2Campaigns, 'Lỗi tải chiến dịch...');
    }
}


async function loadAdsetDropdown(campaignIds) {
    const placeholder = 'Chọn nhóm quảng cáo...';
    resetDropdown(s2Adsets, 'Đang tải nhóm QC...');
    
    try {
        const response = await fetch('/api/adsets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ campaign_ids: campaignIds, ...getDateFilterParams() })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const adsets = await response.json();
        
        s2Adsets.empty(); // Xóa "Đang tải"

        // THÊM: Thêm tùy chọn "Tất cả" LÊN ĐẦU
        if (adsets.length > 0) {
            const allOption = new Option('TẤT CẢ (Chọn / Bỏ chọn)', 'all');
            allOption.id = 'adsets-all'; // ID cho CSS
            s2Adsets.append(allOption);
        }

        adsets.forEach(a => {
            const option = new Option(a.name, a.adset_id);
            s2Adsets.append(option);
        });
        
        s2Adsets.prop('disabled', false).trigger('change');
        s2Adsets.select2({ placeholder: placeholder, closeOnSelect: false, allowClear: true });
        
    } catch (error) {
        console.error('Lỗi tải nhóm QC:', error);
        resetDropdown(s2Adsets, 'Lỗi tải nhóm QC...');
    }
}

async function loadAdDropdown(adsetIds) {
    const placeholder = 'Chọn quảng cáo...';
    resetDropdown(s2Ads, 'Đang tải quảng cáo...');
    
    try {
        const response = await fetch('/api/ads', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ adset_ids: adsetIds, ...getDateFilterParams() })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const ads = await response.json();
        
        s2Ads.empty(); // Xóa "Đang tải"

        // THÊM: Thêm tùy chọn "Tất cả" LÊN ĐẦU
        if (ads.length > 0) {
            const allOption = new Option('TẤT CẢ (Chọn / Bỏ chọn)', 'all');
            allOption.id = 'ads-all'; // ID cho CSS
            s2Ads.append(allOption);
        }
        
        ads.forEach(ad => {
            const option = new Option(ad.name, ad.ad_id);
            s2Ads.append(option);
        });

        s2Ads.prop('disabled', false).trigger('change');
        s2Ads.select2({ placeholder: placeholder, closeOnSelect: false, allowClear: true });
        
    } catch (error) {
        console.error('Lỗi tải quảng cáo:', error);
        resetDropdown(s2Ads, 'Lỗi tải quảng cáo...');
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
    if (spendTrendChartInstance) {
        spendTrendChartInstance.data.labels = chartData.labels;
        spendTrendChartInstance.data.datasets = chartData.datasets;
        spendTrendChartInstance.update();
    }
}


// --- CÁC HÀM TRỢ GIÚP (HELPERS) ---

function getFilterPayload() {
    const filters = {};
    
    filters.account_id = s2Account.val(); // Dùng .val()
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

    // Lọc ra 'all' trước khi gửi API
    const campaignIds = s2Campaigns.val() ? s2Campaigns.val().filter(id => id !== 'all') : [];
    if (campaignIds.length > 0) {
        filters.campaign_ids = campaignIds;
    }
    
    const adsetIds = s2Adsets.val() ? s2Adsets.val().filter(id => id !== 'all') : [];
    if (adsetIds.length > 0) {
        filters.adset_ids = adsetIds;
    }
    
    const adIds = s2Ads.val() ? s2Ads.val().filter(id => id !== 'all') : [];
    if (adIds.length > 0) {
        filters.ad_ids = adIds;
    }

    return filters;
}

function getDateFilterParams(forRefresh = false) {
    // ... (Giữ nguyên, không thay đổi)
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

function resetDropdown(select2Instance, placeholder) {
    if (select2Instance) {
        select2Instance.empty(); // Xóa tất cả <option>
        
        // Thêm một option "placeholder" tạm thời khi đang tải/reset
        const tempOption = new Option(placeholder, 'loading', false, false);
        tempOption.disabled = true;
        select2Instance.append(tempOption);
        
        select2Instance.prop('disabled', true).trigger('change');
        
        // Cập nhật placeholder cho Select2
        select2Instance.select2({ 
            placeholder: placeholder, 
            closeOnSelect: false, 
            allowClear: true 
        });
    }
}

function setButtonLoading(button, loadingText) {
    // ... (Giữ nguyên, không thay đổi)
    button.disabled = true;
    const span = button.querySelector('span');
    if (span) span.innerText = loadingText;
    button.classList.add('button-loading');
}

function setButtonIdle(button, originalText) {
    // ... (Giữ nguyên, không thay đổi)
    button.disabled = false;
    const span = button.querySelector('span');
    if (span) span.innerText = originalText;
    button.classList.remove('button-loading');
}

function showNotification(message, type = 'info') {
    // ... (Giữ nguyên, không thay đổi)
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
    
    resetDropdown(s2Campaigns, 'Chọn chiến dịch...');
    resetDropdown(s2Adsets, 'Chọn nhóm quảng cáo...');
    resetDropdown(s2Ads, 'Chọn quảng cáo...');

    if (accountId && dateParams) {
        loadCampaignDropdown(accountId, dateParams);
    }
}

/**
 * Thêm logic "Chọn tất cả" / "Bỏ chọn tất cả" cho một instance Select2.
 * @param {jQuery} $select2Instance - Đối tượng jQuery của thẻ <select>
 * @param {string} allId - ID duy nhất cho option "all" (e.g., 'campaigns-all')
 */
function setupSelectAll($select2Instance, allId) {
    const ALL_ID = 'all';

    // Dùng event 'select2:select' để bắt sự kiện ngay lập tức
    $select2Instance.on('select2:select', function (e) {
        if (e.params.data.id === ALL_ID) {
            // Case 1: Người dùng vừa BẤM CHỌN "Tất cả"
            // Lấy tất cả ID của <option> NGOẠI TRỪ 'all'
            const allOptionValues = $select2Instance.find('option')
                .map((i, el) => $(el).val())
                .get();
                
            // Set giá trị bao gồm tất cả
            $select2Instance.val(allOptionValues).trigger('change');
        }
    });

    // Dùng event 'select2:unselect'
    $select2Instance.on('select2:unselect', function (e) {
        const currentValues = $select2Instance.val() || [];
        
        if (e.params.data.id === ALL_ID) {
            // Case 2: Người dùng vừa BỎ CHỌN "Tất cả" (qua badge 'x')
            $select2Instance.val(null).trigger('change');
            
        } else if (currentValues.includes(ALL_ID)) {
            // Case 3: Đã chọn "Tất cả", nhưng người dùng bỏ 1 item khác
            // -> Bỏ chọn "Tất cả" và giữ các item còn lại
            const newValuesWithoutAll = currentValues.filter(id => id !== ALL_ID);
            $select2Instance.val(newValuesWithoutAll).trigger('change');
        }
    });

    // Bổ sung: Tự động check "Tất cả" nếu user chọn hết
    $select2Instance.on('change', function(e) {
        // Chỉ chạy logic này khi event không phải do trigger nội bộ
        if (e.originalEvent) {
             const currentValues = $select2Instance.val() || [];
             const allOptionIds = $select2Instance.find('option').map((i, el) => $(el).val()).get();
             const regularOptionIds = allOptionIds.filter(id => id !== ALL_ID);

             if (!currentValues.includes(ALL_ID) && 
                 currentValues.length === regularOptionIds.length && 
                 regularOptionIds.length > 0) {
                 
                // Case 4: Người dùng tự tay chọn HẾT tất cả
                // -> Tự động chọn thêm "Tất cả" cho họ
                $select2Instance.val(allOptionIds).trigger('change.select2'); // trigger nội bộ
             }
        }
    });
    
    // Thêm class CSS cho option "Tất cả"
    // (Vì Select2 tạo lại DOM)
    $select2Instance.on('select2:open', function() {
        // Thêm class CSS cho option 'all'
        // jQuery `find` không tìm thấy option 'all' vì nó nằm trong dropdown
        // Phải tìm trong `results`
        setTimeout(() => {
             $('.select2-results__option[id="' + allId + '"]').addClass('select2-results__option--all');
        }, 0);
    });
}