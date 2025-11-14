/**
 * main.js
 * File JavaScript chính cho Meta Ads Dashboard
 * CẤU TRÚC KẾT HỢP:
 * - Sử dụng <select> đơn tiêu chuẩn (DOM) cho Account, Time.
 * - Sử dụng MultiselectDropdown.js cho multi-select (Campaigns, Adsets, Ads).
 */

// --- BIẾN TOÀN CỤC ---
let spendTrendChartInstance = null;
let platformChartInstance = null;

let elAccount, elTime;
let elCampaigns, elAdsets, elAds;

// [MỚI] Thêm biến cho 2 dropdown của biểu đồ tròn
let elChartMetric, elChartDimension;

// --- HÀM KHỞI CHẠY KHI TRANG ĐƯỢC TẢI ---
document.addEventListener('DOMContentLoaded', () => {
    
    MultiselectDropdown(window.MultiselectDropdownOptions); 

    feather.replace();
    initializeCharts();
    initializeSelects(); 
    setupEventListeners();
    loadAccountDropdown();
});

// --- KHỞI TẠO GIAO DIỆN ---

function initializeCharts() {
    // (Giữ nguyên)
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

function initializeSelects() {
    // 1. Dropdown Tài khoản
    elAccount = document.getElementById('filter-ad-account');

    // 2. Dropdown Thời gian
    elTime = document.getElementById('filter-time');

    // 3. Multi-selects
    elCampaigns = document.getElementById('filter-campaigns');
    elAdsets = document.getElementById('filter-adsets');
    elAds = document.getElementById('filter-ads');

    // [MỚI] Lấy DOM elements cho 2 dropdown của biểu đồ tròn
    elChartMetric = document.getElementById('chart-metric');
    elChartDimension = document.getElementById('chart-dimension');

    // [MỚI] Set giá trị mặc định (bạn có thể đổi ở đây)
    elChartMetric.value = 'purchases';
    elChartDimension.value = 'placement';

    // Vô hiệu hóa tất cả dropdown con ban đầu
    elAccount.disabled = true;
    elTime.disabled = true;
    elCampaigns.disabled = true;
    elAdsets.disabled = true;
    elAds.disabled = true;
    
    // [MỚI] Vô hiệu hóa cả dropdown của biểu đồ
    elChartMetric.disabled = true;
    elChartDimension.disabled = true;
    
    elCampaigns.loadOptions();
    elAdsets.loadOptions();
    elAds.loadOptions();
}

// --- GẮN CÁC BỘ LẮNG NGHE SỰ KIỆN ---

function setupEventListeners() {
    // Nút
    document.getElementById('btn-refresh-data').addEventListener('click', handleRefreshData);
    document.getElementById('btn-apply-filters').addEventListener('click', handleApplyFilters);

    // Filter thời gian
    elTime.addEventListener('change', (e) => {
        const value = e.currentTarget.value;
        const customDateRange = document.getElementById('custom-date-range');
        
        if (value === 'custom') {
            customDateRange.classList.remove('hidden');
            resetDropdown(elCampaigns);
            resetDropdown(elAdsets);
            resetDropdown(elAds);
        } else {
            customDateRange.classList.add('hidden');
            triggerCampaignLoad();
        }
    });

    // Filter ngày tùy chỉnh
    document.getElementById('date-from').addEventListener('change', handleCustomDateChange);
    document.getElementById('date-to').addEventListener('change', handleCustomDateChange);

    // Filter tài khoản
    elAccount.addEventListener('change', () => {
        triggerCampaignLoad();
    });

    // Filter Campaigns
    elCampaigns.addEventListener('change', () => { 
        const selectedCampaigns = Array.from(elCampaigns.selectedOptions).map(o => o.value); 
        resetDropdown(elAdsets);
        resetDropdown(elAds);
        if (selectedCampaigns && selectedCampaigns.length > 0) {
            loadAdsetDropdown(selectedCampaigns);
            elAdsets.disabled = false;
            elAdsets.loadOptions(); 
        }
    });

    // Filter Adsets
    elAdsets.addEventListener('change', () => { 
        const selectedAdsets = Array.from(elAdsets.selectedOptions).map(o => o.value); 
        resetDropdown(elAds);
        if (selectedAdsets && selectedAdsets.length > 0) {
            loadAdDropdown(selectedAdsets);
            elAds.disabled = false;
            elAds.loadOptions(); 
        }
    });

    elAds.addEventListener('change', () => { 
        // Không cần làm gì
    });

    // [MỚI] Thêm sự kiện cho 2 dropdown của biểu đồ tròn
    // Khi thay đổi, gọi hàm handlePieChartUpdate
    elChartMetric.addEventListener('change', handlePieChartUpdate);
    elChartDimension.addEventListener('change', handlePieChartUpdate);
}

// --- CÁC HÀM XỬ LÝ SỰ KIỆN ---

async function handleRefreshData() {
    // (Giữ nguyên)
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

    // [THAY ĐỔI] Lấy payload của biểu đồ tròn một cách linh động
    const pieChartFilters = getPieChartPayload();
    
    // getPieChartPayload đã bao gồm cả getFilterPayload
    // nên nếu nó null, nghĩa là filter cơ bản đã lỗi.
    if (!pieChartFilters) {
        setButtonIdle(button, originalText);
        return;
    }
    
    // Gán lại filters từ pieChartFilters (vì getPieChartPayload đã gộp)
    const filters = pieChartFilters;

    try {
        // [THAY ĐỔI] Chúng ta gọi cả 3 API cùng lúc
        const [overviewRes, chartRes, pieChartRes] = await Promise.all([
            // 1. Overview
            fetch('/api/overview_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) // Dùng filters chung
            }),
            // 2. Line Chart
            fetch('/api/chart_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) // Dùng filters chung
            }),
            // 3. Pie Chart
            fetch('/api/breakdown_chart', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(pieChartFilters) // Dùng payload riêng của pie
            })
        ]);

        // Xử lý lỗi
        if (!overviewRes.ok) {
            const err = await overviewRes.json();
            throw new Error(`Lỗi overview: ${err.error}`);
        }
        if (!chartRes.ok) {
            const err = await chartRes.json();
            throw new Error(`Lỗi chart: ${err.error}`);
        }
        if (!pieChartRes.ok) { // [MỚI]
            const err = await pieChartRes.json();
            throw new Error(`Lỗi pie chart: ${err.error}`);
        }

        // Lấy JSON
        const overviewData = await overviewRes.json();
        const chartData = await chartRes.json();
        const pieChartData = await pieChartRes.json(); // [MỚI]

        // Render dữ liệu
        renderOverviewData(overviewData.scorecards);
        renderChartData(chartData);
        
        // [THAY ĐỔI] Render pie chart với tiêu đề động
        const metricText = elChartMetric.options[elChartMetric.selectedIndex].text;
        const dimText = elChartDimension.options[elChartDimension.selectedIndex].text;
        renderPieChartData(pieChartData, `${metricText} theo ${dimText}`);

        
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

/**
 * [MỚI] Hàm này CHỈ cập nhật biểu đồ tròn
 * (Được gọi khi thay đổi metric hoặc dimension)
 */
async function handlePieChartUpdate() {
    console.log("Đang cập nhật biểu đồ tròn...");
    
    // 1. Lấy payload (bao gồm filter chung + filter riêng của pie)
    const payload = getPieChartPayload();
    if (!payload) {
        return; // Lỗi đã được hiển thị bởi getPieChartPayload
    }

    // TODO: Thêm hiệu ứng loading cho biểu đồ tròn (tùy chọn)
    // platformChartInstance.options.plugins.title.text = 'Đang tải...';
    // platformChartInstance.update();
            
    try {
        // 2. Chỉ gọi API của biểu đồ breakdown
        const response = await fetch('/api/breakdown_chart', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        
        if (!response.ok) {
            const err = await response.json();
            throw new Error(`Lỗi pie chart: ${err.error}`);
        }
        
        const pieChartData = await response.json();
        
        // 3. Lấy text từ dropdown để tạo tiêu đề động
        const metricText = elChartMetric.options[elChartMetric.selectedIndex].text;
        const dimText = elChartDimension.options[elChartDimension.selectedIndex].text;
        const title = `${metricText} theo ${dimText}`;
        
        // 4. Render lại biểu đồ
        renderPieChartData(pieChartData, title);
        
    } catch (error) {
        console.error('Lỗi khi cập nhật pie chart:', error);
        showNotification(error.message, 'error');
        renderPieChartData(null, 'Lỗi tải dữ liệu');
    }
}


// --- CÁC HÀM TẢI DROPDOWN ---

async function loadAccountDropdown() {
    setDropdownLoading(elAccount, 'Đang tải tài khoản...');
    
    try {
        const response = await fetch('/api/accounts');
        if (!response.ok) throw new Error('Lỗi mạng khi tải tài khoản');
        const accounts = await response.json();
        
        elAccount.innerHTML = '';
        
        accounts.forEach(c => {
            const idString = String(c.id);
            const lastFourDigits = idString.slice(-4);
            const newText = `${c.name} (${lastFourDigits})`;
            const option = new Option(newText, c.id);
            elAccount.appendChild(option);
        });

        elAccount.disabled = false;
        elTime.disabled = false;
        
        // [MỚI] Mở khóa 2 dropdown của biểu đồ tròn
        elChartMetric.disabled = false;
        elChartDimension.disabled = false;

        if (accounts.length > 0) {
            elAccount.value = accounts[0].id;
            elAccount.dispatchEvent(new Event('change'));
        }
    } catch (error) {
        console.error('Lỗi tải tài khoản:', error);
        setDropdownLoading(elAccount, 'Lỗi tải tài khoản');
    }
}

async function loadCampaignDropdown(accountId, dateParams) {
    // (Giữ nguyên)
    setDropdownLoading(elCampaigns, 'Đang tải chiến dịch...');
    
    try {
        const response = await fetch('/api/campaigns', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ account_id: accountId, ...dateParams })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const campaigns = await response.json();

        elCampaigns.innerHTML = '';
        campaigns.forEach(c => {
            const option = new Option(c.name, c.campaign_id);
            elCampaigns.appendChild(option);
        });
        
        elCampaigns.disabled = false;
        elCampaigns.loadOptions();

    } catch (error) {
        console.error('Lỗi tải chiến dịch:', error);
        setDropdownLoading(elCampaigns, 'Lỗi tải chiến dịch...');
    }
}


async function loadAdsetDropdown(campaignIds) {
    // (Giữ nguyên)
    setDropdownLoading(elAdsets, 'Đang tải nhóm QC...');
    
    try {
        const response = await fetch('/api/adsets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ campaign_ids: campaignIds, ...getDateFilterParams() })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const adsets = await response.json();
        
        elAdsets.innerHTML = '';
        adsets.forEach(a => {
            const option = new Option(a.name, a.adset_id);
            elAdsets.appendChild(option);
        });
        
        elAdsets.disabled = false;
        elAdsets.loadOptions();
        
    } catch (error) {
        console.error('Lỗi tải nhóm QC:', error);
        setDropdownLoading(elAdsets, 'Lỗi tải nhóm QC...');
    }
}

async function loadAdDropdown(adsetIds) {
    // (Giữ nguyên)
    setDropdownLoading(elAds, 'Đang tải quảng cáo...');
    
    try {
        const response = await fetch('/api/ads', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ adset_ids: adsetIds, ...getDateFilterParams() })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const ads = await response.json();
        
        elAds.innerHTML = '';
        ads.forEach(ad => {
            const option = new Option(ad.name, ad.ad_id);
            elAds.appendChild(option);
        });

        elAds.disabled = false;
        elAds.loadOptions();
        
    } catch (error) {
        console.error('Lỗi tải quảng cáo:', error);
        setDropdownLoading(elAds, 'Lỗi tải quảng cáo...');
    }
}


// --- CÁC HÀM RENDER DỮ LIỆU ---

function renderOverviewData(data) {
    // (Giữ nguyên)
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
    // (Giữ nguyên)
    if (spendTrendChartInstance) {
        spendTrendChartInstance.data.labels = chartData.labels;
        spendTrendChartInstance.data.datasets = chartData.datasets;
        spendTrendChartInstance.update();
    }
}

/**
 * [THAY ĐỔI] Hàm render biểu đồ tròn (pie chart)
 * (Được cập nhật từ phiên bản trước)
 * @param {object} pieChartData - Dữ liệu từ API (/api/breakdown_chart)
 * @param {string} title - Tiêu đề động cho biểu đồ
 */
function renderPieChartData(pieChartData, title = 'Breakdown') {
    if (platformChartInstance) {
        // Kiểm tra xem có dữ liệu hay không
        if (pieChartData && pieChartData.labels && pieChartData.labels.length > 0) {
            platformChartInstance.data.labels = pieChartData.labels;
            platformChartInstance.data.datasets = pieChartData.datasets;
            platformChartInstance.options.plugins.title.text = title; // Cập nhật tiêu đề
        } else {
            // Nếu không có dữ liệu (hoặc là null do lỗi)
            platformChartInstance.data.labels = ['Không có dữ liệu'];
            platformChartInstance.data.datasets = [{ 
                label: 'Phân bổ', 
                data: [1], 
                backgroundColor: ['#E5E7EB'], 
                hoverOffset: 4 
            }];
            // Sử dụng tiêu đề được truyền vào (ví dụ: 'Lỗi tải dữ liệu' hoặc 'Không tìm thấy dữ liệu')
            platformChartInstance.options.plugins.title.text = (title === 'Breakdown') ? 'Không tìm thấy dữ liệu' : title;
        }
        platformChartInstance.update();
    }
}


// --- CÁC HÀM TRỢ GIÚP (HELPERS) ---

function getFilterPayload() {
    // (Giữ nguyên)
    const filters = {};
    
    filters.account_id = elAccount.value;
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

/**
 * [MỚI] Hàm helper để lấy payload đầy đủ cho biểu đồ tròn
 */
function getPieChartPayload() {
    // 1. Lấy các filter cơ bản (ngày, tài khoản, campaign...)
    const baseFilters = getFilterPayload();
    if (!baseFilters) {
        return null; // Lỗi đã được hiển thị bởi getFilterPayload
    }
    
    // 2. Lấy các filter riêng của biểu đồ tròn (metric, dimension)
    const metric = elChartMetric.value;
    const dimension = elChartDimension.value;
    
    if (!metric || !dimension) {
        showNotification('Vui lòng chọn Chỉ số và Chiều cho biểu đồ tròn.', 'error');
        return null;
    }

    // 3. Gộp chúng lại và trả về
    return {
        ...baseFilters,
        metric: metric,
        dimension: dimension
    };
}

function getDateFilterParams(forRefresh = false) {
    // (Giữ nguyên)
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

function resetDropdown(instance) {
    // (Giữ nguyên)
    if (!instance) return;
    if (instance.loadOptions) {
        instance.innerHTML = '';
        instance.disabled = true;
        instance.loadOptions();
    } else {
        instance.innerHTML = '';
        instance.disabled = true;
    }
}

function setDropdownLoading(instance, loadingText) {
    // (Giữ nguyên)
    if (!instance) return;
    if (instance.loadOptions) {
        instance.innerHTML = '';
        instance.disabled = true;
        instance.setAttribute('placeholder', loadingText);
        instance.loadOptions();
    } else {
        instance.innerHTML = '';
        instance.appendChild(new Option(loadingText, ''));
        instance.disabled = true;
    }
}

function setButtonLoading(button, loadingText) {
    // (Giữ nguyên)
    button.disabled = true;
    const span = button.querySelector('span');
    if (span) span.innerText = loadingText;
    button.classList.add('button-loading');
}

function setButtonIdle(button, originalText) {
    // (GiGit nguyên)
    button.disabled = false;
    const span = button.querySelector('span');
    if (span) span.innerText = originalText;
    button.classList.remove('button-loading');
}

function showNotification(message, type = 'info') {
    // (Giữ nguyên)
    if (type === 'error') {
        console.error(message);
        alert(`LỖI: ${message}`);
    } else {
        console.log(message);
        alert(message);
    }
}

function handleCustomDateChange() {
    // (Giữ nguyên)
    const dateFrom = document.getElementById('date-from').value;
    const dateTo = document.getElementById('date-to').value;
    
    if (dateFrom && dateTo) {
        console.log("Ngày tùy chỉnh đã hợp lệ. Đang tải lại campaigns...");
        triggerCampaignLoad();
    }
}

function triggerCampaignLoad() {
    // (Giữ nguyên)
    const accountId = elAccount.value;
    const dateParams = getDateFilterParams();
    console.log("triggerCampaignLoad được gọi. Giá trị dateParams:", dateParams);
    
    resetDropdown(elCampaigns);
    resetDropdown(elAdsets);
    resetDropdown(elAds);

    if (accountId && dateParams) {
        loadCampaignDropdown(accountId, dateParams);
    }
}