/**
 * main.js
 * File JavaScript chính cho Meta Ads Dashboard
 * CẤU TRÚC KẾT HỢP:
 * - Logic cho Panel "Tổng quan" (Ads)
 * - Logic cho Panel "Fanpage overview" (Organic)
 * - Logic cho "AI Chatbot"
 * - Logic "Chuyển Panel"
 */

// --- BIẾN TOÀN CỤC ---

// (Ads)
let spendTrendChartInstance = null;
let platformChartInstance = null;
let elAccount, elTime;
let elCampaigns, elAdsets, elAds;
let elChartMetric, elChartDimension;

// (Fanpage)
let fpMainChartInstance = null;
let elFpFilterPage, elFpFilterDate, elFpBtnApply, elFpBtnRefresh, elFpCustomDateRange;
let elFpDateFrom, elFpDateTo;


// --- HÀM KHỞI CHẠY KHI TRANG ĐƯỢC TẢI ---
document.addEventListener('DOMContentLoaded', () => {
    
    // --- PHẦN 1: LOGIC DASHBOARD (ADS) ---
    MultiselectDropdown(window.MultiselectDropdownOptions); 
    feather.replace(); // Chạy feather icons cho toàn bộ trang
    initializeCharts();
    initializeSelects(); 
    setupEventListeners();
    loadAccountDropdown();
    
    // --- PHẦN 2: LOGIC CHATBOT AI (GIỮ NGUYÊN) ---
    console.log("Khởi tạo logic cho Chatbot AI...");
    const chatBubble = document.getElementById("chat-bubble");
    const chatWindow = document.getElementById("chat-window");
    const minimizeBtn = document.getElementById("chat-minimize-btn");
    const chatBody = document.getElementById("chat-body");
    const input = document.getElementById("user-message-input");
    const sendBtn = document.getElementById("send-message-btn");

    if (!chatBubble || !chatWindow || !minimizeBtn || !chatBody || !input || !sendBtn) {
        console.warn("Không tìm thấy các thành phần UI của Chatbot. Chatbot sẽ không hoạt động.");
    } else {
        function openChatWindow() {
            chatBubble.classList.add("opacity-0", "translate-y-8", "hidden");
            chatWindow.classList.remove("hidden");
            setTimeout(() => {
                chatWindow.classList.remove("opacity-0", "translate-y-full");
            }, 10);
        }

        function closeChatWindow() {
            chatWindow.classList.add("opacity-0", "translate-y-full");
            setTimeout(() => {
                chatWindow.classList.add("hidden");
                chatBubble.classList.remove("hidden", "opacity-0", "translate-y-8");
            }, 300);
        }

        chatBubble.addEventListener("click", openChatWindow);
        minimizeBtn.addEventListener("click", closeChatWindow);

        function addMessage(text, sender) {
            const messageWrapper = document.createElement("div");
            const messageDiv = document.createElement("div");
            if (sender === 'user') {
                messageWrapper.classList.add("flex", "justify-end");
                messageDiv.classList.add("bg-blue-600", "text-white", "p-3", "rounded-lg", "max-w-xs", "shadow-sm");
                messageDiv.textContent = text;
            } else if (sender === 'bot') {
                messageWrapper.classList.add("flex", "justify-start");
                messageDiv.classList.add("bg-gray-100", "text-gray-800", "p-3", "rounded-lg", "max-w-xs", "shadow-sm");
                messageDiv.innerHTML = text.replace(/\n/g, '<br>');
            } else if (sender === 'loading') {
                messageWrapper.classList.add("flex", "justify-start", "loading-indicator-wrapper");
                messageDiv.classList.add("bg-gray-100", "text-gray-800", "p-3", "rounded-lg", "max-w-xs", "shadow-sm");
                messageDiv.innerHTML = `<div class="typing-indicator"><span></span><span></span><span></span></div>`;
            }
            messageWrapper.appendChild(messageDiv);
            chatBody.appendChild(messageWrapper);
            chatBody.scrollTop = chatBody.scrollHeight;
            return messageWrapper; 
        }

        function removeLoadingIndicator() {
            const loadingIndicator = chatBody.querySelector(".loading-indicator-wrapper");
            if (loadingIndicator) {
                chatBody.removeChild(loadingIndicator);
            }
        }

        async function sendMessageToServer() {
            const messageText = input.value.trim();
            if (messageText === "") return;
            addMessage(messageText, "user");
            input.value = ""; 
            addMessage(null, "loading");
            try {
                const response = await fetch("/api/chat", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ message: messageText }),
                });
                removeLoadingIndicator();
                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.error || `Lỗi server: ${response.statusText}`);
                }
                const data = await response.json();
                addMessage(data.response, "bot");
            } catch (error) {
                console.error("Lỗi khi chat:", error);
                removeLoadingIndicator();
                addMessage(`Xin lỗi, đã xảy ra lỗi: ${error.message}`, "bot");
            }
        }
        sendBtn.addEventListener("click", sendMessageToServer);
        input.addEventListener("keypress", function(e) {
            if (e.key === "Enter") {
                e.preventDefault(); 
                sendMessageToServer();
            }
        });
    } 

    // --- PHẦN 3: LOGIC CHUYỂN PANEL (GIỮ NGUYÊN) ---
    console.log("Khởi tạo logic chuyển panel...");
    const navLinks = {
        'nav-tong-quan': document.getElementById('nav-tong-quan'),
        'nav-fanpage-overview': document.getElementById('nav-fanpage-overview')
    };
    const panels = {
        'panel-tong-quan': document.getElementById('panel-tong-quan'),
        'panel-fanpage-overview': document.getElementById('panel-fanpage-overview')
    };
    const allNavLinks = document.querySelectorAll('.nav-link');

    const setActiveLink = (clickedLink) => {
        if (!clickedLink) return;
        allNavLinks.forEach(link => {
            link.classList.remove('bg-gray-200', 'text-gray-700', 'font-medium');
            link.classList.add('text-gray-600', 'hover:bg-gray-100');
        });
        clickedLink.classList.add('bg-gray-200', 'text-gray-700', 'font-medium');
        clickedLink.classList.remove('text-gray-600', 'hover:bg-gray-100');
    };

    const showPanel = (panelId) => {
        Object.values(panels).forEach(panel => {
            if (panel) panel.classList.add('hidden');
        });
        if (panels[panelId]) {
            panels[panelId].classList.remove('hidden');
        }
    };

    if (navLinks['nav-tong-quan']) {
        navLinks['nav-tong-quan'].addEventListener('click', (e) => {
            e.preventDefault();
            setActiveLink(navLinks['nav-tong-quan']);
            showPanel('panel-tong-quan');
        });
    }

    if (navLinks['nav-fanpage-overview']) {
        navLinks['nav-fanpage-overview'].addEventListener('click', (e) => {
            e.preventDefault();
            setActiveLink(navLinks['nav-fanpage-overview']);
            showPanel('panel-fanpage-overview');
        });
    }
    showPanel('panel-tong-quan');

    // --- PHẦN 4: LOGIC RIÊNG CỦA FANPAGE (MỚI) ---
    console.log("Khởi tạo logic cho Panel Fanpage...");
    loadFanpageDropdown(); // Tải danh sách Fanpage khi trang vừa load

}); // <-- **KẾT THÚC** document.addEventListener('DOMContentLoaded')


// --- CÁC HÀM TIỆN ÍCH TOÀN CỤC (HELPERS) ---
// [TÁI CẤU TRÚC] Di chuyển các hàm format ra toàn cục
const formatNumber = (num) => new Intl.NumberFormat('vi-VN').format(Math.round(num || 0));
const formatCurrency = (num) => new Intl.NumberFormat('vi-VN', { style: 'currency', currency: 'VND' }).format(Math.round(num || 0));
const formatPercent = (num) => `${parseFloat(num || 0).toFixed(2)}%`;

/**
 * [TÁI CẤU TRÚC] Hàm helper để tạo HTML cho chỉ số tăng trưởng/thay đổi
 */
const renderGrowthHtml = (value, type) => {
    if (value === null || typeof value === 'undefined' || value === 0) {
        return `<span class="flex items-center text-sm text-gray-500 mt-1">
                    <i data-feather="minus" class="w-4 h-4"></i>
                    <span class="font-semibold ml-1">(n/a)</span>
                </span>`;
    }
    const isPositive = value > 0;
    const colorClass = isPositive ? 'text-green-600' : 'text-red-600';
    const iconName = isPositive ? 'arrow-up' : 'arrow-down';
    let formattedValue;
    if (type === 'percent') {
        formattedValue = `${Math.abs(value * 100).toFixed(1)}%`;
    } else if (type === 'percent_points') {
        formattedValue = `${Math.abs(value).toFixed(1)} pp`;
    } else if (type === 'currency') {
        formattedValue = formatCurrency(Math.abs(value));
    } else { // 'number'
        formattedValue = formatNumber(Math.abs(value));
    }
    return `<span class="${colorClass} flex items-center text-sm mt-1">
                <i data-feather="${iconName}" class="w-4 h-4 mr-1"></i>
                <span class="font-semibold">${formattedValue}</span>
            </span>`;
};


// --- KHỞI TẠO GIAO DIỆN ---

function initializeCharts() {
    // (Ads)
    const ctxSpend = document.getElementById('spendTrendChart').getContext('2d');
    spendTrendChartInstance = new Chart(ctxSpend, {
        type: 'line',
        data: { labels: [], datasets: [] },
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            elements: { line: { tension: 0.4 } }, // Bỏ fill: true
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

    // Khởi tạo biểu đồ cho Fanpage Overview
    const ctxFpMain = document.getElementById('fp-main-chart');
    if (ctxFpMain) {
        fpMainChartInstance = new Chart(ctxFpMain.getContext('2d'), {
            type: 'bar', // <-- THAY ĐỔI: Type chính là 'bar'
            data: { labels: [], datasets: [] },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                elements: { line: { tension: 0.4 } }, // Giữ tension cho line chart
                plugins: {
                    legend: { display: false } 
                },
                scales: {
                    x: {
                        grid: { display: false },
                        stacked: true // <-- THÊM MỚI: Bật stacked cho trục X
                    },
                    y: {
                        beginAtZero: true
                    },
                    y1: { // Trục y1 sẽ được cấu hình trong hàm render
                        beginAtZero: true
                    }
                }
            }
        });
        console.log("Đã khởi tạo 'fpMainChartInstance' (dạng mixed bar/line).");
    }
}

function initializeSelects() {
    // (Ads)
    elAccount = document.getElementById('filter-ad-account');
    elTime = document.getElementById('filter-time');
    elCampaigns = document.getElementById('filter-campaigns');
    elAdsets = document.getElementById('filter-adsets');
    elAds = document.getElementById('filter-ads');
    elChartMetric = document.getElementById('chart-metric');
    elChartDimension = document.getElementById('chart-dimension');

    if (elChartMetric) elChartMetric.value = 'purchases';
    if (elChartDimension) elChartDimension.value = 'placement';

    if (elAccount) elAccount.disabled = true;
    if (elTime) elTime.disabled = true;
    if (elCampaigns) {
        elCampaigns.disabled = true;
        elCampaigns.loadOptions();
    }
    if (elAdsets) {
        elAdsets.disabled = true;
        elAdsets.loadOptions();
    }
    if (elAds) {
        elAds.disabled = true;
        elAds.loadOptions();
    }
    if (elChartMetric) elChartMetric.disabled = true;
    if (elChartDimension) elChartDimension.disabled = true;

    // [MỚI] Khởi tạo biến cho Fanpage
    elFpFilterPage = document.getElementById('fp-filter-fanpage');
    elFpFilterDate = document.getElementById('fp-filter-date');
    elFpBtnApply = document.getElementById('fp-btn-apply');
    elFpBtnRefresh = document.getElementById('fp-btn-refresh'); // <-- THÊM MỚI
    elFpCustomDateRange = document.getElementById('fp-custom-date-range'); // <-- THÊM MỚI
    elFpDateFrom = document.getElementById('fp-date-from'); // <-- THÊM MỚI
    elFpDateTo = document.getElementById('fp-date-to'); // <-- THÊM MỚI

    // Vô hiệu hóa bộ lọc fanpage ban đầu
    if (elFpFilterPage) elFpFilterPage.disabled = true;
    if (elFpFilterDate) elFpFilterDate.disabled = true;
    if (elFpBtnApply) elFpBtnApply.disabled = true;
    if (elFpBtnRefresh) elFpBtnRefresh.disabled = true; // <-- THÊM MỚI
}

// --- GẮN CÁC BỘ LẮNG NGHE SỰ KIỆN ---

function setupEventListeners() {
    // (Ads)
    document.getElementById('btn-refresh-data').addEventListener('click', handleRefreshData);
    document.getElementById('btn-apply-filters').addEventListener('click', handleApplyFilters);
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
    document.getElementById('date-from').addEventListener('change', handleCustomDateChange);
    document.getElementById('date-to').addEventListener('change', handleCustomDateChange);
    elAccount.addEventListener('change', () => {
        triggerCampaignLoad();
    });
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
    elAdsets.addEventListener('change', () => { 
        const selectedAdsets = Array.from(elAdsets.selectedOptions).map(o => o.value); 
        resetDropdown(elAds);
        if (selectedAdsets && selectedAdsets.length > 0) {
            loadAdDropdown(selectedAdsets);
            elAds.disabled = false;
            elAds.loadOptions(); 
        }
    });
    elAds.addEventListener('change', () => { });
    if (elChartMetric) elChartMetric.addEventListener('change', handlePieChartUpdate);
    if (elChartDimension) elChartDimension.addEventListener('change', handlePieChartUpdate);

    // [MỚI] Gán sự kiện cho Panel Fanpage
    if (elFpFilterDate) {
        elFpFilterDate.addEventListener('change', (e) => {
            if (e.currentTarget.value === 'custom') {
                elFpCustomDateRange.classList.remove('hidden');
            } else {
                elFpCustomDateRange.classList.add('hidden');
            }
        });
    }
    if (elFpBtnApply) {
        elFpBtnApply.addEventListener('click', handleFanpageApplyFilters);
    }
    if (elFpBtnRefresh) {
        elFpBtnRefresh.addEventListener('click', handleFanpageRefreshData); // <-- THÊM MỚI
    }
}


// --- CÁC HÀM XỬ LÝ SỰ KIỆN (ADS - GIỮ NGUYÊN) ---

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
    // (Giữ nguyên)
    const button = document.getElementById('btn-apply-filters');
    const originalText = button.querySelector('span').innerText;
    setButtonLoading(button, 'Đang tải...');

    const pieChartFilters = getPieChartPayload();
    if (!pieChartFilters) {
        setButtonIdle(button, originalText);
        return;
    }
    const filters = pieChartFilters;

    try {
        const [overviewRes, chartRes, pieChartRes, tableRes] = await Promise.all([
            fetch('/api/overview_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
            }),
            fetch('/api/chart_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
            }),
            fetch('/api/breakdown_chart', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(pieChartFilters) 
            }),
            fetch('/api/table_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
            })
        ]);

        if (!overviewRes.ok) { const err = await overviewRes.json(); throw new Error(`Lỗi overview: ${err.error}`); }
        if (!chartRes.ok) { const err = await chartRes.json(); throw new Error(`Lỗi chart: ${err.error}`); }
        if (!pieChartRes.ok) { const err = await pieChartRes.json(); throw new Error(`Lỗi pie chart: ${err.error}`); }
        if (!tableRes.ok) { const err = await tableRes.json(); throw new Error(`Lỗi table data: ${err.error}`); }

        const overviewData = await overviewRes.json();
        const chartData = await chartRes.json();
        const pieChartData = await pieChartRes.json(); 
        const tableData = await tableRes.json(); 

        renderOverviewData(overviewData.scorecards);
        renderChartData(chartData);
        
        const metricText = elChartMetric.options[elChartMetric.selectedIndex].text;
        const dimText = elChartDimension.options[elChartDimension.selectedIndex].text;
        renderPieChartData(pieChartData, `${metricText} theo ${dimText}`);
        renderTableData(tableData);

    } catch (error) {
        console.error('Lỗi khi áp dụng bộ lọc:', error);
        showNotification(`Lỗi khi lấy dữ liệu: ${error.message}`, 'error');
        renderTableData(null, error.message);
    } finally {
        setButtonIdle(button, originalText);
    }
}

async function handlePieChartUpdate() {
    // (Giữ nguyên)
    console.log("Đang cập nhật biểu đồ tròn...");
    const payload = getPieChartPayload();
    if (!payload) { return; }
    try {
        const response = await fetch('/api/breakdown_chart', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!response.ok) { const err = await response.json(); throw new Error(`Lỗi pie chart: ${err.error}`); }
        const pieChartData = await response.json();
        const metricText = elChartMetric.options[elChartMetric.selectedIndex].text;
        const dimText = elChartDimension.options[elChartDimension.selectedIndex].text;
        const title = `${metricText} theo ${dimText}`;
        renderPieChartData(pieChartData, title);
    } catch (error) {
        console.error('Lỗi khi cập nhật pie chart:', error);
        showNotification(error.message, 'error');
        renderPieChartData(null, 'Lỗi tải dữ liệu');
    }
}


// --- CÁC HÀM TẢI DROPDOWN (ADS - GIỮ NGUYÊN) ---

async function loadAccountDropdown() {
    // (Giữ nguyên)
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


// --- CÁC HÀM RENDER DỮ LIỆU (ADS - GIỮ NGUYÊN) ---

function renderOverviewData(data) {
    // (Giữ nguyên)
    // Lấy DOM elements
    const kpiSpend = document.getElementById('kpi-total-spend');
    const kpiImpressions = document.getElementById('kpi-total-impressions');
    const kpiCtr = document.getElementById('kpi-avg-ctr');
    const kpiPurchases = document.getElementById('kpi-total-purchases');
    
    // Render
    if (kpiSpend) kpiSpend.innerText = formatCurrency(data.total_spend);
    if (kpiImpressions) kpiImpressions.innerText = formatNumber(data.total_impressions);
    if (kpiCtr) kpiCtr.innerText = formatPercent(data.ctr); 
    if (kpiPurchases) kpiPurchases.innerText = formatNumber(data.total_purchases);

    // Cập nhật 4 Scorecard - GROWTH (Percent)
    document.getElementById('kpi-total-spend-growth').innerHTML = renderGrowthHtml(data.total_spend_growth, 'percent');
    document.getElementById('kpi-total-impressions-growth').innerHTML = renderGrowthHtml(data.total_impressions_growth, 'percent');
    document.getElementById('kpi-avg-ctr-growth').innerHTML = renderGrowthHtml(data.ctr_growth, 'percent');
    document.getElementById('kpi-total-purchases-growth').innerHTML = renderGrowthHtml(data.total_purchases_growth, 'percent');
    
    // Funnel
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
    
    // 1. Nhóm hiển thị
    const elDetailImp = document.getElementById('kpi-detail-impressions');
    if (elDetailImp) elDetailImp.innerText = formatNumber(data.total_impressions); 
    document.getElementById('kpi-detail-impressions-growth').innerHTML = renderGrowthHtml(data.total_impressions_absolute, 'number');

    const elDetailReach = document.getElementById('kpi-detail-reach');
    if (elDetailReach) elDetailReach.innerText = formatNumber(data.total_reach);
    document.getElementById('kpi-detail-reach-growth').innerHTML = renderGrowthHtml(data.total_reach_absolute, 'number');
    
    const elDetailCtr = document.getElementById('kpi-detail-ctr'); 
    if (elDetailCtr) elDetailCtr.innerText = formatPercent(data.ctr); 
    document.getElementById('kpi-detail-ctr-growth').innerHTML = renderGrowthHtml(data.ctr_absolute, 'percent_points'); 

    // 2. Nhóm chỉ số tương tác
    const elDetailEng = document.getElementById('kpi-detail-post-engagement');
    if (elDetailEng) elDetailEng.innerText = formatNumber(data.total_post_engagement);
    document.getElementById('kpi-detail-post-engagement-growth').innerHTML = renderGrowthHtml(data.total_post_engagement_absolute, 'number'); 

    const elDetailLinkClick = document.getElementById('kpi-detail-link-click');
    if (elDetailLinkClick) elDetailLinkClick.innerText = formatNumber(data.total_link_click);
    document.getElementById('kpi-detail-link-click-growth').innerHTML = renderGrowthHtml(data.total_link_click_absolute, 'number'); 

    // 3. Nhóm tỉ lệ chuyển đổi
    const elDetailMsg = document.getElementById('kpi-detail-messages');
    if (elDetailMsg) elDetailMsg.innerText = formatNumber(data.total_messages);
    document.getElementById('kpi-detail-messages-growth').innerHTML = renderGrowthHtml(data.total_messages_absolute, 'number'); 

    const elDetailPurch = document.getElementById('kpi-detail-purchases');
    if (elDetailPurch) elDetailPurch.innerText = formatNumber(data.total_purchases);
    document.getElementById('kpi-detail-purchases-growth').innerHTML = renderGrowthHtml(data.total_purchases_absolute, 'number');

    const elDetailPurchVal = document.getElementById('kpi-detail-purchase-value');
    if (elDetailPurchVal) elDetailPurchVal.innerText = formatCurrency(data.total_purchase_value);
    document.getElementById('kpi-detail-purchase-value-growth').innerHTML = renderGrowthHtml(data.total_purchase_value_absolute, 'currency'); 
    
    feather.replace();
}

function renderChartData(chartData) {
    // (Giữ nguyên)
    if (spendTrendChartInstance) {
        spendTrendChartInstance.data.labels = chartData.labels;
        spendTrendChartInstance.data.datasets = chartData.datasets;
        spendTrendChartInstance.update();
    }
}

function renderPieChartData(pieChartData, title = 'Breakdown') {
    // (Giữ nguyên)
    if (platformChartInstance) {
        if (pieChartData && pieChartData.labels && pieChartData.labels.length > 0) {
            platformChartInstance.data.labels = pieChartData.labels;
            platformChartInstance.data.datasets = pieChartData.datasets;
            platformChartInstance.options.plugins.title.text = title; 
        } else {
            platformChartInstance.data.labels = ['Không có dữ liệu'];
            platformChartInstance.data.datasets = [{ label: 'Phân bổ', data: [1], backgroundColor: ['#E5E7EB'], hoverOffset: 4 }];
            platformChartInstance.options.plugins.title.text = (title === 'Breakdown') ? 'Không tìm thấy dữ liệu' : title;
        }
        platformChartInstance.update();
    }
}


// --- CÁC HÀM TRỢ GIÚP (ADS - GIỮ NGUYÊN) ---

function getFilterPayload() {
    // (Giữ nguyên)
    const filters = {};
    filters.account_id = elAccount.value;
    if (!filters.account_id) {
        showNotification('Vui lòng chọn một Tài khoản Quảng cáo.', 'error');
        return null;
    }
    const dateParams = getDateFilterParams();
    if (dateParams === null) { return null; }
    filters.date_preset = dateParams.date_preset;
    filters.start_date = dateParams.start_date;
    filters.end_date = dateParams.end_date;
    const campaignIds = Array.from(elCampaigns.selectedOptions).map(o => o.value);
    if (campaignIds.length > 0) { filters.campaign_ids = campaignIds; }
    const adsetIds = Array.from(elAdsets.selectedOptions).map(o => o.value);
    if (adsetIds.length > 0) { filters.adset_ids = adsetIds; }
    const adIds = Array.from(elAds.selectedOptions).map(o => o.value);
    if (adIds.length > 0) { filters.ad_ids = adIds; }
    return filters;
}

function getPieChartPayload() {
    // (Giữ nguyên)
    const baseFilters = getFilterPayload();
    if (!baseFilters) { return null; }
    const metric = elChartMetric.value;
    const dimension = elChartDimension.value;
    if (!metric || !dimension) {
        showNotification('Vui lòng chọn Chỉ số và Chiều cho biểu đồ tròn.', 'error');
        return null;
    }
    return { ...baseFilters, metric: metric, dimension: dimension };
}

function getDateFilterParams(forRefresh = false) {
    // (Giữ nguyên)
    const timeFilter = document.getElementById('filter-time');
    let date_preset = timeFilter.value;
    let start_date = document.getElementById('date-from').value;
    let end_date = document.getElementById('date-to').value;
    const today = new Date().toISOString().split('T')[0];
    if (forRefresh && date_preset !== 'custom') { end_date = today; }
    if (date_preset !== 'custom') { start_date = null; } else {
        if (!start_date || !end_date) {
            showNotification('Tùy chỉnh: Vui lòng chọn Từ ngày và Đến ngày!', 'error');
            return null;
        }
        date_preset = null;
    }
    if (!end_date) { end_date = today; }
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
    button.classList.add('button-loading'); // Có thể bạn có CSS cho class này
}

function setButtonIdle(button, originalText) {
    // (Giữ nguyên)
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
        // Tạm thời dùng alert, bạn có thể thay bằng thư viện toast
        // alert(message); 
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

function getStatusBadge(status) {
    // (Giữ nguyên)
    status = status ? status.toUpperCase() : 'UNKNOWN';
    switch (status) {
        case 'ACTIVE': return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-green-100 text-green-800">Hoạt động</span>`;
        case 'PAUSED': return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-yellow-100 text-yellow-800">Tạm dừng</span>`;
        case 'ARCHIVED': return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-red-100 text-red-800">Đã lưu trữ</span>`;
        case 'DELETED': return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-red-100 text-red-800">Đã xóa</span>`;
        default: return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-gray-100 text-gray-800">${status}</span>`;
    }
}

function renderTableData(data, errorMsg = null) {
    // (GiVertical_Align_Start)
    const tableBody = document.getElementById('campaign-table-body');
    if (!tableBody) return;
    if (errorMsg) {
        tableBody.innerHTML = `<tr><td colspan="6" class="py-4 px-4 text-center text-red-500">${errorMsg}</td></tr>`;
        return;
    }
    if (!data || data.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="6" class="py-4 px-4 text-center text-gray-500">Không tìm thấy dữ liệu chiến dịch phù hợp.</td></tr>`;
        return;
    }
    let html = '';
    data.forEach(row => {
        html += `<tr class="border-b hover:bg-gray-50">`;
        html += `<td class="py-3 px-4 font-medium text-gray-900">${row.campaign_name}</td>`;
        html += `<td class="py-3 px-4">${getStatusBadge(row.status)}</td>`;
        html += `<td class="py-3 px-4 text-right">${formatCurrency(row.spend)}</td>`;
        html += `<td class="py-3 px-4 text-right">${formatNumber(row.impressions)}</td>`;
        html += `<td class="py-3 px-4 text-right">${formatNumber(row.purchases)}</td>`;
        html += `<td class="py-3 px-4 text-right">${formatCurrency(row.cpa)}</td>`;
        html += `</tr>`;
    });
    tableBody.innerHTML = html;
}


// ======================================================================
// --- [MỚI] CÁC HÀM LOGIC CHO FANPAGE OVERVIEW ---
// ======================================================================

/**
 * Tải danh sách Fanpage vào dropdown
 */
async function loadFanpageDropdown() {
    if (!elFpFilterPage) return; // Kiểm tra nếu element không tồn tại

    setDropdownLoading(elFpFilterPage, 'Đang tải Fanpage...');
    try {
        const response = await fetch('/api/fanpage/list');
        if (!response.ok) throw new Error('Lỗi mạng khi tải Fanpage');
        const pages = await response.json();
        
        elFpFilterPage.innerHTML = '';
        
        if (pages.length === 0) {
            setDropdownLoading(elFpFilterPage, 'Không tìm thấy Fanpage');
            return;
        }

        pages.forEach(page => {
            const option = new Option(page.name, page.id);
            elFpFilterPage.appendChild(option);
        });

        // Mở khóa bộ lọc
        elFpFilterPage.disabled = false;
        elFpFilterDate.disabled = false;
        elFpBtnApply.disabled = false;
        elFpBtnRefresh.disabled = false; // <-- Mở khóa nút Refresh

        // Tự động chọn Fanpage đầu tiên
        elFpFilterPage.value = pages[0].id;
        // Tự động nhấn "Áp dụng" lần đầu
        handleFanpageApplyFilters(); 

    } catch (error) {
        console.error('Lỗi tải Fanpage:', error);
        setDropdownLoading(elFpFilterPage, 'Lỗi tải Fanpage');
    }
}

/**
 * [MỚI] Lấy tham số ngày tháng từ bộ lọc Fanpage
 */
function getFanpageDateFilterParams(forRefresh = false) {
    let date_preset = elFpFilterDate.value;
    let start_date = elFpDateFrom.value;
    let end_date = elFpDateTo.value;
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
 * [MỚI] Xử lý sự kiện khi nhấn nút "Làm mới" trên panel Fanpage
 */
async function handleFanpageRefreshData() {
    if (!elFpFilterDate || !elFpBtnRefresh) return;

    const button = elFpBtnRefresh;
    const originalText = button.querySelector('span').innerText;
    setButtonLoading(button, 'Đang tải...');

    try {
        // Lấy khoảng ngày từ bộ lọc Fanpage
        const dateParams = getFanpageDateFilterParams(true);
        if (dateParams === null) throw new Error("Ngày không hợp lệ.");
        
        const response = await fetch('/api/refresh_fanpage', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(dateParams)
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Lỗi khi làm mới dữ liệu Fanpage.');
        }

        const result = await response.json();
        showNotification(result.message || 'Làm mới dữ liệu Fanpage thành công!', 'success');
        
        // Sau khi làm mới, tự động nhấn "Áp dụng" để tải lại dữ liệu
        handleFanpageApplyFilters(); 

    } catch (error) {
        console.error('Lỗi khi tải dữ liệu Fanpage:', error);
        showNotification(`Lỗi khi tải dữ liệu: ${error.message}`, 'error');
    } finally {
        setButtonIdle(button, originalText);
    }
}

/**
 * [MỚI] Xử lý sự kiện khi nhấn nút "Áp dụng" trên panel Fanpage
 */
async function handleFanpageApplyFilters() {
    if (!elFpFilterPage || !elFpFilterDate || !elFpBtnApply) return;

    const page_id = elFpFilterPage.value;
    const dateParams = getFanpageDateFilterParams(); // Lấy ngày từ hàm helper mới

    if (!page_id) {
        showNotification('Vui lòng chọn một Fanpage.', 'error');
        return;
    }
    if (dateParams === null) {
        // Hàm getFanpageDateFilterParams đã show thông báo lỗi
        return;
    }

    const originalText = elFpBtnApply.querySelector('span').innerText;
    setButtonLoading(elFpBtnApply, 'Đang tải...');

    // Xây dựng payload
    const payload = {
        page_id: page_id,
        ...dateParams // Bao gồm date_preset, start_date, end_date
    };

    // Gọi API tải ảnh bìa (không cần await, để nó chạy ngầm)
    loadAndRenderFanpageCover(page_id);

    try {
        const response = await fetch('/api/fanpage/overview_data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Lỗi khi tải dữ liệu Fanpage.');
        }

        const data = await response.json();
        
        // Gọi các hàm render chi tiết
        renderFanpageScorecards(data.scorecards);
        renderFanpageMainChart(data.mainChartData);
        renderFanpageInteractionsTable(data.interactionsTable);
        renderFanpageContentTypeTable(data.contentTypeTable);
        renderFanpageTopContent(data.topContentData);

    } catch (error) {
        console.error('Lỗi khi áp dụng bộ lọc Fanpage:', error);
        showNotification(`Lỗi: ${error.message}`, 'error');
    } finally {
        setButtonIdle(elFpBtnApply, originalText);
        feather.replace(); // Chạy lại feather icons sau khi render
    }
}

/**
 * [MỚI] Render 8 thẻ KPI (scorecards)
 */
function renderFanpageScorecards(scorecards) {
    if (!scorecards) return;

    document.getElementById('fp-kpi-total-likes').innerText = formatNumber(scorecards.total_likes);
    document.getElementById('fp-kpi-new-likes').innerText = formatNumber(scorecards.new_likes);
    document.getElementById('fp-kpi-new-likes-change').innerHTML = renderGrowthHtml(scorecards.new_likes_growth, 'percent');
    document.getElementById('fp-kpi-impressions').innerText = formatNumber(scorecards.impressions);
    document.getElementById('fp-kpi-impressions-growth').innerHTML = renderGrowthHtml(scorecards.impressions_growth, 'percent');
    document.getElementById('fp-kpi-engagement').innerText = formatNumber(scorecards.engagement);
    document.getElementById('fp-kpi-engagement-growth').innerHTML = renderGrowthHtml(scorecards.engagement_growth, 'percent');
    document.getElementById('fp-kpi-video-views').innerText = formatNumber(scorecards.video_views);
    document.getElementById('fp-kpi-video-views-growth').innerHTML = renderGrowthHtml(scorecards.video_views_growth, 'percent');
    document.getElementById('fp-kpi-clicks').innerText = formatNumber(scorecards.clicks);
    document.getElementById('fp-kpi-clicks-growth').innerHTML = renderGrowthHtml(scorecards.clicks_growth, 'percent');
    document.getElementById('fp-kpi-comments').innerText = formatNumber(scorecards.comments);
    document.getElementById('fp-kpi-comments-growth').innerHTML = renderGrowthHtml(scorecards.comments_growth, 'percent');
    document.getElementById('fp-kpi-post-likes').innerText = formatNumber(scorecards.post_likes);
    document.getElementById('fp-kpi-post-likes-growth').innerHTML = renderGrowthHtml(scorecards.post_likes_growth, 'percent');
}

/**
 * [MỚI] Render biểu đồ đường chính
 */
function renderFanpageMainChart(chartData) {
    if (!fpMainChartInstance || !chartData) return;

    fpMainChartInstance.data.labels = chartData.labels;
    
    // --- [SỬA ĐỔI] ---
    fpMainChartInstance.data.datasets = [
        {
            type: 'line', // <-- THÊM MỚI
            label: 'New likes',
            data: chartData.datasets[0].data,
            borderColor: chartData.datasets[0].borderColor,
            backgroundColor: 'rgba(59, 130, 246, 0.1)',
            fill: true,
            yAxisID: 'y',
            tension: 0.4 // Đảm bảo đường cong
        },
        {
            type: 'bar', // <-- THÊM MỚI
            label: 'Page Impressions',
            data: chartData.datasets[1].data,
            backgroundColor: chartData.datasets[1].borderColor, // Dùng màu đậm cho bar
            yAxisID: 'y1',
            stack: 'bar_stack' // <-- THÊM MỚI: Đặt tên stack
        },
        {
            type: 'bar', // <-- THÊM MỚI
            label: 'Page Post Engagements',
            data: chartData.datasets[2].data,
            backgroundColor: chartData.datasets[2].borderColor, // Dùng màu đậm cho bar
            yAxisID: 'y1',
            stack: 'bar_stack' // <-- THÊM MỚI: Đặt tên stack
        }
    ];

    // Cấu hình 2 trục Y (bật stacked cho y1)
    fpMainChartInstance.options.scales = {
        x: { 
            grid: { display: false },
            stacked: true // Đảm bảo trục X cũng stacked
        },
        y: { 
            type: 'linear', 
            display: true, 
            position: 'left',
            title: { display: true, text: 'New Likes' },
            grid: { display: false }
        },
        y1: { 
            type: 'linear', 
            display: true, 
            position: 'right', 
            title: { display: true, text: 'Impressions / Engagements' },
            grid: { drawOnChartArea: false },
            stacked: true // <-- THÊM MỚI: Bật stacked cho trục Y phụ
        }
    };
    fpMainChartInstance.update();
}

/**
 * [MỚI] Render bảng Tác động tương tác theo ngày
 */
function renderFanpageInteractionsTable(tableData) {
    const tableBody = document.getElementById('fp-interactions-table-body');
    if (!tableBody) return;

    let html = '';
    let totalImpressions = 0;
    let totalEngagement = 0;
    let totalImpressionsUnique = 0; // <-- THÊM MỚI
    let totalFanRemoves = 0; // <-- THÊM MỚI
    let totalVideoViews = 0;

    if (!tableData || tableData.length === 0) {
        html = `<tr><td colspan="6" class="text-center p-4 text-gray-500">Không có dữ liệu.</td></tr>`;
    } else {
        tableData.forEach(row => {
            html += `
                <tr class="border-b">
                    <td class="px-4 py-2.5">${row.date}</td>
                    <td class="px-4 py-2.5">${formatNumber(row.impressions)}</td>
                    <td class="px-4 py-2.5">${formatNumber(row.engagement)}</td>
                    <td class="px-4 py-2.5">${formatNumber(row.impressions_unique)}</td> 
                    <td class="px-4 py-2.5">${formatNumber(row.fan_removes)}</td> 
                    <td class="px-4 py-2.5">${formatNumber(row.video_views)}</td>
                </tr>
            `;
            // Cộng dồn tổng
            // Dùng Number() để đảm bảo cộng số
            totalImpressions += Number(row.impressions) || 0;
            totalEngagement += Number(row.engagement) || 0;
            totalImpressionsUnique += Number(row.impressions_unique) || 0; 
            totalFanRemoves += Number(row.fan_removes) || 0; 
            totalVideoViews += Number(row.video_views) || 0;
        });
    }

    // Luôn chèn dòng tổng cộng
    html += `
        <tr class="font-bold bg-gray-100 sticky bottom-0">
            <td class="px-4 py-3">Tổng cộng</td>
            <td id="fp-total-impressions" class="px-4 py-3">${formatNumber(totalImpressions)}</td>
            <td id="fp-total-engagement" class="px-4 py-3">${formatNumber(totalEngagement)}</td>
            <td id="fp-total-impressions-unique" class="px-4 py-3">${formatNumber(totalImpressionsUnique)}</td> 
            <td id="fp-total-fan-removes" class="px-4 py-3">${formatNumber(totalFanRemoves)}</td> 
            <td id="fp-total-video-views" class="px-4 py-3">${formatNumber(totalVideoViews)}</td>
        </tr>
    `;
    
    tableBody.innerHTML = html;
}

/**
 * [MỚI] Render bảng Số lượng Content (Nhóm Content)
 */
function renderFanpageContentTypeTable(tableData) {
    const tableBody = document.getElementById('fp-content-type-body');
    if (!tableBody) return;

    let html = '';
    let totalCount = 0;

    if (!tableData || tableData.length === 0) {
        html = `<tr><td colspan="2" class="text-center p-2 text-gray-500">Không có dữ liệu.</td></tr>`;
    } else {
        tableData.forEach(row => {
            html += `
                <tr>
                    <td class="p-2 capitalize">${row.type}</td>
                    <td class="p-2 text-right">${formatNumber(row.count)}</td>
                </tr>
            `;
            totalCount += row.count;
        });
    }

    // Chèn dòng tổng cộng
    html += `
        <tr class="font-bold bg-gray-50">
            <td class="p-2">Tổng cộng</td>
            <td id="fp-content-type-total" class="p-2 text-right">${formatNumber(totalCount)}</td>
        </tr>
    `;
    
    tableBody.innerHTML = html;
}

/**
 * [MỚI] Render 3 bảng Top 5 Content
 */
function renderFanpageTopContent(topContentData) {
    if (!topContentData) return;

    // --- LOGIC ĐÃ SỬA ---
    // Hàm helper để render 1 bảng
    const renderTopTable = (tbodyId, totalCellId, data) => {
        const tableBody = document.getElementById(tbodyId);
        if (!tableBody) return;

        let html = '';
        let total = 0;

        if (!data || data.length === 0) {
            html = `<tr><td colspan="3" class="text-center py-4 px-2 text-gray-500">N/A</td></tr>`;
        } else {
            data.forEach(row => {
                // Rút gọn message (truncation)
                const message = row.message ? (row.message.length > 40 ? row.message.substring(0, 40) + '...' : row.message) : 'N/A';
                const imageHtml = row.image 
                    ? `<img src="${row.image}" alt="Post thumbnail" class="w-8 h-8 object-cover rounded-md">`
                    : `<span class="flex items-center justify-center w-8 h-8 bg-gray-200 rounded-md">
                         <i data-feather="image" class="w-4 h-4 text-gray-500"></i>
                       </span>`;
                
                html += `
                    <tr>
                        <td class="py-2 px-2 w-10">${imageHtml}</td>
                        <td class="py-2 px-2 text-gray-700 truncate" title="${row.message || ''}">${message}</td>
                        <td class="py-2 px-2 text-right font-medium text-gray-900">${formatNumber(row.value)}</td>
                    </tr>
                `;
                total += Number(row.value) || 0; // Đảm bảo cộng số
            });
        }
        
        // 1. Render các dòng dữ liệu động
        tableBody.innerHTML = html;

        // 2. Cập nhật ô tổng cộng
        const totalCell = document.getElementById(totalCellId);
        if (totalCell) {
            totalCell.innerText = formatNumber(total);
        }
    };

    // Render 3 bảng
    renderTopTable('fp-top-content-impr-body', 'fp-top-impr-total', topContentData.impressions);
    renderTopTable('fp-top-content-like-body', 'fp-top-like-total', topContentData.likes);
    renderTopTable('fp-top-content-click-body', 'fp-top-click-total', topContentData.clicks);
}

/**
 * [MỚI] Tải và render ảnh bìa Fanpage
 * (Được gọi bởi handleFanpageApplyFilters)
 */
async function loadAndRenderFanpageCover(page_id) {
    const placeholder = document.getElementById('fp-cover-image-placeholder');
    if (!placeholder) return;

    // 1. Đặt về trạng thái đang tải
    placeholder.innerHTML = `
        <span class="text-gray-400 text-sm">Đang tải ảnh bìa...</span>
    `;

    try {
        // 2. Gọi API (đã viết ở app.py)
        const response = await fetch(`/api/fanpage/cover?page_id=${page_id}`);
        
        if (!response.ok) {
             // Lỗi 404 (không có ảnh) hoặc 500 (token hết hạn)
            const errorData = await response.json();
            throw new Error(errorData.error || 'Lỗi không xác định');
        }

        const data = await response.json();
        const cover_url = data.cover_url;

        if (cover_url) {
            // 3. Thành công: Render ảnh
            placeholder.innerHTML = `
                <img src="${cover_url}" alt="Ảnh bìa Fanpage" 
                     class="w-full h-full object-cover">
            `;
        } else {
            // API thành công nhưng không trả về URL
            throw new Error('Không tìm thấy URL ảnh bìa.');
        }

    } catch (error) {
        console.warn(`Không thể tải ảnh bìa: ${error.message}`);
        // 4. Thất bại: Render trạng thái mặc định
        placeholder.innerHTML = `
            <span class="text-gray-400 text-sm">Không có ảnh bìa</span>
        `;
    }
}