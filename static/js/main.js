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

// [MỚI] Biến cho Panel Chiến dịch (Campaign Dashboard)
let elPanelChienDich;
let elCdFilterAccount, elCdFilterTime, elCdFilterCampaigns;
let elCdBtnApply;
let elCdCustomDateRange, elCdDateFrom, elCdDateTo;
let cdAgeGenderChartInstance = null;
let cdRegionPieChartInstance = null;
let cdDrilldownChartInstance = null; // Biểu đồ khi click drilldown
let cdMapLoaded = false; // Biến cờ để chỉ tải bản đồ 1 lần


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
                messageDiv.classList.add("bg-blue-600", "text-white", "p-3", "rounded-lg", "max-w-xs", "shadow-sm", "break-words");
                messageDiv.textContent = text;
            } else if (sender === 'bot') {
                messageWrapper.classList.add("flex", "justify-start");
                messageDiv.classList.add("bg-gray-100", "text-gray-800", "p-3", "rounded-lg", "max-w-xs", "shadow-sm", "break-words");
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
            
            // Thêm indicator loading trước khi gọi API
            const loadingIndicatorWrapper = addMessage(null, "loading");
            let botMessageDiv = null; // Biến để giữ tham chiếu đến div tin nhắn của bot

            try {
                // [THAY ĐỔI LỚN] Sử dụng Fetch API với Response Streaming
                const response = await fetch("/api/chat", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ message: messageText }),
                });

                // Kiểm tra HTTP Status
                if (!response.ok || !response.body) {
                    const errorText = await response.text();
                    throw new Error(`Lỗi server (${response.status}): ${errorText.substring(0, 100)}...`);
                }
                
                // Khởi tạo div tin nhắn bot
                removeLoadingIndicator(); 
                const messageWrapper = addMessage("", "bot");
                botMessageDiv = messageWrapper.querySelector('div');
                botMessageDiv.textContent = ""; // Xóa nội dung mặc định

                // Khởi tạo Reader và Decoder cho luồng dữ liệu
                const reader = response.body.getReader();
                const decoder = new TextDecoder("utf-8");
                let buffer = "";

                // Bắt đầu đọc luồng
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;

                    buffer += decoder.decode(value, { stream: true });
                    
                    // Xử lý luồng SSE (Server-Sent Events)
                    // Vì chúng ta đang dùng format SSE: data: <JSON payload>\n\n
                    let boundary = buffer.indexOf('\n\n');
                    while (boundary !== -1) {
                        const chunk = buffer.substring(0, boundary).trim();
                        buffer = buffer.substring(boundary + 2);

                        if (chunk.startsWith("data:")) {
                            let jsonString = chunk.substring(5).trim();
                            
                            // [DONE] là tín hiệu kết thúc
                            if (jsonString === '[DONE]') {
                                break;
                            }
                            
                            try {
                                const payload = JSON.parse(jsonString);
                                
                                // Nếu có lỗi (được gửi trong luồng)
                                if (payload.error) {
                                    throw new Error(payload.error);
                                }
                                
                                // Gắn phần text chunk vào nội dung
                                if (payload.text) {
                                    botMessageDiv.textContent += payload.text;
                                    chatBody.scrollTop = chatBody.scrollHeight; // Cuộn xuống
                                }
                                
                            } catch (e) {
                                console.error("Lỗi parse JSON trong stream:", e);
                                // Gán lỗi và kết thúc
                                botMessageDiv.textContent += `\n\n[LỖI PHÂN TÍCH]`;
                                break; 
                            }
                        }

                        boundary = buffer.indexOf('\n\n');
                    }
                    if (buffer.includes('[DONE]')) break; // Thoát vòng lặp nếu DONE
                }
                
                // Xử lý các xuống dòng HTML sau khi stream xong
                if (botMessageDiv) {
                    botMessageDiv.innerHTML = botMessageDiv.textContent.replace(/\n/g, '<br>');
                }

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

    // --- PHẦN 3: LOGIC CHUYỂN PANEL ---
    console.log("Khởi tạo logic chuyển panel...");
    
    const navLinks = {
        'nav-tong-quan': document.getElementById('nav-tong-quan'),
        'nav-fanpage-overview': document.getElementById('nav-fanpage-overview'),
        'nav-chien-dich': document.getElementById('nav-chien-dich'), 
        'nav-huong-dan': document.getElementById('nav-huong-dan'), 
        'nav-cai-dat': document.getElementById('nav-cai-dat')
    };

    const panels = {
        'panel-tong-quan': document.getElementById('panel-tong-quan'),
        'panel-fanpage-overview': document.getElementById('panel-fanpage-overview'),
        'panel-chien-dich': document.getElementById('panel-chien-dich'), 
        'panel-huong-dan': document.getElementById('panel-huong-dan'), 
        'panel-cai-dat': document.getElementById('panel-cai-dat')
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

    if (navLinks['nav-chien-dich']) {
        navLinks['nav-chien-dich'].addEventListener('click', (e) => {
            e.preventDefault();
            setActiveLink(navLinks['nav-chien-dich']);
            showPanel('panel-chien-dich');
            
            // Tự động tải Account cho dropdown của Panel CĐ (nếu chưa)
            loadCDAccountDropdown();
        });
    }

    if (navLinks['nav-huong-dan']) {
        navLinks['nav-huong-dan'].addEventListener('click', (e) => {
            e.preventDefault();
            setActiveLink(navLinks['nav-huong-dan']);
            showPanel('panel-huong-dan');
        });
    }

    if (navLinks['nav-cai-dat']) {
        navLinks['nav-cai-dat'].addEventListener('click', (e) => {
            e.preventDefault();
            setActiveLink(navLinks['nav-cai-dat']);
            showPanel('panel-cai-dat');
        });
    }
    showPanel('panel-tong-quan'); // Bắt đầu ở Tổng quan

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
                    legend: { 
                        display: true, 
                        position: 'top'
                    } 
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
    }
    // [MỚI] Khởi tạo biểu đồ cho Panel Chiến dịch
    const ctxAgeGender = document.getElementById('sex-age-allocate');
    if (ctxAgeGender) {
        cdAgeGenderChartInstance = new Chart(ctxAgeGender.getContext('2d'), {
            type: 'bar', // Grouped bar chart
            data: { labels: [], datasets: [] },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { position: 'top' },
                    tooltip: {
                        callbacks: {
                            // Thêm tiêu đề cho tooltip (e.g., "Male - 18-24")
                            title: function(tooltipItems) {
                                const item = tooltipItems[0];
                                const age = item.label;
                                const gender = item.dataset.label;
                                return `${gender} | ${age}`;
                            }
                        }
                    }
                },
                scales: {
                    x: { stacked: false, grid: { display: false } },
                    y: { stacked: false, beginAtZero: true, title: { display: true, text: 'Impressions' } }
                },
                // [MỚI] Thêm sự kiện onClick để drilldown
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const chart = cdAgeGenderChartInstance;
                        const elementIndex = elements[0].index;
                        const datasetIndex = elements[0].datasetIndex;
                        
                        const age = chart.data.labels[elementIndex];
                        const gender = chart.data.datasets[datasetIndex].label.toLowerCase(); // 'Female' -> 'female'
                        
                        // Gọi hàm drilldown
                        handleAgeGenderDrilldown(age, gender);
                    }
                }
            }
        });
    }

    const ctxRegionPie = document.getElementById('region-pie-chart');
    if (ctxRegionPie) {
        cdRegionPieChartInstance = new Chart(ctxRegionPie.getContext('2d'), {
            type: 'doughnut',
            data: { labels: ['Chưa tải'], datasets: [{ data: [1] }] },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    legend: { 
                        position: 'right',
                        labels: {
                            boxWidth: 20
                        }
                    } 
                }
            }
        });
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
    elFpBtnRefresh = document.getElementById('fp-btn-refresh'); 
    elFpCustomDateRange = document.getElementById('fp-custom-date-range'); 
    elFpDateFrom = document.getElementById('fp-date-from'); 
    elFpDateTo = document.getElementById('fp-date-to'); 

    // Vô hiệu hóa bộ lọc fanpage ban đầu
    if (elFpFilterPage) elFpFilterPage.disabled = true;
    if (elFpFilterDate) elFpFilterDate.disabled = true;
    if (elFpBtnApply) elFpBtnApply.disabled = true;
    if (elFpBtnRefresh) elFpBtnRefresh.disabled = true;

    // [MỚI] Khởi tạo biến cho Panel Chiến dịch
    elPanelChienDich = document.getElementById('panel-chien-dich');
    elCdFilterAccount = document.getElementById('cd-filter-ad-account');
    elCdFilterTime = document.getElementById('cd-filter-time');
    elCdFilterCampaigns = document.getElementById('cd-filter-campaigns');
    elCdBtnApply = document.getElementById('cd-btn-apply-filters');
    elCdCustomDateRange = document.getElementById('cd-custom-date-range');
    elCdDateFrom = document.getElementById('cd-date-from');
    elCdDateTo = document.getElementById('cd-date-to');

    // Vô hiệu hóa bộ lọc CĐ ban đầu
    if (elCdFilterAccount) elCdFilterAccount.disabled = true;
    if (elCdFilterTime) elCdFilterTime.disabled = true;
    if (elCdFilterCampaigns) {
        elCdFilterCampaigns.disabled = true;
        elCdFilterCampaigns.loadOptions();
    }
    if (elCdBtnApply) elCdBtnApply.disabled = true;
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
    // [MỚI] Gán sự kiện cho Panel Chiến dịch
    if (elCdFilterAccount) {
        elCdFilterAccount.addEventListener('change', triggerCDCampaignLoad);
    }
    if (elCdFilterTime) {
        elCdFilterTime.addEventListener('change', (e) => {
            const value = e.currentTarget.value;
            if (value === 'custom') {
                elCdCustomDateRange.classList.remove('hidden');
                resetDropdown(elCdFilterCampaigns);
            } else {
                elCdCustomDateRange.classList.add('hidden');
                triggerCDCampaignLoad();
            }
        });
    }
    if (elCdDateFrom) elCdDateFrom.addEventListener('change', triggerCDCampaignLoad);
    if (elCdDateTo) elCdDateTo.addEventListener('change', triggerCDCampaignLoad);
    
    if (elCdBtnApply) {
        elCdBtnApply.addEventListener('click', handleCDApplyFilters);
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

        // --- [BỔ SUNG LOGIC VALIDATION] ---
        if (dateParams.date_preset) {
            // 1. Chặn ngay lập tức nếu dùng Preset (7 ngày qua, Tháng này...)
            throw new Error("Làm mới: Vui lòng sử dụng 'Tùy chỉnh' và chọn tối đa 2 ngày.");
        }
        else if (dateParams.start_date && dateParams.end_date) { 
            const startDate = new Date(dateParams.start_date);
            const endDate = new Date(dateParams.end_date);
            const diffTime = Math.abs(endDate - startDate);
            // +1 để đếm cả ngày bắt đầu
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24)) + 1; 

            if (diffDays >= 2) {
                // Nếu >= 2 ngày, báo lỗi và dừng lại
                throw new Error("Tùy chỉnh: Chỉ được làm mới dữ liệu trong khoảng 1 ngày một lần.");
            }
        }

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
        showNotification(result.message || 'Đã tiếp nhận yêu cầu làm mới dữ liệu Ads!', 'success');
        startAdsRefreshStatusCheck(button, originalText); 
    } catch (error) {
        console.error('Lỗi khi tải dữ liệu:', error);
        // Lỗi (bao gồm cả lỗi validation) sẽ được show ở đây
        showNotification(`Lỗi khi tải dữ liệu: ${error.message}`, 'error');
    } finally {
        // Đặt nút về trạng thái bình thường nếu có lỗi (trước khi API được gọi)
        // hoặc nếu API không thành công (mà không có status check)
        if (!task_status['ads_refreshing']) { // Chỉ set idle nếu không đang chạy ngầm
             setButtonIdle(button, originalText);
        }
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
    if (spendTrendChartInstance) {
        
        spendTrendChartInstance.data.labels = chartData.labels;
        spendTrendChartInstance.data.datasets = [
            {
                type: 'line', // Chỉ định đây là 'line'
                label: 'Chi phí (Spend)',
                data: chartData.datasets[0].data,
                borderColor: 'rgb(255, 99, 132)',
                
                backgroundColor: 'rgba(255, 99, 132, 0.1)', // Màu fill nhạt
                fill: true, 
                
                yAxisID: 'y', 
                tension: 0.4 // Đảm bảo line cong
            },
            {
                type: 'line', // Chỉ định đây là 'bar'
                label: 'Lượt hiển thị (Impressions)',
                data: chartData.datasets[1].data,
                borderColor: 'rgb(54, 162, 235)',

                backgroundColor: 'rgba(109, 171, 243, 0.1)', // Màu fill nhạt
                fill: true,

                yAxisID: 'y1', 
                tension: 0.4
            }
        ];

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
    const timeFilter = document.getElementById('filter-time');
    let date_preset = timeFilter.value;
    let start_date = document.getElementById('date-from').value;
    let end_date = document.getElementById('date-to').value;
    const today = new Date().toISOString().split('T')[0];

    if (date_preset !== 'custom') {
        start_date = null;
        // [SỬA LỖI] Luôn đặt end_date là 'today' khi dùng preset
        // để backend (app.py) luôn dùng 'today' làm mốc tính toán
        end_date = today; 
    } else {
        // (Logic 'custom' giữ nguyên)
        if (!start_date || !end_date) {
            showNotification('Tùy chỉnh: Vui lòng chọn Từ ngày và Đến ngày!', 'error');
            return null;
        }
        date_preset = null;
    }
    
    // Fallback cuối cùng (nếu 'custom' nhưng end_date rỗng)
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

// [MỚI] Logic tự động mở Panel dựa trên URL Param (để giữ trạng thái sau khi Admin submit form)
    const urlParams = new URLSearchParams(window.location.search);
    const activePanel = urlParams.get('panel');

    if (activePanel === 'settings') {
        const settingsLink = document.getElementById('nav-cai-dat');
        if (settingsLink) {
            // Nếu link tồn tại (nghĩa là user là admin), giả lập click vào nó
            settingsLink.click();
            
            // Xóa param khỏi URL để nhìn cho sạch (không reload trang)
            window.history.replaceState({}, document.title, "/");
        }
    } else {
        // Mặc định mở tab Tổng quan như cũ
        showPanel('panel-tong-quan'); 
    }

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

    if (date_preset !== 'custom') {
        start_date = null; 
        // [SỬA LỖI] Luôn đặt end_date là 'today' khi dùng preset
        end_date = today;
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
        
        // --- [BỔ SUNG LOGIC VALIDATION] ---
        if (dateParams.date_preset) {
            // 1. Chặn nếu người dùng chọn preset
            throw new Error("Làm mới: Vui lòng sử dụng 'Tùy chỉnh' và chọn 1 ngày.");

        } else if (dateParams.start_date && dateParams.end_date) { 
            // 2. Kiểm tra 'Tùy chỉnh' (giữ nguyên logic cũ)
            const startDate = new Date(dateParams.start_date);
            const endDate = new Date(dateParams.end_date);
            const diffTime = Math.abs(endDate - startDate);
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24)) + 1;

            if (diffDays >= 2) {
                // Nếu >= 3 ngày, báo lỗi và dừng lại
                throw new Error("Tùy chỉnh: Chỉ được làm mới dữ liệu trong khoảng 1 ngày một lần.");
            }
        }
        
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
        showNotification(result.message || 'Đã tiếp nhận yêu cầu làm mới Fanpage!', 'success');
        
        startFanpageRefreshStatusCheck(button, originalText); 

    } catch (error) {
        console.error('Lỗi khi tải dữ liệu Fanpage:', error);
        // Lỗi (bao gồm cả lỗi validation) sẽ được show ở đây
        showNotification(`Lỗi khi tải dữ liệu: ${error.message}`, 'error');
    }
}

/**
 * [MỚI] Kiểm tra trạng thái làm mới dữ liệu Ads (Panel Tổng quan)
 */
function startAdsRefreshStatusCheck(button, originalText) {
    const checkStatus = async () => {
        try {
            const response = await fetch('/api/status/ads');
            const statusData = await response.json();

            if (statusData.status === 'ads_refreshing') {
                // Vẫn đang chạy, đặt trạng thái loading và lặp lại sau 3 giây
                button.querySelector('span').innerText = `Đang cập nhật (${statusData.elapsed_time}s)...`;
                setTimeout(checkStatus, 3000);
            } else {
                // Đã xong hoặc lỗi
                showNotification('Cập nhật dữ liệu Ads HOÀN TẤT.', 'success');
                setButtonIdle(button, originalText);
                triggerCampaignLoad(); // Tải lại dropdowns
                handleApplyFilters(); // Tải lại dashboard
            }
        } catch (error) {
            console.error('Lỗi kiểm tra trạng thái Ads:', error);
            showNotification('Lỗi server khi kiểm tra trạng thái Ads.', 'error');
            setButtonIdle(button, originalText);
        }
    };
    // Khởi động lần đầu
    setTimeout(checkStatus, 100); 
}

/**
 * [MỚI] Kiểm tra trạng thái làm mới dữ liệu Fanpage (Panel Fanpage)
 */
function startFanpageRefreshStatusCheck(button, originalText) {
    const checkStatus = async () => {
        try {
            const response = await fetch('/api/status/fanpage');
            const statusData = await response.json();

            if (statusData.status === 'fanpage_refreshing') {
                // Vẫn đang chạy
                button.querySelector('span').innerText = `Đang cập nhật (${statusData.elapsed_time}s)...`;
                setTimeout(checkStatus, 3000);
            } else {
                // Đã xong hoặc lỗi
                showNotification('Cập nhật dữ liệu Fanpage HOÀN TẤT.', 'success');
                setButtonIdle(button, originalText);
                handleFanpageApplyFilters(); // Tải lại dashboard
            }
        } catch (error) {
            console.error('Lỗi kiểm tra trạng thái Fanpage:', error);
            showNotification('Lỗi server khi kiểm tra trạng thái Fanpage.', 'error');
            setButtonIdle(button, originalText);
        }
    };
    // Khởi động lần đầu
    setTimeout(checkStatus, 100); 
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
    
    fpMainChartInstance.data.datasets = [
        {
            type: 'line', 
            label: 'New likes', // <-- (Yêu cầu 2)
            data: chartData.datasets[0].data,
            borderColor: chartData.datasets[0].borderColor,
            backgroundColor: 'rgba(59, 130, 246, 0.1)',
            fill: true,
            yAxisID: 'y',
            tension: 0.4
        },
        {
            type: 'bar', 
            label: 'Page Impressions',
            data: chartData.datasets[1].data,
            backgroundColor: chartData.datasets[1].borderColor, 
            yAxisID: 'y1',
            stack: 'bar_stack'
        },
        {
            type: 'bar', 
            label: 'Page Post Engagements',
            data: chartData.datasets[2].data,
            backgroundColor: chartData.datasets[2].borderColor, 
            yAxisID: 'y1',
            stack: 'bar_stack'
        }
    ];

    // Cấu hình 2 trục Y
    fpMainChartInstance.options.scales = {
        x: { 
            grid: { display: false },
            stacked: true
        },
        y: { 
            type: 'linear', 
            display: true, 
            position: 'left',
            title: { display: true, text: 'New Likes' },
            grid: { display: false } // Giữ lưới của trục này ẩn
        },
        y1: { 
            type: 'linear', 
            display: true, 
            position: 'right', 
            title: { display: true, text: 'Impressions / Engagements' },
            // [SỬA] (Yêu cầu 1) Xóa 'grid: { drawOnChartArea: false }'
            // để các đường lưới ngang hiển thị
            stacked: true 
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

// ======================================================================
// --- [MỚI] CÁC HÀM LOGIC CHO PANEL CHIẾN DỊCH ---
// ======================================================================

/**
 * [MỚI] Lấy tham số ngày tháng từ bộ lọc Panel Chiến dịch
 */
function getCDDateFilterParams() {
    let date_preset = elCdFilterTime.value;
    let start_date = elCdDateFrom.value;
    let end_date = elCdDateTo.value;
    const today = new Date().toISOString().split('T')[0];

    if (date_preset !== 'custom') {
        start_date = null; 
        // [SỬA LỖI] Luôn đặt end_date là 'today' khi dùng preset
        end_date = today;
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
 * [MỚI] Lấy bộ lọc đầy đủ từ Panel Chiến dịch
 */
function getCDFilterPayload() {
    const filters = {};
    filters.account_id = elCdFilterAccount.value;
    if (!filters.account_id) {
        showNotification('Vui lòng chọn một Tài khoản Quảng cáo.', 'error');
        return null;
    }
    const dateParams = getCDDateFilterParams();
    if (dateParams === null) { return null; }
    filters.date_preset = dateParams.date_preset;
    filters.start_date = dateParams.start_date;
    filters.end_date = dateParams.end_date;
    
    const campaignIds = Array.from(elCdFilterCampaigns.selectedOptions).map(o => o.value);
    if (campaignIds.length > 0) { 
        filters.campaign_ids = campaignIds; 
    }
    
    return filters;
}

/**
 * [MỚI] Tải danh sách Account cho Panel Chiến dịch
 */
async function loadCDAccountDropdown() {
    if (!elCdFilterAccount || elCdFilterAccount.options.length > 1) {
        // Chỉ tải nếu dropdown rỗng (chưa tải lần nào)
        return; 
    }

    setDropdownLoading(elCdFilterAccount, 'Đang tải tài khoản...');
    try {
        const response = await fetch('/api/accounts');
        if (!response.ok) throw new Error('Lỗi mạng khi tải tài khoản');
        const accounts = await response.json();
        
        elCdFilterAccount.innerHTML = ''; // Xóa 'Đang tải...'
        
        accounts.forEach(c => {
            const idString = String(c.id);
            const lastFourDigits = idString.slice(-4);
            const newText = `${c.name} (${lastFourDigits})`;
            const option = new Option(newText, c.id);
            elCdFilterAccount.appendChild(option);
        });
        
        // Mở khóa bộ lọc
        elCdFilterAccount.disabled = false;
        elCdFilterTime.disabled = false;
        elCdBtnApply.disabled = false;

        if (accounts.length > 0) {
            elCdFilterAccount.value = accounts[0].id;
            // Kích hoạt tải campaign
            elCdFilterAccount.dispatchEvent(new Event('change'));
        }
    } catch (error) {
        console.error('Lỗi tải tài khoản CĐ:', error);
        setDropdownLoading(elCdFilterAccount, 'Lỗi tải tài khoản');
    }
}

/**
 * [MỚI] Kích hoạt tải Campaign cho Panel CĐ
 */
function triggerCDCampaignLoad() {
    const accountId = elCdFilterAccount.value;
    const dateParams = getCDDateFilterParams();
    
    resetDropdown(elCdFilterCampaigns);
    
    if (accountId && dateParams) {
        loadCDCampaignDropdown(accountId, dateParams);
    }
}

/**
 * [MỚI] Tải danh sách Campaign cho Panel CĐ
 */
async function loadCDCampaignDropdown(accountId, dateParams) {
    setDropdownLoading(elCdFilterCampaigns, 'Đang tải chiến dịch...');
    try {
        const response = await fetch('/api/campaigns', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ account_id: accountId, ...dateParams })
        });
        if (!response.ok) throw new Error('Lỗi mạng');
        const campaigns = await response.json();
        
        elCdFilterCampaigns.innerHTML = '';
        campaigns.forEach(c => {
            const option = new Option(c.name, c.campaign_id);
            elCdFilterCampaigns.appendChild(option);
        });
        
        elCdFilterCampaigns.disabled = false;
        elCdFilterCampaigns.loadOptions(); // Tải lại multiselect
    } catch (error) {
        console.error('Lỗi tải chiến dịch CĐ:', error);
        setDropdownLoading(elCdFilterCampaigns, 'Lỗi tải chiến dịch...');
    }
}


/**
 * [MỚI] Xử lý sự kiện khi nhấn nút "Áp dụng" trên panel Chiến dịch
 */
async function handleCDApplyFilters() {
    const button = elCdBtnApply;
    const originalText = button.querySelector('span').innerText;
    setButtonLoading(button, 'Đang tải...');

    const filters = getCDFilterPayload();
    if (!filters) {
        setButtonIdle(button, originalText);
        return;
    }

    // Hủy biểu đồ drilldown cũ (nếu có)
    if (cdDrilldownChartInstance) {
        cdDrilldownChartInstance.destroy();
        cdDrilldownChartInstance = null;
        document.getElementById('drilldown-chart-container').innerHTML = '';
        document.getElementById('drilldown-modal-title').innerText = 'Drilldown';
        document.getElementById('drilldown-modal').classList.add('hidden');
    }

    try {
        const [mapRes, performanceRes, ageGenderRes, waffleRes] = await Promise.all([
            fetch('/api/geo_map_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
            }),
            fetch('/api/camp_performance', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
            }),
            fetch('/api/age_gender_chart', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
            }),
            fetch('/api/waffle_chart', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters)
            })
        ]);

        if (!mapRes.ok) { const err = await mapRes.json(); throw new Error(`Lỗi bản đồ: ${err.error}`); }
        if (!performanceRes.ok) { const err = await performanceRes.json(); throw new Error(`Lỗi bảng: ${err.error}`); }
        if (!ageGenderRes.ok) { const err = await ageGenderRes.json(); throw new Error(`Lỗi biểu đồ A/G: ${err.error}`); }
        if (!waffleRes.ok) { const err = await waffleRes.json(); throw new Error(`Lỗi Waffle: ${err.error}`); }

        const mapData = await mapRes.json();
        const performanceData = await performanceRes.json();
        const ageGenderData = await ageGenderRes.json();
        const waffleData = await waffleRes.json();

        // Render tất cả
        renderGeoMapData(mapData);
        renderCDPerformanceData(performanceData);
        renderAgeGenderChart(ageGenderData);
        renderWaffleChart(waffleData);

    } catch (error) {
        console.error('Lỗi khi áp dụng bộ lọc CĐ:', error);
        showNotification(`Lỗi khi lấy dữ liệu: ${error.message}`, 'error');
    } finally {
        setButtonIdle(button, originalText);
        feather.replace();
    }
}

/**
 * [MỚI] Render bản đồ Folium
 */
function renderGeoMapData(data) {
    const container = document.getElementById('folium-map-container');
    if (!container) return;

    if (data.error) {
        container.innerHTML = `<p class="text-red-500">Lỗi: ${data.error}</p>`;
        return;
    }
    
    container.innerHTML = ''; // Xóa chữ "Đang tải..."
    const iframe = document.createElement('iframe');
    iframe.srcdoc = data.map_html; // Đặt HTML của bản đồ vào srcdoc
    iframe.style.width = '100%';
    iframe.style.height = '100%';
    iframe.style.minHeight = '500px'; 
    iframe.style.border = 'none';
    iframe.style.borderRadius = '0.5rem'; // bo góc
    
    container.appendChild(iframe);
    
    // Đánh dấu là đã tải thành công
    cdMapLoaded = true;
}

/**
 * [MỚI] Render 3 Bảng (Gender, Age, Geo) và 1 Pie Chart (Region)
 */
function renderCDPerformanceData(data) {
    if (!data) return;

    // --- 1. Bảng Giới tính ---
    const genderTableBody = document.getElementById('gender-table-body');
    let genderHtml = '';
    let gTotalImp = 0, gTotalClicks = 0, gTotalValue = 0;
    
    if (data.gender_table.length > 0) {
        data.gender_table.forEach(row => {
            genderHtml += `
                <tr class="border-b">
                    <td class="px-4 py-2.5 capitalize">${row.gender}</td>
                    <td class="px-4 py-2.5 text-right">${formatNumber(row.impressions)}</td>
                    <td class="px-4 py-2.5 text-right">${formatNumber(row.clicks)}</td>
                    <td class="px-4 py-2.5 text-right">${formatCurrency(row.purchase_value)}</td>
                </tr>
            `;
            gTotalImp += Number(row.impressions) || 0;
            gTotalClicks += Number(row.clicks) || 0;
            gTotalValue += Number(row.purchase_value) || 0;
        });
    } else {
        genderHtml = '<tr><td colspan="4" class="text-center p-4 text-gray-500">Không có dữ liệu.</td></tr>';
    }
        
    // [SỬA LỖI] Thêm dòng "Tổng cộng" vào chuỗi HTML
    genderHtml += `
        <tr class="font-bold bg-gray-100 sticky bottom-0">
            <td class="px-4 py-3">Tổng cộng</td>
            <td class="px-4 py-3 text-right">${formatNumber(gTotalImp)}</td>
            <td class="px-4 py-3 text-right">${formatNumber(gTotalClicks)}</td>
            <td class="px-4 py-3 text-right">${formatCurrency(gTotalValue)}</td>
        </tr>
    `;
    genderTableBody.innerHTML = genderHtml; // Ghi đè bằng HTML hoàn chỉnh

    // --- 2. Bảng Độ tuổi ---
    const ageTableBody = document.getElementById('age-table-body');
    let ageHtml = '';
    let aTotalImp = 0, aTotalClicks = 0, aTotalValue = 0;
    
    if (data.age_table.length > 0) {
        data.age_table.forEach(row => {
            ageHtml += `
                <tr class="border-b">
                    <td class="px-4 py-2.5">${row.age}</td>
                    <td class="px-4 py-2.5 text-right">${formatNumber(row.impressions)}</td>
                    <td class="px-4 py-2.5 text-right">${formatNumber(row.clicks)}</td>
                    <td class="px-4 py-2.5 text-right">${formatCurrency(row.purchase_value)}</td>
                </tr>
            `;
            aTotalImp += Number(row.impressions) || 0;
            aTotalClicks += Number(row.clicks) || 0;
            aTotalValue += Number(row.purchase_value) || 0;
        });
    } else {
        ageHtml = '<tr><td colspan="4" class="text-center p-4 text-gray-500">Không có dữ liệu.</td></tr>';
    }
    
    // [SỬA LỖI] Thêm dòng "Tổng cộng" vào chuỗi HTML
    ageHtml += `
        <tr class="font-bold bg-gray-100 sticky bottom-0">
            <td class="px-4 py-3">Tổng cộng</td>
            <td class="px-4 py-3 text-right">${formatNumber(aTotalImp)}</td>
            <td class,"px-4 py-3 text-right">${formatNumber(aTotalClicks)}</td>
            <td class="px-4 py-3 text-right">${formatCurrency(aTotalValue)}</td>
        </tr>
    `;
    ageTableBody.innerHTML = ageHtml; // Ghi đè bằng HTML hoàn chỉnh


    // --- 3. Bảng Địa lý ---
    const geoTableBody = document.getElementById('geo-table-body');
    let geoHtml = '';
    let geoTotalSpend = 0;
    let geoTotalPurchases = 0;
    // Không cộng dồn CPA, chỉ cộng dồn Spend và Purchases
    
    if (data.geo_table.length > 0) {
        data.geo_table.forEach(row => {
            geoHtml += `
                <tr class="border-b">
                    <td class="px-4 py-2.5">${row.region_name}</td>
                    <td class="px-4 py-2.5 text-right">${formatCurrency(row.spend)}</td>
                    <td class="px-4 py-2.5 text-right">${formatNumber(row.purchases)}</td>
                    <td class="px-4 py-2.5 text-right">${formatCurrency(row.cpa)}</td>
                </tr>
            `;
            // Cộng dồn 2 chỉ số gốc
            geoTotalSpend += Number(row.spend) || 0;
            geoTotalPurchases += Number(row.purchases) || 0;
        });
    } else {
        geoHtml = '<tr><td colspan="4" class="text-center p-4 text-gray-500">Không có dữ liệu.</td></tr>';
    }
    
    // Tính CPA tổng (Total CPA) dựa trên Tổng Spend / Tổng Purchases
    const totalAvgCPA = geoTotalPurchases > 0 ? (geoTotalSpend / geoTotalPurchases) : 0;
    
    geoHtml += `
        <tr class="font-bold bg-gray-100 sticky bottom-0">
            <td class="px-4 py-3">Tổng cộng</td>
            <td class="px-4 py-3 text-right">${formatCurrency(geoTotalSpend)}</td>
            <td class="px-4 py-3 text-right">${formatNumber(geoTotalPurchases)}</td>
            <td class="px-4 py-3 text-right">${formatCurrency(totalAvgCPA)}</td>
        </tr>
    `;
    geoTableBody.innerHTML = geoHtml; // Ghi đè bằng HTML hoàn chỉnh
    
    // --- 4. Biểu đồ tròn Địa lý ---
    if (cdRegionPieChartInstance) {
        if (data.geo_pie_chart.labels.length > 0) {
            cdRegionPieChartInstance.data.labels = data.geo_pie_chart.labels;
            cdRegionPieChartInstance.data.datasets = data.geo_pie_chart.datasets;
        } else {
            cdRegionPieChartInstance.data.labels = ['Không có dữ liệu'];
            cdRegionPieChartInstance.data.datasets = [{ data: [1], backgroundColor: ['#E5E7EB'] }];
        }
        cdRegionPieChartInstance.update();
    }
}

/**
 * [MỚI] Render biểu đồ cột đôi (Grouped Bar) Age-Gender
 */
function renderAgeGenderChart(data) {
    if (!cdAgeGenderChartInstance) return;
    
    if (data.labels.length > 0) {
        cdAgeGenderChartInstance.data.labels = data.labels;
        cdAgeGenderChartInstance.data.datasets = data.datasets;
    } else {
        cdAgeGenderChartInstance.data.labels = ['Không có dữ liệu'];
        cdAgeGenderChartInstance.data.datasets = [];
    }
    cdAgeGenderChartInstance.update();
}

/**
 * [MỚI] Xử lý sự kiện click drilldown từ biểu đồ Age-Gender
 */
async function handleAgeGenderDrilldown(age, gender) {
    console.log(`Bắt đầu drilldown cho: Age ${age}, Gender ${gender}`);
    
    // 1. Lấy bộ lọc CĐ hiện tại
    const filters = getCDFilterPayload();
    if (!filters) {
        showNotification('Không thể drilldown, bộ lọc không hợp lệ.', 'error');
        return;
    }

    // 2. Chuẩn bị payload cho API drilldown
    const payload = {
        ...filters, // Bộ lọc chính (account, date, campaigns)
        group_by_dimension: 'campaign',
        primary_metric: 'impressions',
        secondary_metric: 'purchase_value',
        drilldown_filters: { // Bộ lọc phụ
            age: age,
            gender: gender
        }
    };
    
    // 3. Hiển thị modal và trạng thái loading
    const modal = document.getElementById('drilldown-modal');
    const modalTitle = document.getElementById('drilldown-modal-title');
    const chartContainer = document.getElementById('drilldown-chart-container');
    
    modalTitle.innerText = `Top 5 Chiến dịch cho: ${gender.toUpperCase()} (Tuổi ${age})`;
    chartContainer.innerHTML = '<p class="text-gray-500">Đang tải dữ liệu drilldown...</p>';
    modal.classList.remove('hidden');

    // 4. Hủy biểu đồ cũ nếu có
    if (cdDrilldownChartInstance) {
        cdDrilldownChartInstance.destroy();
        cdDrilldownChartInstance = null;
    }

    try {
        // 5. Gọi API
        const response = await fetch('/api/drilldown_chart', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            const err = await response.json();
            throw new Error(err.error || 'Lỗi tải dữ liệu drilldown');
        }

        const chartData = await response.json();

        // 6. Vẽ biểu đồ mới
        if (chartData.labels.length === 0) {
             chartContainer.innerHTML = '<p class="text-gray-500">Không tìm thấy dữ liệu drilldown cho nhóm này.</p>';
             return;
        }
        
        // Xóa 'Đang tải...'
        chartContainer.innerHTML = ''; 
        const canvas = document.createElement('canvas');
        chartContainer.appendChild(canvas);

        cdDrilldownChartInstance = new Chart(canvas.getContext('2d'), {
            type: 'bar', // Combo chart
            data: chartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'top' } },
                scales: {
                    x: { grid: { display: false } },
                    y: { 
                        type: 'linear', 
                        position: 'left', 
                        title: { display: true, text: 'Impressions' },
                        beginAtZero: true
                    },
                    y1: { 
                        type: 'linear', 
                        position: 'right', 
                        title: { display: true, text: 'Purchase Value' },
                        grid: { drawOnChartArea: false }, // Chỉ vẽ lưới cho trục chính
                        beginAtZero: true
                    }
                }
            }
        });

    } catch (error) {
        console.error('Lỗi khi drilldown:', error);
        chartContainer.innerHTML = `<p class="text-red-500">Lỗi: ${error.message}</p>`;
    }
}

// (Thay thế hàm renderWaffleChart cũ bằng hàm này)

/**
 * [MỚI] Render Waffle Chart
 */
async function renderWaffleChart(data) {
    const container = document.getElementById('waffle-chart-container');
    if (!container) return;
        
    try {
        // 1. Kiểm tra xem data (kết quả JSON) có lỗi không
        if (data.error) {
            throw new Error(data.error);
        }

        // 2. Kiểm tra xem có ảnh base64 không
        if (data.image_base64) {
            container.innerHTML = `
                <img src="${data.image_base64}" alt="Waffle Chart Purchase Value Campaign-based" 
                     class="w-full h-auto object-contain">
            `;
        } else {
            // Trường hợp API chạy thành công nhưng không tạo ra ảnh
            throw new Error('Không nhận được dữ liệu ảnh từ server.');
        }

    } catch (error) {
        console.error('Lỗi khi render Waffle Chart:', error);
        // Hiển thị lỗi ngay tại container
        container.innerHTML = `<p class="text-red-500 p-4">Lỗi: ${error.message}</p>`;
    }
}

/**
 * [MỚI] Đóng modal drilldown
 */
function closeDrilldownModal(event) {
    // Ngăn sự kiện click lan truyền
    if (event) event.stopPropagation();
    
    const modal = document.getElementById('drilldown-modal');
    if (modal) {
        modal.classList.add('hidden');
    }
    
    // Hủy biểu đồ cũ để giải phóng bộ nhớ
    if (cdDrilldownChartInstance) {
        cdDrilldownChartInstance.destroy();
        cdDrilldownChartInstance = null;
    }
}