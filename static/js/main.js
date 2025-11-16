/**
 * main.js
 * File JavaScript chính cho Meta Ads Dashboard
 * CẤU TRÚC KẾT HỢP:
 * - Sử dụng <select> đơn tiêu chuẩn (DOM) cho Account, Time.
 * - Sử dụng MultiselectDropdown.js cho multi-select (Campaigns, Adsets, Ads).
 * - Bổ sung logic cho AI Chatbot.
 * - [MỚI] Bổ sung logic chuyển đổi Panel (Tổng quan vs Fanpage).
 */

// --- BIẾN TOÀN CỤC ---
let spendTrendChartInstance = null;
let platformChartInstance = null;

let elAccount, elTime;
let elCampaigns, elAdsets, elAds;

// Thêm biến cho 2 dropdown của biểu đồ tròn
let elChartMetric, elChartDimension;

// --- HÀM KHỞI CHẠY KHI TRANG ĐƯỢC TẢI ---
document.addEventListener('DOMContentLoaded', () => {
    
    // --- PHẦN 1: LOGIC DASHBOARD (ĐÃ CÓ) ---
    MultiselectDropdown(window.MultiselectDropdownOptions); 
    feather.replace(); // Chạy feather icons cho toàn bộ trang
    initializeCharts();
    initializeSelects(); 
    setupEventListeners();
    loadAccountDropdown();
    
    // --- PHẦN 2: BỔ SUNG LOGIC CHO CHATBOT AI ---
    console.log("Khởi tạo logic cho Chatbot AI...");

    // === Khai báo biến ===
    const chatBubble = document.getElementById("chat-bubble");
    const chatWindow = document.getElementById("chat-window");
    const minimizeBtn = document.getElementById("chat-minimize-btn");
    const chatBody = document.getElementById("chat-body");
    const input = document.getElementById("user-message-input");
    const sendBtn = document.getElementById("send-message-btn");

    // An toàn: Kiểm tra xem các element có tồn tại không
    if (!chatBubble || !chatWindow || !minimizeBtn || !chatBody || !input || !sendBtn) {
        console.warn("Không tìm thấy các thành phần UI của Chatbot. Chatbot sẽ không hoạt động.");
        // Không return, để dashboard vẫn chạy
    } else {
        // === Logic Đóng/Mở Cửa sổ Chat ===
        function openChatWindow() {
            chatBubble.classList.add("opacity-0", "translate-y-8");
            chatBubble.classList.add("hidden");
            
            chatWindow.classList.remove("hidden");
            setTimeout(() => {
                chatWindow.classList.remove("opacity-0", "translate-y-full");
            }, 10); // Đợi 1 frame
        }

        function closeChatWindow() {
            chatWindow.classList.add("opacity-0", "translate-y-full");
            setTimeout(() => {
                chatWindow.classList.add("hidden");
                
                chatBubble.classList.remove("hidden");
                chatBubble.classList.remove("opacity-0", "translate-y-8");
            }, 300); // Đợi transition hoàn thành
        }

        // Gán sự kiện
        chatBubble.addEventListener("click", openChatWindow);
        minimizeBtn.addEventListener("click", closeChatWindow);

        // === Logic Gửi Tin Nhắn ===

        /**
         * Thêm tin nhắn vào cửa sổ chat
         * @param {string | null} text - Nội dung tin nhắn. Nếu là null, sẽ hiển thị "đang gõ".
         * @param {'user' | 'bot' | 'loading'} sender - Người gửi
         */
        function addMessage(text, sender) {
            const messageWrapper = document.createElement("div");
            const messageDiv = document.createElement("div");

            // Thêm class Tailwind dựa trên người gửi
            if (sender === 'user') {
                messageWrapper.classList.add("flex", "justify-end"); // Căn phải
                messageDiv.classList.add("bg-blue-600", "text-white", "p-3", "rounded-lg", "max-w-xs", "shadow-sm");
                messageDiv.textContent = text;
            } else if (sender === 'bot') {
                messageWrapper.classList.add("flex", "justify-start"); // Căn trái
                messageDiv.classList.add("bg-gray-100", "text-gray-800", "p-3", "rounded-lg", "max-w-xs", "shadow-sm");
                // Chuyển đổi ký tự xuống dòng (\n) thành thẻ <br> để hiển thị
                messageDiv.innerHTML = text.replace(/\n/g, '<br>');
            } else if (sender === 'loading') {
                messageWrapper.classList.add("flex", "justify-start", "loading-indicator-wrapper"); // Căn trái
                messageDiv.classList.add("bg-gray-100", "text-gray-800", "p-3", "rounded-lg", "max-w-xs", "shadow-sm");
                messageDiv.innerHTML = `
                    <div class="typing-indicator">
                        <span></span><span></span><span></span>
                    </div>
                `;
            }

            messageWrapper.appendChild(messageDiv);
            chatBody.appendChild(messageWrapper);
            
            // Tự động cuộn xuống tin nhắn mới nhất
            chatBody.scrollTop = chatBody.scrollHeight;
            return messageWrapper; // Trả về để có thể xóa (nếu là loading)
        }

        /**
         * Xóa chỉ báo "đang gõ"
         */
        function removeLoadingIndicator() {
            const loadingIndicator = chatBody.querySelector(".loading-indicator-wrapper");
            if (loadingIndicator) {
                chatBody.removeChild(loadingIndicator);
            }
        }

        /**
         * Gửi tin nhắn đến server và nhận phản hồi
         */
        async function sendMessageToServer() {
            const messageText = input.value.trim();
            if (messageText === "") return;

            // 1. Hiển thị tin nhắn của người dùng
            addMessage(messageText, "user");
            input.value = ""; // Xóa input

            // 2. Hiển thị chỉ báo "đang gõ"
            addMessage(null, "loading");

            try {
                // 3. Gửi yêu cầu đến API
                const response = await fetch("/api/chat", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({ message: messageText }),
                });

                // 4. Xóa chỉ báo "đang gõ"
                removeLoadingIndicator();

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.error || `Lỗi server: ${response.statusText}`);
                }

                const data = await response.json();
                
                // 5. Hiển thị câu trả lời của AI
                addMessage(data.response, "bot");

            } catch (error) {
                console.error("Lỗi khi chat:", error);
                // 6. Xóa "đang gõ" và hiển thị lỗi
                removeLoadingIndicator();
                addMessage(`Xin lỗi, đã xảy ra lỗi: ${error.message}`, "bot");
            }
        }

        // Gán sự kiện gửi
        sendBtn.addEventListener("click", sendMessageToServer);
        input.addEventListener("keypress", function(e) {
            if (e.key === "Enter") {
                e.preventDefault(); // Ngăn xuống dòng
                sendMessageToServer();
            }
        });
    } // Kết thúc khối if (kiểm tra chatbot UI)

    // --- PHẦN 3: LOGIC CHUYỂN PANEL (MỚI THÊM) ---
    console.log("Khởi tạo logic chuyển panel...");
    
    // Khai báo biến cho panel
    const navLinks = {
        'nav-tong-quan': document.getElementById('nav-tong-quan'),
        'nav-fanpage-overview': document.getElementById('nav-fanpage-overview')
        // Thêm các nav link khác ở đây nếu cần
    };

    const panels = {
        'panel-tong-quan': document.getElementById('panel-tong-quan'),
        'panel-fanpage-overview': document.getElementById('panel-fanpage-overview')
        // Thêm các panel khác ở đây nếu cần
    };

    const allNavLinks = document.querySelectorAll('.nav-link');

    // Hàm để set link active
    const setActiveLink = (clickedLink) => {
        if (!clickedLink) return;
        
        allNavLinks.forEach(link => {
            link.classList.remove('bg-gray-200', 'text-gray-700', 'font-medium');
            link.classList.add('text-gray-600', 'hover:bg-gray-100');
        });

        clickedLink.classList.add('bg-gray-200', 'text-gray-700', 'font-medium');
        clickedLink.classList.remove('text-gray-600', 'hover:bg-gray-100');
    };

    // Hàm để hiển thị panel
    const showPanel = (panelId) => {
        Object.values(panels).forEach(panel => {
            if (panel) panel.classList.add('hidden');
        });
        if (panels[panelId]) {
            panels[panelId].classList.remove('hidden');
        }
    };

    // Gán sự kiện click cho "Tổng quan"
    if (navLinks['nav-tong-quan']) {
        navLinks['nav-tong-quan'].addEventListener('click', (e) => {
            e.preventDefault();
            setActiveLink(navLinks['nav-tong-quan']);
            showPanel('panel-tong-quan');
        });
    }

    // Gán sự kiện click cho "Fanpage overview"
    if (navLinks['nav-fanpage-overview']) {
        navLinks['nav-fanpage-overview'].addEventListener('click', (e) => {
            e.preventDefault();
            setActiveLink(navLinks['nav-fanpage-overview']);
            showPanel('panel-fanpage-overview');
        });
    }

    // Đặt trạng thái ban đầu khi tải trang (hiển thị panel 'Tổng quan')
    showPanel('panel-tong-quan');


}); // <-- **KẾT THÚC** document.addEventListener('DOMContentLoaded')

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
            elements: { line: { fill: true, tension: 0.4 } },
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

    // [MỚI] Khởi tạo biểu đồ cho Fanpage Overview (nếu element tồn tại)
    const ctxFpMain = document.getElementById('fp-main-chart');
    if (ctxFpMain) {
        // Bạn có thể tạo một biến instance mới cho biểu đồ này nếu cần
        // let fpMainChartInstance = new Chart(ctxFpMain.getContext('2d'), { ... });
        // Tạm thời chỉ log
        console.log("Đã tìm thấy canvas 'fp-main-chart' và sẵn sàng khởi tạo.");
    }
}

function initializeSelects() {
    // (Giữ nguyên)
    // 1. Dropdown Tài khoản
    elAccount = document.getElementById('filter-ad-account');

    // 2. Dropdown Thời gian
    elTime = document.getElementById('filter-time');

    // 3. Multi-selects
    elCampaigns = document.getElementById('filter-campaigns');
    elAdsets = document.getElementById('filter-adsets');
    elAds = document.getElementById('filter-ads');

    // Lấy DOM elements cho 2 dropdown của biểu đồ tròn
    elChartMetric = document.getElementById('chart-metric');
    elChartDimension = document.getElementById('chart-dimension');

    // Set giá trị mặc định (bạn có thể đổi ở đây)
    // [EDIT] Kiểm tra null trước khi gán
    if (elChartMetric) elChartMetric.value = 'purchases';
    if (elChartDimension) elChartDimension.value = 'placement';


    // Vô hiệu hóa tất cả dropdown con ban đầu
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
    
    // Vô hiệu hóa cả dropdown của biểu đồ
    if (elChartMetric) elChartMetric.disabled = true;
    if (elChartDimension) elChartDimension.disabled = true;
}

// --- GẮN CÁC BỘ LẮNG NGHE SỰ KIỆN ---

function setupEventListeners() {
    // (Giữ nguyên)
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

    // Thêm sự kiện cho 2 dropdown của biểu đồ tròn
    // [EDIT] Kiểm tra null
    if (elChartMetric) elChartMetric.addEventListener('change', handlePieChartUpdate);
    if (elChartDimension) elChartDimension.addEventListener('change', handlePieChartUpdate);
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
            // 1. Overview
            fetch('/api/overview_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
            }),
            // 2. Line Chart
            fetch('/api/chart_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
            }),
            // 3. Pie Chart
            fetch('/api/breakdown_chart', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(pieChartFilters) 
            }),
            // 4. Table Data
            fetch('/api/table_data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(filters) 
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
        if (!pieChartRes.ok) { 
            const err = await pieChartRes.json();
            throw new Error(`Lỗi pie chart: ${err.error}`);
        }
        if (!tableRes.ok) { 
            const err = await tableRes.json();
            throw new Error(`Lỗi table data: ${err.error}`);
        }

        // Lấy JSON
        const overviewData = await overviewRes.json();
        const chartData = await chartRes.json();
        const pieChartData = await pieChartRes.json(); 
        const tableData = await tableRes.json(); 

        // Render dữ liệu
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

/**
 * Hàm này CHỈ cập nhật biểu đồ tròn
 */
async function handlePieChartUpdate() {
    // (Giữ nguyên)
    console.log("Đang cập nhật biểu đồ tròn...");
    
    const payload = getPieChartPayload();
    if (!payload) {
        return; 
    }
            
    try {
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


// --- CÁC HÀM TẢI DROPDOWN ---

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
        
        // Mở khóa 2 dropdown của biểu đồ tròn
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

    /**
     * Hàm helper để tạo HTML cho chỉ số tăng trưởng/thay đổi
     */
    const renderGrowthHtml = (value, type) => {
        if (value === null || typeof value === 'undefined' || value === 0) {
            return `<span>(n/a)</span>`;
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

        return `<span class="${colorClass} flex items-center">
                    <i data-feather="${iconName}" class="w-4 h-4 mr-1"></i>
                    <span>${formattedValue}</span>
                </span>`;
    };

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

/**
 * Hàm render biểu đồ tròn (pie chart)
 */
function renderPieChartData(pieChartData, title = 'Breakdown') {
    // (Giữ nguyên)
    if (platformChartInstance) {
        if (pieChartData && pieChartData.labels && pieChartData.labels.length > 0) {
            platformChartInstance.data.labels = pieChartData.labels;
            platformChartInstance.data.datasets = pieChartData.datasets;
            platformChartInstance.options.plugins.title.text = title; 
        } else {
            platformChartInstance.data.labels = ['Không có dữ liệu'];
            platformChartInstance.data.datasets = [{ 
                label: 'Phân bổ', 
                data: [1], 
                backgroundColor: ['#E5E7EB'], 
                hoverOffset: 4 
            }];
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
 * Hàm helper để lấy payload đầy đủ cho biểu đồ tròn
 */
function getPieChartPayload() {
    // (Giữ nguyên)
    const baseFilters = getFilterPayload();
    if (!baseFilters) {
        return null; 
    }
    
    const metric = elChartMetric.value;
    const dimension = elChartDimension.value;
    
    if (!metric || !dimension) {
        showNotification('Vui lòng chọn Chỉ số và Chiều cho biểu đồ tròn.', 'error');
        return null;
    }

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

/**
 * Hàm helper để tạo badge cho trạng thái chiến dịch
 */
function getStatusBadge(status) {
    // (Giữ nguyên)
    status = status ? status.toUpperCase() : 'UNKNOWN';
    
    switch (status) {
        case 'ACTIVE':
            return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-green-100 text-green-800">Hoạt động</span>`;
        case 'PAUSED':
            return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-yellow-100 text-yellow-800">Tạm dừng</span>`;
        case 'ARCHIVED':
            return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-red-100 text-red-800">Đã lưu trữ</span>`;
        case 'DELETED':
            return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-red-100 text-red-800">Đã xóa</span>`;
        default:
            return `<span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full bg-gray-100 text-gray-800">${status}</span>`;
    }
}

/**
 * Hàm render dữ liệu cho bảng hiệu suất chiến dịch
 */
function renderTableData(data, errorMsg = null) {
    // (Giữ nguyên)
    const tableBody = document.getElementById('campaign-table-body');
    if (!tableBody) return;

    // Các hàm định dạng (local scope)
    const formatCurrency = (num) => new Intl.NumberFormat('vi-VN', { style: 'currency', currency: 'VND' }).format(Math.round(num || 0));
    const formatNumber = (num) => new Intl.NumberFormat('vi-VN').format(Math.round(num || 0));

    // Trường hợp 1: Có lỗi
    if (errorMsg) {
        tableBody.innerHTML = `<tr><td colspan="6" class="py-4 px-4 text-center text-red-500">${errorMsg}</td></tr>`;
        return;
    }
    
    // Trường hợp 2: Không có dữ liệu
    if (!data || data.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="6" class="py-4 px-4 text-center text-gray-500">Không tìm thấy dữ liệu chiến dịch phù hợp.</td></tr>`;
        return;
    }

    // Trường hợp 3: Render dữ liệu
    let html = '';
    data.forEach(row => {
        html += `<tr class="border-b hover:bg-gray-50">`;
        // Tên chiến dịch
        html += `<td class="py-3 px-4 font-medium text-gray-900">${row.campaign_name}</td>`;
        // Trạng thái (dùng helper)
        html += `<td class="py-3 px-4">${getStatusBadge(row.status)}</td>`;
        // Chi tiêu (căn phải)
        html += `<td class="py-3 px-4 text-right">${formatCurrency(row.spend)}</td>`;
        // Hiển thị (căn phải)
        html += `<td class="py-3 px-4 text-right">${formatNumber(row.impressions)}</td>`;
        // Chuyển đổi (căn phải)
        html += `<td class="py-3 px-4 text-right">${formatNumber(row.purchases)}</td>`;
        // CPA (căn phải)
        html += `<td class="py-3 px-4 text-right">${formatCurrency(row.cpa)}</td>`;
        html += `</tr>`;
    });

    tableBody.innerHTML = html;
}