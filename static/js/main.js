// === LOGIC MỚI CHO BỘ LỌC NGÀY ===
const timeFilter = document.getElementById('filter-time');
const customDateRange = document.getElementById('custom-date-range');

// Kiểm tra ngay khi tải trang (phòng trường hợp trình duyệt nhớ lựa chọn 'custom')
if (timeFilter.value === 'custom') {
    customDateRange.classList.remove('hidden');
}

// Thêm event listener
timeFilter.addEventListener('change', function() {
    if (this.value === 'custom') {
        customDateRange.classList.remove('hidden');
    } else {
        customDateRange.classList.add('hidden');
    }
});
// === KẾT THÚC LOGIC MỚI ===

// Kích hoạt Feather Icons
feather.replace();

// --- Dữ liệu giả (Mock Data) cho biểu đồ ---

// Dữ liệu Biểu đồ đường (Chi tiêu)
const spendTrendData = {
    labels: ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T10'],
    datasets: [
        {
            label: 'Chi tiêu (Triệu đ)',
            data: [12, 19, 15, 22, 18, 25, 20, 30, 28, 35],
            borderColor: 'rgb(59, 130, 246)', // blue-500
            backgroundColor: 'rgba(59, 130, 246, 0.1)',
            fill: true,
            tension: 0.4,
            yAxisID: 'y',
        },
        {
            label: 'Hiển thị (Triệu)',
            data: [1.5, 2.2, 1.8, 2.8, 2.0, 3.0, 2.5, 3.8, 3.5, 4.5],
            borderColor: 'rgb(16, 185, 129)', // emerald-500
            backgroundColor: 'rgba(16, 185, 129, 0.1)',
            fill: true,
            tension: 0.4,
            yAxisID: 'y1',
        }
    ]
};

// Dữ liệu Biểu đồ tròn (Nền tảng) - (Giữ nguyên)
const platformData = {
    labels: ['Facebook Feed', 'Instagram Feed', 'Instagram Stories', 'Messenger'],
    datasets: [{
        label: 'Phân bổ chi tiêu',
        data: [45, 25, 20, 10],
        backgroundColor: [
            'rgb(59, 130, 246)',  // blue-500
            'rgb(236, 72, 153)',  // pink-500
            'rgb(139, 92, 246)',  // violet-500
            'rgb(22, 163, 74)'   // green-600
        ],
        hoverOffset: 4
    }]
};

// --- Khởi tạo Chart.js ---

// Khởi tạo Biểu đồ đường
const ctxSpend = document.getElementById('spendTrendChart').getContext('2d');
new Chart(ctxSpend, {
    type: 'line',
    data: spendTrendData,
    options: {
        responsive: true,
        interaction: {
            mode: 'index',
            intersect: false,
        },
        scales: {
            y: {
                type: 'linear',
                display: true,
                position: 'left',
                title: {
                    display: true,
                    text: 'Chi tiêu (Triệu đ)'
                }
            },
            y1: {
                type: 'linear',
                display: true,
                position: 'right',
                title: {
                    display: true,
                    text: 'Hiển thị (Triệu)'
                },
                grid: {
                    drawOnChartArea: false, // Chỉ hiển thị lưới cho trục y
                },
            },
        }
    }
});

// Khởi tạo Biểu đồ tròn (Giữ nguyên)
const ctxPlatform = document.getElementById('platformChart').getContext('2d');
new Chart(ctxPlatform, {
    type: 'doughnut',
    data: platformData,
    options: {
        responsive: true,
        plugins: {
            legend: {
                position: 'bottom',
            },
        }
    }
});