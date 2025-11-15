import os
import logging
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit

# Import DatabaseManager để lấy 'engine' kết nối
from database_manager import DatabaseManager

# Tải biến môi trường (quan trọng để lấy GOOGLE_API_KEY)
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

class AIAgent:
    def __init__(self):
        logger.info("Khởi tạo AI Agent...")
        self.db_manager = DatabaseManager()
        self.engine = self.db_manager.engine
        
        # Kiểm tra API key
        if not os.getenv("GOOGLE_API_KEY"):
            logger.error("GOOGLE_API_KEY chưa được thiết lập!")
            raise EnvironmentError("GOOGLE_API_KEY chưa được thiết lập.")

        # Khởi tạo LLM (Gemini)
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-pro", 
            temperature=0, # =0 để AI trả lời dứt khoát, không sáng tạo
        )

        # Kết nối LangChain với Database của bạn
        # Đây là bước quan trọng:
        # Chúng ta chỉ định rõ các bảng AI được phép truy cập.
        self.db = SQLDatabase(
            engine=self.engine,
            # Liệt kê các bảng AI cần biết
            include_tables=[
                'dim_ad_account',
                'dim_campaign',
                'dim_adset',
                'dim_ad',
                'dim_date',
                'dim_platform',
                'dim_placement',
                'fact_performance_platform',
                'fact_performance_demographic'
            ]
        )
        
        # Cung cấp "meta-data" (mô tả) cho AI
        custom_table_info = {
            "fact_performance_platform": """
                Đây là Bảng Fact (bảng sự kiện) chính.
                - Chứa các chỉ số hiệu suất ('spend', 'impressions', 'clicks', 'purchases', 'messages_started', 'reach', 'post_engagement', 'link_click')
                - Đặc điểm: Dữ liệu trong bảng này được chia nhỏ (breakdown) theo 'platform_id' (nền tảng) và 'placement_id' (vị trí hiển thị).
                - CẢNH BÁO: Các cột như 'ctr', 'cpm', 'frequency' là các giá trị đã tính ở cấp độ hàng. Để có kết quả chính xác, khi người dùng hỏi về các chỉ số này, bạn nên TÍNH TOÁN LẠI (ví dụ: SUM(clicks) / SUM(impressions) * 100 as CTR) thay vì lấy trung bình (AVG) của các cột đó.
                - Để lấy tên (name) của chiến dịch, nhóm quảng cáo, hoặc quảng cáo, bạn PHẢI JOIN bảng này với 'dim_campaign', 'dim_adset', 'dim_ad'.
                - Để lấy tên nền tảng hoặc vị trí, bạn PHẢI JOIN với 'dim_platform' và 'dim_placement'.
                - Để lọc theo ngày tháng, bạn PHẢI JOIN với 'dim_date' qua 'date_key'.
            """,

            "fact_performance_demographic": """
                Đây là Bảng Fact (bảng sự kiện) quan trọng thứ hai.
                - Chứa các chỉ số hiệu suất ('spend', 'impressions', 'clicks', 'purchases', 'messages_started', 'reach', 'post_engagement', 'link_click')
                - Đặc điểm: Dữ liệu trong bảng này được chia nhỏ (breakdown) theo 'gender' (giới tính) và 'age' (độ tuổi).
                - CẢNH BÁO: Các cột như 'ctr', 'cpm', 'frequency' là các giá trị đã tính ở cấp độ hàng. Để có kết quả chính xác, khi người dùng hỏi về các chỉ số này, bạn nên TÍNH TOÁN LẠI (ví dụ: SUM(clicks) / SUM(impressions) * 100 as CTR) thay vì lấy trung bình (AVG) của các cột đó.
                - Để lấy tên (name) của chiến dịch, nhóm quảng cáo, hoặc quảng cáo, bạn PHẢI JOIN bảng này với 'dim_campaign', 'dim_adset', 'dim_ad'.
                - Để lọc theo ngày tháng, bạn PHẢI JOIN với 'dim_date' qua 'date_key'.
            """,

            "dim_date": """
                Bảng Dim (chiều) về Thời gian. Bảng này LÀ BẮT BUỘC để lọc dữ liệu theo khoảng thời gian.
                - Dùng 'date_key' để JOIN với các bảng Fact (ví dụ: fact_performance_platform.date_key = dim_date.date_key).
                - Dùng 'full_date' (kiểu date) để lọc theo một khoảng ngày cụ thể (ví dụ: BETWEEN '2025-10-01' AND '2025-10-31').
                - Dùng 'day', 'month', 'year', 'quarter' để lọc theo các khoảng thời gian cụ thể (ví dụ: WHERE month = 10 AND year = 2025).
            """,
            
            "dim_ad_account": """
                Bảng Dim (chiều) về Tài khoản Quảng cáo. 
                Chứa 'name' (tên tài khoản). 
                Sử dụng 'ad_account_id' để JOIN với 'dim_campaign' hoặc các bảng Dim khác.
            """,

            "dim_campaign": """
                Bảng Dim (chiều) về Chiến dịch. 
                Chứa 'name' (tên chiến dịch) và 'status' (trạng thái). 
                Sử dụng 'campaign_id' để JOIN với 'fact_performance_platform', 'fact_performance_demographic' hoặc 'dim_adset'.
            """,

            "dim_adset": """
                Bảng Dim (chiều) về Nhóm Quảng cáo (Ad Set). 
                Chứa 'name' (tên nhóm quảng cáo) và 'status' (trạng thái). 
                Sử dụng 'adset_id' để JOIN với 'fact_performance_platform', 'fact_performance_demographic' hoặc 'dim_ad'.
            """,
            
            "dim_ad": """
                Bảng Dim (chiều) về Quảng cáo (Ad). 
                Đây là cấp độ chi tiết nhất. Chứa 'name' (tên quảng cáo) và 'status' (trạng thái). 
                Sử dụng 'ad_id' để JOIN với các bảng Fact (fact_performance_platform, fact_performance_demographic).
            """,

            "dim_platform": """
                Bảng Dim (chiều) về Nền tảng hiển thị.
                Chứa 'platform_name' (ví dụ: 'facebook', 'instagram', 'messenger').
                Sử dụng 'platform_id' để JOIN với 'fact_performance_platform'.
            """,
            
            "dim_placement": """
                Bảng Dim (chiều) về Vị trí hiển thị.
                Chứa 'placement_name' (ví dụ: 'feed', 'stories', 'reels').
                Sử dụng 'placement_id' để JOIN với 'fact_performance_platform'.
            """
        }

        # Tạo một bộ công cụ (toolkit) cho AI
        self.toolkit = SQLDatabaseToolkit(
            db=self.db, 
            llm=self.llm,
            custom_table_info=custom_table_info
        )

        # Tạo Agent
        # Agent này là sự kết hợp của LLM (bộ não) và Toolkit (công cụ truy vấn DB)
        self.agent_executor = create_sql_agent(
            llm=self.llm,
            toolkit=self.toolkit,
            verbose=True, # =True để xem log AI đang "suy nghĩ" gì
            agent_type="openai-functions", # Hoạt động tốt với cả Gemini
            prefix="Bạn là một chuyên gia phân tích dữ liệu marketing cho Meta Ads. Nhiệm vụ của bạn là trả lời các câu hỏi của người dùng bằng cách truy vấn cơ sở dữ liệu.",
            suffix="""
            QUAN TRỌNG: 
            1. Khi tính toán các chỉ số như CTR, CPM, CPA, hãy tính toán thủ công (ví dụ: SUM(clicks) / SUM(impressions)) thay vì lấy giá trị trung bình từ cột.
            2. Luôn ưu tiên JOIN với 'dim_date' để lọc thời gian khi được yêu cầu.
            3. Hãy trả lời bằng tiếng Việt.
            """
        )
        logger.info("AI Agent đã sẵn sàng.")

    def ask(self, query: str) -> str:
        """
Nơi nhận câu hỏi từ người dùng và trả về câu trả lời của AI.
"""
        logger.info(f"Nhận câu hỏi mới: {query}")
        try:
            # Thực thi câu hỏi qua agent
            result = self.agent_executor.invoke(query)
            return result.get('output', 'Tôi không thể tìm thấy câu trả lời.')
        except Exception as e:
            logger.error(f"Lỗi khi AI Agent xử lý câu hỏi: {e}", exc_info=True)
            return "Xin lỗi, tôi gặp lỗi khi đang xử lý câu hỏi của bạn."