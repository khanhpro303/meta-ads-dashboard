from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.rate_limiters import InMemoryRateLimiter
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import HumanMessage
from langchain.tools import tool
from functools import lru_cache
from dotenv import load_dotenv
import os
import logging
import datetime
import requests
import base64
from functools import lru_cache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

class AIAgent:
    def __init__(self):
        # Giới hạn 1 request/6 giây (10 request/phút)
        rate_limiter = InMemoryRateLimiter(
            requests_per_second=0.8,
            check_every_n_seconds=0.1,
            max_bucket_size=10,
        )
        # Initialize model
        self.model = init_chat_model(
            "google_genai:gemini-2.5-flash",
            rate_limiter=rate_limiter,
            temperature=0.3
            )

        # Connect to Postgres
        self.db_url = os.getenv('DATABASE_URL')
        if not self.db_url:
            logger.error("DATABASE_URL không được cấu hình trong biến môi trường.")
            return
        
        if self.db_url.startswith("postgres://"):
            self.db_url = self.db_url.replace("postgres://", "postgresql://", 1)

        self.db = SQLDatabase.from_uri(self.db_url)
        logger.info(f"Dialect: {self.db.dialect}")
        logger.info(f"Available tables: {self.db.get_usable_table_names()}")
        logger.info(f'Sample output: {self.db.run("SELECT * FROM dim_ad_account LIMIT 2;")}')

        # Create toolkit with tools
        toolkit = SQLDatabaseToolkit(db=self.db, llm=self.model)
        tools_db = toolkit.get_tools()

        @lru_cache(maxsize=50) # Cache 50 request gần nhất
        def _fetch_image_base64(url: str):
            response = requests.get(url, stream=True, timeout=10)
            if response.status_code != 200:
                return None
            return base64.b64encode(response.content).decode("utf-8")

        @tool
        def analyze_image_from_url(image_url: str, question: str):
            """
            Công cụ lấy dữ liệu ảnh từ URL để AI phân tích. 
            Luôn dùng công cụ này khi người dùng hỏi về nội dung của một bức ảnh (url).
            """
            try:
                # Gọi hàm đã được cache
                image_data = _fetch_image_base64(image_url)
                if not image_data:
                    return f"Lỗi: Không thể tải ảnh từ {image_url}"
                
                # Trả về cấu trúc content block chuẩn của Gemini/LangChain
                # Để model hiểu đây là input đa phương thức
                return {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{image_data}"},
                    "text_context": question # Gợi ý context
                }
            except Exception as e:
                return f"Lỗi khi xử lý ảnh: {str(e)}"
        
        self.tools = tools_db + [analyze_image_from_url]

        for t in self.tools:
            logger.info(f"{t.name}: {t.description}\n")
        
        # Create system prompt
        system_prompt = """
        Bối cảnh chung: Hiện tại đang là năm {year}, bạn là một chuyên gia phân tích dữ liệu của ngành phân phối mũ bảo hiểm và đồ bảo hộ tại Việt Nam.
        Bạn được thiết kế để tương tác với cơ sở dữ liệu Postgres SQL.
        Bạn sẽ nhận một câu hỏi bằng tiếng Việt, hãy tạo một truy vấn {dialect} đúng cú pháp để truy vấn được,
        sau đó xem kết quả truy vấn và trả lời. Trừ khi người dùng 
        chỉ định một số lượng ví dụ cụ thể mà họ muốn lấy, luôn giới hạn truy vấn tối đa {top_k} kết quả.

        Bạn có thể sắp xếp kết quả theo cột phù hợp để trả về kết quả thú vị nhất
        trong cơ sở dữ liệu. Không bao giờ truy vấn tất cả các cột từ một bảng cụ thể,
        chỉ yêu cầu các cột có liên quan cho câu hỏi.

        Bạn PHẢI kiểm tra lại truy vấn của mình trước khi thực hiện nó. Nếu bạn gặp bất kỳ lỗi trong khi
        thực hiện một truy vấn, viết lại truy vấn và thử lại.

        KHÔNG thực hiện bất kỳ câu lệnh DML nào (CHÈN, CẬP NHẬT, XÓA, THẢ, v.v.) đối với cơ sở dữ liệu.

        QUY TRÌNH LÀM VIỆC:
        1. Nếu cần dữ liệu số: Kiểm tra bảng -> Query schema -> Query dữ liệu.
        2. Nếu cần xem ảnh: 
           - Query lấy URL ảnh từ DB.
           - Dùng tool `analyze_image_from_url` với URL đó. Tool này sẽ trả về dữ liệu ảnh.
           - Bạn tự sử dụng khả năng vision của mình để trả lời câu hỏi dựa trên dữ liệu ảnh nhận được.

        Bạn PHẢI luôn trả lời bằng tiếng Việt. Văn phong chuyên nghiệp, đi vào trọng tâm.
        Bạn không dùng markdown khi gửi trả lời để tránh hiển thị ***. Đừng quên điều này.
        """.format(
            dialect=self.db.dialect,
            top_k=2,
            year=datetime.date.today().year
        )

        # Create agent
        self.agent = create_agent(
            self.model,
            self.tools,
            system_prompt=system_prompt
        )
    
    def ask(self, query: str):
        """
        Hàm nhận câu hỏi và trả về từng phần (chunk) của câu trả lời.
        Nó hoạt động như một Generator, chỉ yield phần text có sẵn.
        """
        for step in self.agent.stream(
            {"messages": [{"role": "user", "content": query}]},
            stream_mode="values",
        ):
            # Lấy tin nhắn mới nhất trong bước
            last_message = step["messages"][-1]
            
            # --- LOGIC KIỂM TRA VÀ TRUY CẬP AN TOÀN ---
            
            # 1. Kiểm tra thuộc tính 'content' có tồn tại không
            if not hasattr(last_message, 'content'):
                continue
                
            content = last_message.content
            
            # 2. Kiểm tra nếu 'content' là một LIST (cấu trúc phức tạp, ví dụ: Final Answer)
            if isinstance(content, list) and content and content[0].get("text"):
                # Lấy nội dung văn bản từ MessagePart đầu tiên
                text_content = content[0].get("text")
                yield text_content
                
            # 3. Kiểm tra nếu 'content' là một STRING (cấu trúc đơn giản, ví dụ: Tool Call)
            # Bỏ qua các bước là Tool Call hoặc Tool Message
            # Vì chúng ta chỉ muốn hiển thị câu trả lời cuối cùng của AI.
            elif isinstance(content, str):
                # Bạn có thể chọn yield nội dung string nếu nó là tin nhắn quan trọng
                # NHƯNG: Đối với Chatbot, chúng ta chỉ muốn hiển thị FINAL ANSWER, 
                # nên tốt nhất là bỏ qua các bước trung gian (như SQL Query, Tool Output).
                pass

def main():
    try:
        ai = AIAgent()
        response = ai.ask("Bài post nào có nhiều like nhất và ảnh đó nói về cái gì?")
        print("".join(response))
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")

if __name__ == "__main__":
    main()