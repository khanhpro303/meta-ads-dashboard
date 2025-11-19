from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
import os
import logging
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

class AIAgent:
    def __init__(self):
        # Initialize model
        self.model = init_chat_model("google_genai:gemini-2.5-pro")

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
        self.tools = toolkit.get_tools()
        for tool in self.tools:
            logger.info(f"{tool.name}: {tool.description}\n")
        
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

        Để bắt đầu, bạn LUÔN nên nhìn vào các bảng trong cơ sở dữ liệu để xem bạn
        có thể truy vấn những gì. KHÔNG bỏ qua bước này.

        Sau đó, bạn nên truy vấn schema của các bảng phù hợp nhất.

        Bạn PHẢI luôn trả lời bằng tiếng Việt. Văn phong chuyên nghiệp, đi vào trọng tâm. Đừng quên điều này.
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
        response = ai.ask("Chi tiêu hôm qua là bao nhiêu?")
        print(response)
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")

if __name__ == "__main__":
    main()