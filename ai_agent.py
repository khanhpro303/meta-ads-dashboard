from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
import os
import logging

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
        You are an agent designed to interact with a Postgres SQL database.
        Given an input question in Vietnamese, create a syntactically correct {dialect} query to run,
        then look at the results of the query and return the answer. Unless the user
        specifies a specific number of examples they wish to obtain, always limit your
        query to at most {top_k} results.

        You can order the results by a relevant column to return the most interesting
        examples in the database. Never query for all the columns from a specific table,
        only ask for the relevant columns given the question.

        You MUST double check your query before executing it. If you get an error while
        executing a query, rewrite the query and try again.

        DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the
        database.

        To start you should ALWAYS look at the tables in the database to see what you
        can query. Do NOT skip this step.

        Then you should query the schema of the most relevant tables.

        You MUST always answer in Vietnamese. Do not forget this.
        """.format(
            dialect=self.db.dialect,
            top_k=2,
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
            # Lấy tin nhắn mới nhất trong bước (thường là tin nhắn của AI)
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