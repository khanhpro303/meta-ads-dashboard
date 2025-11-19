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
        Nó hoạt động như một Generator.
        """
        for step in self.agent.stream(
            {"messages": [{"role": "user", "content": query}]},
            stream_mode="values", # Đảm bảo đang streaming giá trị (value)
        ):
            
            # [QUAN TRỌNG]: Trong trường hợp của LangChain Agent, sẽ nhận được
            # cả Tool Calls và Final Answer. Lọc và chỉ yield phần text
            # của Final Answer.
            
            # Chỉ yield phần text có sẵn:
            if step["messages"][-1].content:
                # Lọc để chỉ lấy nội dung văn bản cuối cùng (nếu có)
                text_content = step["messages"][-1].content[0].get("text")
                if text_content:
                    yield text_content
                    
            # [CÁCH CHÍNH XÁC] Yêu cầu agent.stream trả về từng token (nếu LLM stream token)
            # hoặc sử dụng agent.astream_events() để kiểm soát chi tiết hơn.
            # Nhưng đối với hiện tại, chúng ta tạm giữ cách này và chuyển sang Flask.

def main():
    try:
        ai = AIAgent()
        response = ai.ask("Chi tiêu hôm qua là bao nhiêu?")
        print(response)
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")

if __name__ == "__main__":
    main()