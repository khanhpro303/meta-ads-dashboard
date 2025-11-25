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
import time

import hashlib
import json
from langchain.agents.middleware import AgentMiddleware
from langchain.tools.tool_node import ToolCallRequest
from langchain.messages import ToolMessage
from langgraph.types import Command

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

class DynamicCacheMiddleware(AgentMiddleware):
    """Cache động cho bất kỳ tool nào"""
    
    def __init__(self, ttl_seconds: int = 300):
        self.cache = {}
        self.ttl_seconds = ttl_seconds
        self.cache_hits = 0
        self.cache_misses = 0
    
    def _get_cache_key(self, tool_name: str, tool_args: dict) -> str:
        """Tạo key cache từ tool name + arguments"""
        args_str = json.dumps(tool_args, sort_keys=True)
        args_hash = hashlib.sha256(args_str.encode()).hexdigest()[:8]
        return f"{tool_name}:{args_hash}"
    
    def _is_cache_valid(self, cache_entry: dict) -> bool:
        """Kiểm tra cache còn hạn không"""
        current_time = time.time()
        return (current_time - cache_entry["timestamp"]) < self.ttl_seconds
    
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler,
    ) -> ToolMessage | Command:
        """Intercept mỗi tool call"""
        tool_name = request.tool_call["name"]
        tool_args = request.tool_call["args"]
        
        # Bỏ qua cache cho image analysis (thay đổi)
        no_cache_tools = ["analyze_image_from_url"]
        if tool_name in no_cache_tools:
            return handler(request)
        
        # Tạo cache key
        cache_key = self._get_cache_key(tool_name, tool_args)
        
        # Kiểm tra cache
        if cache_key in self.cache:
            cache_entry = self.cache[cache_key]
            if self._is_cache_valid(cache_entry):
                # ✅ HIT - Trả kết quả từ cache
                self.cache_hits += 1
                logger.info(f"🟢 CACHE HIT: {tool_name} (Hits: {self.cache_hits})")
                
                return ToolMessage(
                    content=cache_entry["result"],
                    tool_call_id=request.tool_call["id"],
                    name=tool_name,
                )
            else:
                del self.cache[cache_key]
        
        # ❌ MISS - Chạy tool thực tế
        self.cache_misses += 1
        logger.info(f"🔴 CACHE MISS: {tool_name} (Misses: {self.cache_misses})")
        
        result = handler(request)
        
        # Lưu cache
        if isinstance(result, ToolMessage):
            self.cache[cache_key] = {
                "result": result.content,
                "timestamp": time.time(),
                "tool_name": tool_name,
            }
        
        return result
    
    def get_cache_stats(self) -> dict:
        """Trả về stats cache"""
        total = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total * 100) if total > 0 else 0
        return {
            "total_requests": total,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "hit_rate": f"{hit_rate:.1f}%",
            "cached_items": len(self.cache),
        }

class AIAgent:
    def __init__(self):
        # Giới hạn 1 request/6 giây (10 request/phút)
        rate_limiter = InMemoryRateLimiter(
            requests_per_second=0.167,  # ~10 requests/phút
            check_every_n_seconds=0.1,
            max_bucket_size=2,
        )
        # Initialize model
        self.model = init_chat_model(
            "google_genai:gemini-2.5-flash",
            rate_limiter=rate_limiter
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

        # ĐỊNH NGHĨA VISION TOOL (CÔNG CỤ NHÌN ẢNH)
        @tool
        def analyze_image_from_url(image_url: str, question: str):
            """
            Phân tích ảnh mà KHÔNG gọi LLM thêm.
            Chỉ tải ảnh, không xử lý AI.
            """
            try:
                response = requests.get(image_url, stream=True, timeout=10)
                if response.status_code != 200:
                    return f"Lỗi: Không thể tải ảnh từ {image_url}"
                
                image_data = base64.b64encode(response.content).decode("utf-8")
                
                # ✅ TRẢ VỀ CẤU TRÚC CHO AGENT LÀM VIỆC
                return {
                    "image_url": f"data:image/jpeg;base64,{image_data}",
                    "question": question,
                    "status": "ready_for_analysis"
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

        Để bắt đầu, bạn LUÔN nên nhìn vào các bảng trong cơ sở dữ liệu để xem bạn
        có thể truy vấn những gì. KHÔNG bỏ qua bước này.

        Sau đó, bạn nên truy vấn schema của các bảng phù hợp nhất.

        KHI NGƯỜI DÙNG HỎI VỀ HÌNH ẢNH:
            1. Dùng SQL tool để lấy bài post có hình ảnh
            2. Dùng analyze_image_from_url tool để lấy dữ liệu ảnh (base64)
            3. Nhìn ảnh trực tiếp (AI model của bạn hỗ trợ vision)
            4. Trả lời dựa trên vision reasoning của LLM

        HỮU DỤNG: Khi bạn nhận được kết quả từ analyze_image_from_url, 
        hãy xem ảnh trong nội dung của nó (trường "image_url").

        Bạn PHẢI luôn trả lời bằng tiếng Việt. Văn phong chuyên nghiệp, đi vào trọng tâm.
        Bạn không cần in đậm hay format văn bản gì khi gửi trả lời để tránh hiển thị ***. Đừng quên điều này.
        """.format(
            dialect=self.db.dialect,
            top_k=2,
            year=datetime.date.today().year
        )

        # ✅ TẠO MIDDLEWARE CACHE ĐỘNG
        self.cache_middleware = DynamicCacheMiddleware(ttl_seconds=300)

        # Create agent
        self.agent = create_agent(
            self.model,
            self.tools,
            system_prompt=system_prompt,
            middleware=[self.cache_middleware]
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