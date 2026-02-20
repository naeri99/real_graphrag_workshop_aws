"""
Smart Graph Search using LLM
- Cypher 쿼리 생성
- 쿼리 실행
- 결과 요약
"""
import json
from typing import Dict, Any, List
from datetime import datetime

from strands import Agent
from strands.models import BedrockModel
from utils.query_generator import generate_cypher_prompt
from neptune.cyper_queries import execute_cypher
from utils.parse_utils import parse_cypher_output

# 기본 설정
DEFAULT_MODEL_ID = "global.anthropic.claude-opus-4-5-20251101-v1:0"
DEFAULT_REGION = "us-west-2"


class SmartGraphSearchLLM:
    """Smart Graph Search using LLM to generate Cypher queries and summarize results."""
    
    def __init__(self, model_id=DEFAULT_MODEL_ID, region=DEFAULT_REGION):
        """Initialize the SmartGraphSearchLLM with Bedrock model."""
        self.bedrock_model = BedrockModel(
            model_id=model_id,
            region_name=region,
            temperature=0.1,
        )
        self.agent = Agent(model=self.bedrock_model)
    
    def generate_cypher_query(self, user_question: str) -> str:
        """Generate Cypher query from natural language question using LLM."""
        try:
            prompt = generate_cypher_prompt(user_question)
            response = self.agent(prompt)
            parsed = parse_cypher_output(response)
            
            if parsed and 'cypher_query' in parsed:
                return parsed['cypher_query']
            else:
                return self._extract_cypher_from_text(response)
                
        except Exception as e:
            print(f"Error generating Cypher query: {e}")
            return ""
    
    def _extract_cypher_from_text(self, text: str) -> str:
        """Extract Cypher query from LLM response text as fallback."""
        import re
        
        cypher_patterns = [
            r'```cypher\s*(.*?)\s*```',
            r'```\s*(MATCH.*?)\s*```',
            r'(MATCH.*?)(?:\n\n|\Z)',
        ]
        
        for pattern in cypher_patterns:
            match = re.search(pattern, str(text), re.DOTALL | re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return ""
    
    def execute_query(self, cypher_query: str) -> Dict[str, Any]:
        """Execute Cypher query against Neptune database."""
        try:
            result = execute_cypher(cypher_query)
            return result
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "results": []
            }
    
    def summarize_results(self, user_question: str, results: List[Dict], cypher_query: str) -> str:
        """Generate natural language summary of query results using LLM."""
        try:
            prompt = self._create_summary_prompt(user_question, results, cypher_query)
            summary = self.agent(prompt)
            
            if hasattr(summary, 'text'):
                return summary.text.strip()
            else:
                return str(summary).strip()
            
        except Exception as e:
            return f"요약 생성 중 오류 발생: {str(e)}"
    
    def _create_summary_prompt(self, user_question: str, results: List[Dict], cypher_query: str) -> str:
        """Create prompt for result summarization."""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        limited_results = results[:10] if len(results) > 10 else results
        results_text = json.dumps(limited_results, ensure_ascii=False, indent=2)
        
        prompt = f"""
현재 시간: {current_time}

사용자 질문: {user_question}

실행된 Cypher 쿼리:
{cypher_query}

쿼리 결과 ({len(results)}개 중 최대 10개):
{results_text}

위의 쿼리 결과를 바탕으로 사용자의 질문에 대한 자연스럽고 이해하기 쉬운 한국어 답변을 작성해주세요.

답변 작성 가이드라인:
1. 결과가 없으면 "해당 조건에 맞는 정보를 찾을 수 없습니다"라고 답변
2. 결과가 있으면 핵심 정보를 요약하여 설명
3. 인물 관계나 특정 정보가 있으면 구체적으로 설명
4. 결과 개수가 많으면 "총 X개의 결과 중 주요 내용"임을 언급
5. 자연스럽고 친근한 톤으로 작성

답변:
"""
        return prompt
    
    def smart_search(self, user_question: str) -> Dict[str, Any]:
        """Perform end-to-end smart search: generate query, execute, and summarize."""
        try:
            # Step 1: Generate Cypher query
            cypher_query = self.generate_cypher_query(user_question)
            
            if not cypher_query:
                return {
                    "success": False,
                    "error": "Cypher 쿼리 생성 실패",
                    "user_question": user_question,
                    "cypher_query": "",
                    "results": [],
                    "results_count": 0,
                    "summary": "질문을 이해하지 못했습니다. 다른 방식으로 질문해주세요."
                }
            
            # Step 2: Execute query
            query_result = self.execute_query(cypher_query)
            
            if not query_result or 'results' not in query_result:
                return {
                    "success": False,
                    "error": "쿼리 실행 실패",
                    "user_question": user_question,
                    "cypher_query": cypher_query,
                    "results": [],
                    "results_count": 0,
                    "summary": "데이터베이스 쿼리 실행 중 오류가 발생했습니다."
                }
            
            results = query_result['results']
            results_count = len(results)
            
            # Step 3: Generate summary
            summary = self.summarize_results(user_question, results, cypher_query)
            
            return {
                "success": True,
                "user_question": user_question,
                "cypher_query": cypher_query,
                "results": results,
                "results_count": results_count,
                "summary": summary
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "user_question": user_question,
                "cypher_query": "",
                "results": [],
                "results_count": 0,
                "summary": f"검색 중 오류가 발생했습니다: {str(e)}"
            }
