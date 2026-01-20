"""
엔티티 및 동의어 추출 모듈
- Bedrock Claude를 사용하여 영화 컨텍스트에서 동의어 추출
"""
from datetime import datetime
from pathlib import Path
from strands import Agent
from strands.models import BedrockModel


def load_synonym_prompt() -> str:
    """프롬프트 파일 로드"""
    prompt_path = Path(__file__).parent.parent / "prompts" / "synonym_generate.md"
    with open(prompt_path, 'r', encoding='utf-8') as f:
        return f.read()


def extract_synonym(payload: dict) -> str:
    """
    영화 컨텍스트에서 동의어 추출
    
    Args:
        payload: {
            "movie_context": str,  # 영화 정보 컨텍스트
            "movie_chunk": str     # 리뷰 텍스트 청크
        }
    
    Returns:
        str: LLM 응답 (동의어 목록)
    """
    bedrock_model = BedrockModel(
        model_id="apac.anthropic.claude-sonnet-4-20250514-v1:0",
        region_name="ap-northeast-2",
        temperature=0.3,
    )

    agent = Agent(model=bedrock_model)
        
    movie_context = payload.get("movie_context", "")
    movie_chunk = payload.get("movie_chunk", "")

    prompt_template = load_synonym_prompt()
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_prompt = prompt_template.format(
        CURRENT_TIME=current_time,
        MOVIE_CONTEXT=movie_context,
        MOVIE_CHUNK=movie_chunk
    )
    
    response = agent(formatted_prompt)
    
    return response
