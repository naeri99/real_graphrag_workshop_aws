"""
Bedrock Agent 유틸리티 - 엔티티 추출
"""
from datetime import datetime
from strands import Agent
from strands.models import BedrockModel

# 기본 설정
DEFAULT_MODEL_ID = "global.anthropic.claude-opus-4-5-20251101-v1:0"
DEFAULT_REGION = "us-west-2"
DEFAULT_TEMPERATURE = 0.3


def get_bedrock_agent(
    model_id: str = DEFAULT_MODEL_ID,
    region_name: str = DEFAULT_REGION,
    temperature: float = DEFAULT_TEMPERATURE
) -> Agent:
    """Bedrock Agent 인스턴스 생성"""
    bedrock_model = BedrockModel(
        model_id=model_id,
        region_name=region_name,
        temperature=temperature,
    )
    return Agent(model=bedrock_model)


def load_get_entity_prompt():
    """Load the entity extraction prompt from file"""
    with open('./prompts/get_entity.md', 'r', encoding='utf-8') as f:
        return f.read()


def extract_entity_from_search(payload):
    """
    사용자 쿼리에서 엔티티를 추출합니다.
    
    Args:
        payload: {"user_query": "코브와 멜은 어떤 관계야?"}
    
    Returns:
        LLM 응답 (엔티티 목록)
    """
    agent = get_bedrock_agent()
    
    user_context = payload.get("user_query", "")
    
    prompt_template = load_get_entity_prompt()
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_prompt = prompt_template.format(
        CURRENT_TIME=current_time,
        USER_QUERY=user_context
    )
    
    response = agent(formatted_prompt)
    
    return response
