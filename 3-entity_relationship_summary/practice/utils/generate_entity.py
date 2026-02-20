"""
Bedrock Agent 유틸리티
- LLM 호출을 위한 공통 함수
"""
from strands import Agent
from strands.models import BedrockModel

# 기본 설정
DEFAULT_MODEL_ID = "global.anthropic.claude-opus-4-5-20251101-v1:0"
DEFAULT_REGION = "us-west-2"
DEFAULT_TEMPERATURE = 0.1


def get_bedrock_agent(
    model_id: str = DEFAULT_MODEL_ID,
    region_name: str = DEFAULT_REGION,
    temperature: float = DEFAULT_TEMPERATURE
) -> Agent:
    """
    Bedrock Agent 인스턴스 생성
    
    Args:
        model_id: Bedrock 모델 ID
        region_name: AWS 리전
        temperature: 생성 온도
    
    Returns:
        Agent: Strands Agent 인스턴스
    """
    bedrock_model = BedrockModel(
        model_id=model_id,
        region_name=region_name,
        temperature=temperature,
    )
    return Agent(model=bedrock_model)


def call_llm(prompt: str, agent: Agent = None) -> str:
    """
    LLM 호출
    
    Args:
        prompt: 프롬프트 문자열
        agent: Agent 인스턴스 (없으면 새로 생성)
    
    Returns:
        str: LLM 응답
    """
    if agent is None:
        agent = get_bedrock_agent()
    
    return agent(prompt)
