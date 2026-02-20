"""
2단계: Relationship Summarization
- Neptune에서 요약이 필요한 관계 조회
- LLM으로 description들을 요약
- Neptune에 relationship summary 저장
"""
from datetime import datetime
from utils.generate_entity import get_bedrock_agent
from utils.parse_utils import parse_summary_output
from neptune.cyper_queries import (
    get_all_relationships_for_summary,
    save_relationship_summary
)


def load_summarize_prompt():
    """Load the summarization prompt from file"""
    with open('./prompts/summarization.md', 'r', encoding='utf-8') as f:
        return f.read()


