"""
엔티티 추출 유틸리티 - Bedrock Claude 사용
"""
import os
from datetime import datetime
from strands import Agent
from strands.models import BedrockModel


def load_graph_extraction_prompt():
    """Load the graph extraction prompt from file"""
    prompt_path = os.path.join(os.path.dirname(__file__), '..', 'prompts', 'graph_extraction.md')
    with open(prompt_path, 'r', encoding='utf-8') as f:
        return f.read()

