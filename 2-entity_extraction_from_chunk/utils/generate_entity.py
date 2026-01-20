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


def extract_entities(payload):
    """
    Extract entities from user queries using graph extraction prompt
    
    Args:
        payload: dict with "user_query" key containing the text to extract entities from
    
    Returns:
        AgentResult with extracted entities
    """
    bedrock_model = BedrockModel(
        model_id="us.anthropic.claude-sonnet-4-20250514-v1:0",
        region_name="us-west-2",
        temperature=0.3,
    )

    agent = Agent(model=bedrock_model)
        
    # Get user query from payload
    user_query = payload.get("user_query", "")
    
    # Load prompt template
    prompt_template = load_graph_extraction_prompt()
    
    # Format prompt with current time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_prompt = prompt_template.format(CURRENT_TIME=current_time)
    
    full_prompt = f"{formatted_prompt}\n\nText:\n{user_query}"
    
    # Extract entities using the agent
    response = agent(full_prompt)
    
    return response
