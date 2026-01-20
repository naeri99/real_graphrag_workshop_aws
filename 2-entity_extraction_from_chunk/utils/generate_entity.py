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


def extract_entities(payload, movie_context: str = ""):
    """
    Extract entities from user queries using graph extraction prompt
    
    Args:
        payload: dict with "user_query" key containing the text to extract entities from
        movie_context: optional movie context string for entity extraction
    
    Returns:
        AgentResult with extracted entities
    """
    bedrock_model = BedrockModel(
        model_id="global.anthropic.claude-opus-4-5-20251101-v1:0",
        region_name="us-west-2",
        temperature=0.3,
    )

    agent = Agent(model=bedrock_model)
        
    # Get user query from payload
    user_query = payload.get("user_query", "")
    
    # Load prompt template
    prompt_template = load_graph_extraction_prompt()
    
    # Format prompt with current time and movie context
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_prompt = prompt_template.format(
        CURRENT_TIME=current_time,
        MOVIE_CONTEXT=movie_context
    )
    
    full_prompt = f"{formatted_prompt}\n\nText:\n{user_query}"
    
    # Extract entities using the agent
    response = agent(full_prompt)
    
    return response


def extract_entity_from_search(payload):
    """
    Extract entities from search context using graph query prompt
    
    Args:
        payload: dict with "user_query" key
    
    Returns:
        AgentResult with extracted entities
    """
    bedrock_model = BedrockModel(
        model_id="global.anthropic.claude-opus-4-5-20251101-v1:0",
        region_name="us-west-2",
        temperature=0.3,
    )

    agent = Agent(model=bedrock_model)
            
    # Get user context from payload
    user_context = payload.get("user_query", "")

    # Load prompt template
    prompt_template = load_graph_query_prompt()
    
    # Format prompt with current time and user query
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_prompt = prompt_template.format(
        CURRENT_TIME=current_time,
        USER_QUERY=user_context
    )
    
    # Extract entities using the agent
    response = agent(formatted_prompt)
    
    return response
