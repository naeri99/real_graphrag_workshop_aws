"""
1단계: Entity Summarization
- Neptune에서 요약이 필요한 엔티티 조회
- LLM으로 description들을 요약
- Neptune에 summary 저장
"""
import json
import re
import uuid
from datetime import datetime
from utils.generate_entity import get_bedrock_agent
from utils.parse_utils import parse_summary_output
from neptune.cyper_queries import (
    get_all_entities_for_summary,
    save_entity_summary,
    execute_cypher
)


def generate_neptune_id(name, entity_type):
    """Neptune ID 생성: 이름_엔티티타입_UUID"""
    clean_name = re.sub(r'[^\w가-힣]', '_', name)
    clean_name = re.sub(r'_+', '_', clean_name)
    clean_name = clean_name.strip('_')
    unique_id = str(uuid.uuid4())[:8]
    return f"{clean_name}_{entity_type}_{unique_id}"


def update_entity_neptune_id(entity_name, entity_type, neptune_id=None):
    """Neptune에서 엔티티에 neptune_id 속성 추가 (기존 ID가 있으면 유지)"""
    check_query = f"""
    MATCH (n:{entity_type} {{name: $entity_name}})
    RETURN n.name AS name, n.neptune_id AS existing_neptune_id
    """
    check_result = execute_cypher(check_query, entity_name=entity_name)
    
    if check_result and 'results' in check_result and check_result['results']:
        existing_id = check_result['results'][0].get('existing_neptune_id')
        if existing_id:
            return {"existing_id": existing_id, "created_new": False}
    
    if not neptune_id:
        neptune_id = generate_neptune_id(entity_name, entity_type)
    
    update_query = f"""
    MATCH (n:{entity_type} {{name: $entity_name}})
    SET n.neptune_id = $neptune_id
    RETURN n.name AS name, n.neptune_id AS neptune_id
    """
    result = execute_cypher(update_query, entity_name=entity_name, neptune_id=neptune_id)
    return {"neptune_id": neptune_id, "created_new": True, "result": result}


def load_summarize_prompt():
    """Load the summarization prompt from file"""
    with open('./prompts/summarization.md', 'r', encoding='utf-8') as f:
        return f.read()



