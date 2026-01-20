"""
LLM 출력 파싱 유틸리티
"""
import json
import re


def parse_cypher_output(output):
    """
    LLM 응답에서 Cypher 쿼리를 파싱합니다.
    """
    try:
        output_str = output['text']
    except (KeyError, TypeError):
        output_str = str(output)
    
    # JSON 블록에서 cypher_query 추출 시도
    json_match = re.search(r'```json\s*(.*?)\s*```', output_str, re.DOTALL)
    if json_match:
        try:
            json_data = json.loads(json_match.group(1))
            if 'cypher_query' in json_data:
                return json_data
        except json.JSONDecodeError:
            pass
    
    # Cypher 코드 블록 추출 시도
    cypher_patterns = [
        r'```cypher\s*(.*?)\s*```',
        r'```\s*(MATCH.*?)\s*```',
        r'(MATCH.*?)(?:\n\n|\Z)',
    ]
    
    for pattern in cypher_patterns:
        match = re.search(pattern, output_str, re.DOTALL | re.IGNORECASE)
        if match:
            cypher_query = match.group(1).strip()
            return {"cypher_query": cypher_query}
    
    # JSON 형태로 cypher_query가 있는지 확인
    json_pattern = r'\{[^{}]*"cypher_query"[^{}]*\}'
    json_match = re.search(json_pattern, output_str, re.DOTALL)
    if json_match:
        try:
            result = json.loads(json_match.group(0))
            return result
        except json.JSONDecodeError:
            pass
    
    return None


def parse_search_context(output_str):
    """
    검색 컨텍스트에서 엔티티를 파싱합니다.
    
    지원하는 형태:
    - ##("entity"|코브)##("entity"|멜)##
    - ##("entity"|코브)##("entity"|멜)##<END>
    - ("entity"|코브)##("entity"|멜)
    """
    try:
        output_str = output_str['text']
    except (KeyError, TypeError):
        output_str = str(output_str)
    
    if "<END>" in output_str:
        output_str = output_str.replace("<END>", "")
    output_str = output_str.strip()
    
    entities = []
    
    pattern = r'\("entity"\|([^)]+)\)'
    matches = re.findall(pattern, output_str)
    
    for match in matches:
        entity_name = match.strip()
        if entity_name:
            entities.append(entity_name)
    
    return entities
