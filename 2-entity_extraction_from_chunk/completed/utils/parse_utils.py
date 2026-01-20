"""
LLM 출력 파싱 유틸리티
"""
import json
import re


def parse_cypher_output(output):
    """
    LLM 응답에서 Cypher 쿼리를 파싱합니다.
    
    Args:
        output: AgentResult 객체 또는 문자열
    
    Returns:
        dict: {"cypher_query": "MATCH ..."} 형태의 딕셔너리
              파싱 실패 시 None 반환
    """
    # AgentResult를 문자열로 변환
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


def parse_summary_output(output):
    """
    LLM 응답에서 JSON 형태의 summary 결과를 파싱합니다.
    
    Args:
        output: AgentResult 객체 또는 문자열
    
    Returns:
        dict: {"entity": "이름", "summary": "요약"} 형태의 딕셔너리
              파싱 실패 시 None 반환
    """
    # AgentResult를 문자열로 변환
    try:
        output_str = output['text']
    except (KeyError, TypeError):
        output_str = str(output)
    
    # JSON 블록 추출 (```json ... ``` 또는 { ... })
    json_match = re.search(r'```json\s*(.*?)\s*```', output_str, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        # ```json 없이 바로 JSON인 경우
        json_match = re.search(r'\{[^{}]*"entity"[^{}]*"summary"[^{}]*\}', output_str, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
        else:
            return None
    
    try:
        result = json.loads(json_str)
        return result
    except json.JSONDecodeError:
        return None


def parse_search_context(output_str):
    """
    검색 컨텍스트에서 엔티티를 파싱합니다.
    
    지원하는 형태:
    - ##("entity"|코브)##("entity"|멜)##
    - ##("entity"|코브)##("entity"|멜)##<END>
    - ("entity"|코브)##("entity"|멜)
    
    Parameters:
        output_str: LLM 출력 문자열 또는 AgentResult 객체
        
    Returns:
        List[str]: 추출된 엔티티 이름 리스트
    """
    # Convert AgentResult to string if needed
    try:
        output_str = output_str['text']
    except (KeyError, TypeError):
        output_str = str(output_str)
    
    # Remove completion markers if present
    if "<END>" in output_str:
        output_str = output_str.replace("<END>", "")
    output_str = output_str.strip()
    
    entities = []
    
    # Find all entity records using regex pattern
    # Pattern matches: ("entity"|<entity_name>)
    pattern = r'\("entity"\|([^)]+)\)'
    matches = re.findall(pattern, output_str)
    
    for match in matches:
        entity_name = match.strip()
        if entity_name:  # Only add non-empty entity names
            entities.append(entity_name)
    
    return entities


def parse_extraction_output(output_str, record_delimiter=None, tuple_delimiter=None):
    """
    Parse a structured output string containing "entity", "relationship" records into separate lists.

    The expected format for each record is:
        ("entity"|<entity_name>|<entity_type>|<entity_description>)
    or
        ("relationship"|<source_entity>|<source_type>|<target_entity>|<target_type>|<relationship_description>|<relationship_strength>)

    Records are separated by a record delimiter. The output string may end with a completion marker
    (for example, "<END>") which will be removed.

    Parameters:
        output_str: The complete string output or AgentResult object.
        record_delimiter (str, optional): The delimiter that separates records.
        tuple_delimiter (str, optional): The delimiter that separates fields within a record.

    Returns:
        Tuple[List[dict], List[dict]]: A tuple of (entities, relationships)
    """
    # Convert AgentResult to string if needed
    try:
        output_str = output_str['text']
    except (KeyError, TypeError):
        output_str = str(output_str)
    
    # Remove the completion delimiter if present.
    if "<END>" in output_str:
        output_str = output_str.replace("<END>", "")
    output_str = output_str.strip()

    # Determine the record delimiter if not provided.
    if record_delimiter is None:
        if "##" in output_str:
            record_delimiter = "##"
        elif "|" in output_str:
            record_delimiter = "|"
        else:
            record_delimiter = "\n"

    # Determine the tuple delimiter if not provided.
    if tuple_delimiter is None:
        if "|" in output_str:
            tuple_delimiter = "|"
        elif ";" in output_str:
            tuple_delimiter = ";"
        else:
            tuple_delimiter = "\t"

    # Split the output into individual record strings.
    raw_records = [r.strip() for r in output_str.split(record_delimiter)]

    entities = []
    relationships = []
    
    for rec in raw_records:
        if not rec:
            continue

        # Remove leading/trailing parentheses if present.
        if rec.startswith("(") and rec.endswith(")"):
            rec = rec[1:-1]
        rec = rec.strip()

        # Split the record into tokens using the tuple delimiter.
        tokens = [token.strip() for token in rec.split(tuple_delimiter)]
        if not tokens:
            continue

        # The first token should be "entity" or "relationship".
        rec_type = tokens[0].strip(' "\'').lower()

        if rec_type == "entity" and len(tokens) == 4:
            record = {
                "record_type": "entity",
                "entity_name": tokens[1],
                "entity_type": tokens[2],
                "entity_description": tokens[3]
            }
            entities.append(record)
        elif rec_type == "relationship" and len(tokens) == 7:
            # New format: ("relationship"|<source_entity>|<source_type>|<target_entity>|<target_type>|<description>|<strength>)
            try:
                strength = float(tokens[6])
                if strength.is_integer():
                    strength = int(strength)
            except ValueError:
                strength = tokens[6]
            record = {
                "record_type": "relationship",
                "source_entity": tokens[1],
                "source_type": tokens[2],
                "target_entity": tokens[3],
                "target_type": tokens[4],
                "relationship_description": tokens[5],
                "relationship_strength": strength
            }
            relationships.append(record)
        elif rec_type == "relationship" and len(tokens) == 5:
            # Legacy format fallback
            try:
                strength = float(tokens[4])
                if strength.is_integer():
                    strength = int(strength)
            except ValueError:
                strength = tokens[4]
            record = {
                "record_type": "relationship",
                "source_entity": tokens[1],
                "source_type": "",
                "target_entity": tokens[2],
                "target_type": "",
                "relationship_description": tokens[3],
                "relationship_strength": strength
            }
            relationships.append(record)
    
    return entities, relationships



