"""
LLM 출력 파싱 유틸리티
"""
import json
import re


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


def parse_extraction_output(output_str, record_delimiter=None, tuple_delimiter=None):
    """
    Parse a structured output string containing "entity", "relationship" records into separate lists.
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
    
    return entities, relationships
