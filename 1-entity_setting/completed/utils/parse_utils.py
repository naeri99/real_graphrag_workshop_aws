"""
LLM 출력 파싱 유틸리티 모듈
"""
import re


def parse_synonym_output(output_str):
    """
    통합 동의어 파싱 함수 - 다양한 형태의 동의어 출력을 파싱합니다.
    
    지원하는 형태:
    1. ## 구분자로 분리된 형태: ("entity"|name|type|synonyms)##("entity"|name2|type2|synonyms2)##
    2. 연결된 형태: ("entity"|name|type|synonyms)("entity"|name2|type2|synonyms2)
    3. 단일 형태: ("entity"|name|type|synonyms)
    
    Parameters:
        output_str: LLM 출력 문자열 또는 AgentResult 객체
        
    Returns:
        List[dict]: 동의어 정보가 담긴 딕셔너리 리스트
                   각 딕셔너리: {"entity_name": str, "entity_type": str, "synonyms": list}
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
    
    synonym_records = []
    
    # Method 1: Try ## delimiter first (preferred format)
    if "##" in output_str:
        records = [record.strip() for record in output_str.split("##") if record.strip()]
        
        for record in records:
            pattern = r'\("entity"\|([^|]+)\|([^|]+)\|([^)]+)\)'
            matches = re.findall(pattern, record)
            
            for match in matches:
                entity_name = match[0].strip()
                entity_type = match[1].strip()
                synonyms_str = match[2].strip()
                
                synonyms = [syn.strip() for syn in synonyms_str.split(',') if syn.strip()]
                
                record_dict = {
                    "entity_name": entity_name,
                    "entity_type": entity_type,
                    "synonyms": synonyms
                }
                synonym_records.append(record_dict)
    
    # Method 2: Try concatenated format )("entity"| pattern
    elif ')("entity"' in output_str:
        if not output_str.startswith('('):
            output_str = '(' + output_str
        
        parts = output_str.split(')("entity"')
        
        for i, part in enumerate(parts):
            part = part.strip()
            
            if i == 0:
                if not part.endswith(')'):
                    part += ')'
            elif i == len(parts) - 1:
                part = '("entity"' + part
            else:
                part = '("entity"' + part + ')'
            
            pattern = r'\("entity"\|([^|]+)\|([^|]+)\|([^)]+)\)'
            matches = re.findall(pattern, part)
            
            for match in matches:
                entity_name = match[0].strip()
                entity_type = match[1].strip()
                synonyms_str = match[2].strip()
                
                synonyms = [syn.strip() for syn in synonyms_str.split(',') if syn.strip()]
                
                record_dict = {
                    "entity_name": entity_name,
                    "entity_type": entity_type,
                    "synonyms": synonyms
                }
                synonym_records.append(record_dict)
    
    # Method 3: Try single or multiple records without specific delimiters
    else:
        pattern = r'\("entity"\|([^|]+)\|([^|]+)\|([^)]+)\)'
        matches = re.findall(pattern, output_str)
        
        for match in matches:
            entity_name = match[0].strip()
            entity_type = match[1].strip()
            synonyms_str = match[2].strip()
            
            synonyms = [syn.strip() for syn in synonyms_str.split(',') if syn.strip()]
            
            record_dict = {
                "entity_name": entity_name,
                "entity_type": entity_type,
                "synonyms": synonyms
            }
            synonym_records.append(record_dict)
    
    return synonym_records


def parse_mixed_synonym_output(output_str):
    """
    Deprecated: Use parse_synonym_output instead.
    This function is kept for backward compatibility.
    """
    return parse_synonym_output(output_str)
