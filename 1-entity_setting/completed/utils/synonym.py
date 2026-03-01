"""
동의어 처리 유틸리티 모듈
- 공백 제거
- 동의어 병합
- OpenSearch 동의어 업데이트
"""


def clean_entity_whitespace(entity_data: dict) -> dict:
    """엔티티 데이터의 모든 문자열 필드에서 공백을 제거합니다."""
    cleaned_entity = {}
    
    for key, value in entity_data.items():
        if isinstance(value, str):
            cleaned_entity[key] = value.strip()
        elif isinstance(value, list):
            flat = []
            for item in value:
                if isinstance(item, str):
                    flat.append(item.strip())
                elif isinstance(item, list):
                    flat.extend(s.strip() for s in item if isinstance(s, str))
                else:
                    flat.append(item)
            cleaned_entity[key] = flat
        else:
            cleaned_entity[key] = value
    
    return cleaned_entity


def clean_entities_whitespace(entities_list: list) -> list:
    """엔티티 리스트의 모든 엔티티에서 공백을 제거합니다."""
    if not entities_list:
        return entities_list
    
    return [clean_entity_whitespace(entity) for entity in entities_list]


def merge_synonyms_with_set(existing_synonyms, new_synonyms) -> list:
    """
    기존 동의어와 새 동의어를 set을 사용하여 중복 제거하고 병합합니다.
    """
    def _flatten(syns):
        result = set()
        if isinstance(syns, str):
            for s in syns.split(','):
                s = s.strip()
                if s:
                    result.add(s)
        elif isinstance(syns, list):
            for item in syns:
                if isinstance(item, str):
                    s = item.strip()
                    if s:
                        result.add(s)
                elif isinstance(item, list):
                    for sub in item:
                        if isinstance(sub, str):
                            s = sub.strip()
                            if s:
                                result.add(s)
        return result

    existing_set = _flatten(existing_synonyms)
    new_set = _flatten(new_synonyms)
    return sorted(list(existing_set.union(new_set)))


def update_entity_synonyms(opensearch_client, entity_id: str, merged_synonyms: list, index_name: str = "entities") -> bool:
    """
    OpenSearch에서 엔티티의 동의어를 업데이트합니다.
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        entity_id: 업데이트할 엔티티의 문서 ID
        merged_synonyms: 병합된 동의어 리스트
        index_name: 인덱스 이름
        
    Returns:
        bool: 업데이트 성공 여부
    """
    try:
        update_body = {
            "doc": {
                "entity": {
                    "synonym": merged_synonyms  # 배열로 저장
                }
            }
        }
        
        response = opensearch_client.update(
            index=index_name,
            id=entity_id,
            body=update_body
        )
        
        return response.get('result') in ['updated', 'noop']
            
    except Exception as e:
        print(f"❌ 동의어 업데이트 오류: {e}")
        return False


def process_entity_synonym(opensearch_client, entity_data: dict, index_name: str = "entities"):
    """
    단일 엔티티의 동의어를 처리합니다.
    1. 공백 제거
    2. OpenSearch에서 기존 엔티티 검색
    3. 동의어 병합 (set으로 중복 제거)
    4. OpenSearch에 저장
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        entity_data: 엔티티 데이터 {'entity_name', 'entity_type', 'synonyms'}
        index_name: 인덱스 이름
        
    Returns:
        dict: 처리 결과
    """
    from opensearch.opensearh_search import find_entity_opensearch
    
    # 1. 공백 제거
    cleaned_entity = clean_entity_whitespace(entity_data)
    entity_name = cleaned_entity['entity_name']
    new_synonyms = cleaned_entity['synonyms']
    
    # 2. OpenSearch에서 기존 엔티티 검색
    existing_entity = find_entity_opensearch(opensearch_client, entity_name, index_name)
    
    if not existing_entity:
        return {'entity_name': entity_name, 'status': 'not_found', 'updated': False}
    
    # 3. 동의어 병합 (set으로 중복 제거)
    existing_synonyms = existing_entity['entity'].get('synonym', [])
    merged_synonyms = merge_synonyms_with_set(existing_synonyms, new_synonyms)
    
    # 4. OpenSearch에 저장
    success = update_entity_synonyms(
        opensearch_client, 
        existing_entity['id'], 
        merged_synonyms, 
        index_name
    )
    
    return {
        'entity_name': entity_name,
        'status': 'updated' if success else 'failed',
        'updated': success,
        'merged_synonyms': merged_synonyms
    }


def process_entities_synonyms(opensearch_client, entities_list: list, index_name: str = "entities") -> dict:
    """
    엔티티 리스트의 동의어를 일괄 처리합니다.
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        entities_list: 엔티티 리스트
        index_name: 인덱스 이름
        
    Returns:
        dict: 처리 결과 통계
    """
    results = {
        'total': len(entities_list),
        'found': 0,
        'not_found': 0,
        'updated': 0,
        'failed': 0
    }
    
    for entity_data in entities_list:
        result = process_entity_synonym(opensearch_client, entity_data, index_name)
        
        if result['status'] == 'not_found':
            results['not_found'] += 1
        elif result['updated']:
            results['found'] += 1
            results['updated'] += 1
        else:
            results['found'] += 1
            results['failed'] += 1
    
    return results
