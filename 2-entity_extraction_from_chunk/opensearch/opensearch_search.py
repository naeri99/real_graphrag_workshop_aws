"""
OpenSearch 엔티티 검색 유틸리티
"""
from typing import Dict, List, Tuple
from opensearch.opensearch_con import get_opensearch_client


def search_entity_by_synonym(
    entity_name: str, 
    entity_type: str, 
    opensearch_client=None, 
    index_name: str = "entities"
) -> Tuple[str, bool, str]:
    """
    OpenSearch에서 동의어를 통해 엔티티를 검색합니다.
    
    Args:
        entity_name: 검색할 엔티티 이름
        entity_type: 엔티티 타입 (ACTOR, MOVIE_CHARACTER, etc.)
        opensearch_client: OpenSearch 클라이언트 (None이면 자동 생성)
        index_name: 검색할 인덱스 이름
        
    Returns:
        Tuple[str, bool, str]: (정확한 엔티티 이름, 매칭 성공 여부, 매칭 타입)
        매칭 타입: 'synonym_exact', 'synonym_partial', 'name_exact', 'not_found'
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    entity_name = entity_name.strip() if entity_name else ""
    entity_type = entity_type.strip() if entity_type else ""
    
    if not entity_name or not entity_type:
        return entity_name, False, 'not_found'
    
    try:
        # 1. 해당 타입의 모든 엔티티에서 동의어 검색
        search_body = {
            "query": {
                "term": {
                    "entity.entity_type": entity_type
                }
            },
            "size": 100,
            "_source": ["entity.name", "entity.synonym", "entity.entity_type"]
        }
        
        response = opensearch_client.search(index=index_name, body=search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        # 정확한 동의어 매칭 찾기
        for hit in hits:
            entity = hit['_source'].get('entity', {})
            entity_real_name = entity.get('name', '').strip()
            synonyms = entity.get('synonym', '')
            
            if not synonyms:
                continue
            
            if isinstance(synonyms, str):
                synonym_list = [s.strip() for s in synonyms.split(',') if s.strip()]
            else:
                synonym_list = synonyms if isinstance(synonyms, list) else []
            
            # 정확한 매칭
            if entity_name in synonym_list:
                return entity_real_name, True, 'synonym_exact'
            
            # 부분 매칭
            if any(entity_name in syn for syn in synonym_list):
                return entity_real_name, True, 'synonym_partial'
        
        # 2. 정확한 이름 매칭 시도
        exact_search_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {"term": {"entity.name.keyword": {"value": entity_name, "boost": 3.0}}},
                                    {"match": {"entity.name": {"query": entity_name, "operator": "and", "boost": 2.0}}}
                                ]
                            }
                        },
                        {"term": {"entity.entity_type": entity_type}}
                    ]
                }
            },
            "size": 1,
            "_source": ["entity.name"]
        }
        
        response = opensearch_client.search(index=index_name, body=exact_search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        if hits:
            exact_name = hits[0]['_source'].get('entity', {}).get('name', entity_name).strip()
            return exact_name, True, 'name_exact'
        
        # 매칭 실패 - 원본 이름 반환
        return entity_name, False, 'not_found'
        
    except Exception as e:
        print(f"   ❌ OpenSearch 검색 오류: {e}")
        return entity_name, False, 'not_found'


def resolve_entities_with_cache(
    entities: List[Dict], 
    opensearch_client=None, 
    index_name: str = "entities"
) -> Tuple[List[Dict], Dict[str, str], Dict]:
    """
    엔티티 리스트를 OpenSearch를 통해 해결하고 캐시와 메트릭을 반환합니다.
    
    Args:
        entities: 엔티티 리스트
        opensearch_client: OpenSearch 클라이언트
        index_name: 검색할 인덱스 이름
        
    Returns:
        Tuple[List[Dict], Dict[str, str], Dict]: 
            - 해결된 엔티티 리스트
            - 이름 매핑 캐시
            - 메트릭 {matched_existing, new_entities, synonym_exact, synonym_partial, name_exact}
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not entities:
        return [], {}, {'matched_existing': 0, 'new_entities': 0, 'synonym_exact': 0, 'synonym_partial': 0, 'name_exact': 0}
    
    resolved_entities = []
    name_cache = {}  # {(original_name, entity_type): resolved_name}
    
    metrics = {
        'matched_existing': 0,  # 기존 엔티티에 매핑된 수
        'new_entities': 0,      # 새로 생성될 엔티티 수
        'synonym_exact': 0,     # 동의어 정확 매칭
        'synonym_partial': 0,   # 동의어 부분 매칭
        'name_exact': 0,        # 이름 정확 매칭
        'mappings': []          # 매핑 상세 정보
    }
    
    for entity in entities:
        original_name = entity.get('entity_name', '').strip()
        entity_type = entity.get('entity_type', '').strip()
        
        if not original_name or not entity_type:
            resolved_entities.append(entity)
            continue
        
        cache_key = (original_name, entity_type)
        
        # 캐시에서 먼저 확인
        if cache_key in name_cache:
            resolved_name = name_cache[cache_key]
            found = resolved_name != original_name
            match_type = 'cached'
        else:
            # OpenSearch에서 검색
            resolved_name, found, match_type = search_entity_by_synonym(
                original_name, entity_type, opensearch_client, index_name
            )
            name_cache[cache_key] = resolved_name
        
        # 메트릭 업데이트
        if found:
            metrics['matched_existing'] += 1
            if match_type == 'synonym_exact':
                metrics['synonym_exact'] += 1
            elif match_type == 'synonym_partial':
                metrics['synonym_partial'] += 1
            elif match_type == 'name_exact':
                metrics['name_exact'] += 1
            
            if resolved_name != original_name:
                metrics['mappings'].append({
                    'original': original_name,
                    'resolved': resolved_name,
                    'type': entity_type,
                    'match_type': match_type
                })
                print(f"   ✅ '{original_name}' → '{resolved_name}' ({entity_type}) [{match_type}]")
        else:
            metrics['new_entities'] += 1
            print(f"   🆕 '{original_name}' ({entity_type}) [NEW]")
        
        # 엔티티 업데이트
        updated_entity = entity.copy()
        updated_entity['entity_name'] = resolved_name
        updated_entity['_is_new'] = not found  # 새 엔티티 여부 표시
        updated_entity['_match_type'] = match_type
        resolved_entities.append(updated_entity)
    
    return resolved_entities, name_cache, metrics


def resolve_relationships_with_cache(
    relationships: List[Dict], 
    name_cache: Dict[str, str],
    opensearch_client=None, 
    index_name: str = "entities"
) -> Tuple[List[Dict], Dict]:
    """
    관계 리스트의 엔티티 이름을 캐시를 활용하여 해결합니다.
    
    Args:
        relationships: 관계 리스트
        name_cache: 엔티티 이름 매핑 캐시 {(original_name, type): resolved_name}
        opensearch_client: OpenSearch 클라이언트
        index_name: 검색할 인덱스 이름
        
    Returns:
        Tuple[List[Dict], Dict]: (해결된 관계 리스트, 메트릭)
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not relationships:
        return [], {'source_matched': 0, 'source_new': 0, 'target_matched': 0, 'target_new': 0}
    
    resolved_relationships = []
    metrics = {
        'source_matched': 0,
        'source_new': 0,
        'target_matched': 0,
        'target_new': 0
    }
    
    for rel in relationships:
        updated_rel = rel.copy()
        
        # Source 엔티티 해결
        source_name = rel.get('source_entity', '').strip()
        source_type = rel.get('source_type', '').strip()
        
        if source_name and source_type:
            cache_key = (source_name, source_type)
            if cache_key in name_cache:
                updated_rel['source_entity'] = name_cache[cache_key]
                metrics['source_matched'] += 1
            else:
                resolved_name, found, _ = search_entity_by_synonym(
                    source_name, source_type, opensearch_client, index_name
                )
                updated_rel['source_entity'] = resolved_name
                name_cache[cache_key] = resolved_name
                if found:
                    metrics['source_matched'] += 1
                else:
                    metrics['source_new'] += 1
        
        # Target 엔티티 해결
        target_name = rel.get('target_entity', '').strip()
        target_type = rel.get('target_type', '').strip()
        
        if target_name and target_type:
            cache_key = (target_name, target_type)
            if cache_key in name_cache:
                updated_rel['target_entity'] = name_cache[cache_key]
                metrics['target_matched'] += 1
            else:
                resolved_name, found, _ = search_entity_by_synonym(
                    target_name, target_type, opensearch_client, index_name
                )
                updated_rel['target_entity'] = resolved_name
                name_cache[cache_key] = resolved_name
                if found:
                    metrics['target_matched'] += 1
                else:
                    metrics['target_new'] += 1
        
        resolved_relationships.append(updated_rel)
    
    return resolved_relationships, metrics
