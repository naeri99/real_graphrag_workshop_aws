"""
OpenSearch 엔티티 검색 유틸리티
"""
from typing import Dict, List, Tuple, Optional
from opensearch.opensearch_con import get_opensearch_client


def search_entity_by_synonym(
    entity_name: str, 
    entity_type: str, 
    opensearch_client=None, 
    index_name: str = "entities"
) -> Tuple[str, bool]:
    """
    OpenSearch에서 동의어를 통해 엔티티를 검색합니다.
    
    Args:
        entity_name: 검색할 엔티티 이름
        entity_type: 엔티티 타입 (ACTOR, MOVIE_CHARACTER, etc.)
        opensearch_client: OpenSearch 클라이언트 (None이면 자동 생성)
        index_name: 검색할 인덱스 이름
        
    Returns:
        Tuple[str, bool]: (정확한 엔티티 이름, 매칭 성공 여부)
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    entity_name = entity_name.strip() if entity_name else ""
    entity_type = entity_type.strip() if entity_type else ""
    
    if not entity_name or not entity_type:
        return entity_name, False
    
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
                return entity_real_name, True
            
            # 부분 매칭
            if any(entity_name in syn for syn in synonym_list):
                return entity_real_name, True
        
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
            return exact_name, True
        
        # 매칭 실패 - 원본 이름 반환
        return entity_name, False
        
    except Exception as e:
        print(f"   ❌ OpenSearch 검색 오류: {e}")
        return entity_name, False


def resolve_entities_with_cache(
    entities: List[Dict], 
    opensearch_client=None, 
    index_name: str = "entities"
) -> Tuple[List[Dict], Dict[str, str]]:
    """
    엔티티 리스트를 OpenSearch를 통해 해결하고 캐시(해시맵)를 반환합니다.
    
    Args:
        entities: 엔티티 리스트
        opensearch_client: OpenSearch 클라이언트
        index_name: 검색할 인덱스 이름
        
    Returns:
        Tuple[List[Dict], Dict[str, str]]: (해결된 엔티티 리스트, 이름 매핑 캐시)
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not entities:
        return [], {}
    
    resolved_entities = []
    name_cache = {}  # {(original_name, entity_type): resolved_name}
    
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
        else:
            # OpenSearch에서 검색
            resolved_name, found = search_entity_by_synonym(
                original_name, entity_type, opensearch_client, index_name
            )
            name_cache[cache_key] = resolved_name
            
            if found and resolved_name != original_name:
                print(f"   ✅ '{original_name}' → '{resolved_name}' ({entity_type})")
        
        # 엔티티 업데이트
        updated_entity = entity.copy()
        updated_entity['entity_name'] = resolved_name
        resolved_entities.append(updated_entity)
    
    return resolved_entities, name_cache


def resolve_relationships_with_cache(
    relationships: List[Dict], 
    name_cache: Dict[str, str],
    opensearch_client=None, 
    index_name: str = "entities"
) -> List[Dict]:
    """
    관계 리스트의 엔티티 이름을 캐시를 활용하여 해결합니다.
    
    Args:
        relationships: 관계 리스트
        name_cache: 엔티티 이름 매핑 캐시 {(original_name, type): resolved_name}
        opensearch_client: OpenSearch 클라이언트
        index_name: 검색할 인덱스 이름
        
    Returns:
        List[Dict]: 해결된 관계 리스트
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not relationships:
        return []
    
    resolved_relationships = []
    
    for rel in relationships:
        updated_rel = rel.copy()
        
        # Source 엔티티 해결
        source_name = rel.get('source_entity', '').strip()
        source_type = rel.get('source_type', '').strip()
        
        if source_name and source_type:
            cache_key = (source_name, source_type)
            if cache_key in name_cache:
                updated_rel['source_entity'] = name_cache[cache_key]
            else:
                resolved_name, _ = search_entity_by_synonym(
                    source_name, source_type, opensearch_client, index_name
                )
                updated_rel['source_entity'] = resolved_name
                name_cache[cache_key] = resolved_name
        
        # Target 엔티티 해결
        target_name = rel.get('target_entity', '').strip()
        target_type = rel.get('target_type', '').strip()
        
        if target_name and target_type:
            cache_key = (target_name, target_type)
            if cache_key in name_cache:
                updated_rel['target_entity'] = name_cache[cache_key]
            else:
                resolved_name, _ = search_entity_by_synonym(
                    target_name, target_type, opensearch_client, index_name
                )
                updated_rel['target_entity'] = resolved_name
                name_cache[cache_key] = resolved_name
        
        resolved_relationships.append(updated_rel)
    
    return resolved_relationships
