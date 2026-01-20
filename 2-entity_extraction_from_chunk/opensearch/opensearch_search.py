"""
OpenSearch 엔티티 검색 유틸리티
"""
from typing import Dict, List, Tuple
from opensearch.opensearch_con import get_opensearch_client


def search_entity_in_opensearch(
    entity_name: str, 
    entity_type: str, 
    opensearch_client=None, 
    index_name: str = "entities"
) -> Tuple[str, bool, str]:
    """
    OpenSearch에서 동의어를 우선으로 엔티티를 검색하여 정확한 이름을 찾습니다.
    
    Returns:
        tuple: (정확한 엔티티 이름, 매칭 여부, 매칭 타입)
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
        
        return entity_name, False, 'not_found'
        
    except Exception as e:
        print(f"   ❌ OpenSearch 검색 오류: {e}")
        return entity_name, False, 'not_found'


def resolve_entities(entities: List[Dict], opensearch_client=None, index_name: str = "entities") -> Tuple[List[Dict], Dict]:
    """
    엔티티 리스트를 OpenSearch를 통해 해결하고 메트릭을 반환합니다.
    
    Returns:
        tuple: (해결된 엔티티 리스트, 메트릭)
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not entities:
        return [], {'matched': 0, 'new': 0, 'synonym_exact': 0, 'synonym_partial': 0, 'name_exact': 0}
    
    resolved = []
    metrics = {'matched': 0, 'new': 0, 'synonym_exact': 0, 'synonym_partial': 0, 'name_exact': 0}
    
    for entity in entities:
        original_name = entity.get('entity_name', '').strip()
        entity_type = entity.get('entity_type', '').strip()
        
        if not original_name or not entity_type:
            resolved.append(entity)
            continue
        
        resolved_name, found, match_type = search_entity_in_opensearch(
            original_name, entity_type, opensearch_client, index_name
        )
        
        if found:
            metrics['matched'] += 1
            metrics[match_type] += 1
        else:
            metrics['new'] += 1
        
        updated = entity.copy()
        updated['entity_name'] = resolved_name
        updated['_original_name'] = original_name
        updated['_matched'] = found
        updated['_match_type'] = match_type
        resolved.append(updated)
    
    return resolved, metrics
