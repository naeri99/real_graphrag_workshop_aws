"""
OpenSearch 엔티티 검색 유틸리티
- 동의어 기반 엔티티 검색
- 엔티티 이름 해결
"""
from typing import Dict, List, Tuple
from opensearch.opensearch_con import get_opensearch_client


def search_entity_in_opensearch(
    entity_name: str, 
    opensearch_client=None, 
    index_name: str = "entities"
) -> str:
    """
    OpenSearch에서 엔티티를 검색하여 정확한 이름을 찾습니다.
    
    Args:
        entity_name: 검색할 엔티티 이름
        opensearch_client: OpenSearch 클라이언트
        index_name: 검색할 인덱스 이름
        
    Returns:
        str: 정확한 엔티티 이름 또는 원본 이름 (찾지 못한 경우)
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    try:
        entity_name = entity_name.strip() if entity_name else ""
        
        if not entity_name:
            return entity_name
        
        print(f"   🔍 검색 중: '{entity_name}'")
        
        # 1. 동의어 필드에서 검색
        search_body = {
            "query": {
                "match": {
                    "entity.synonym": entity_name
                }
            },
            "size": 10,
            "_source": ["entity.name", "entity.synonym", "entity.entity_type"]
        }
        
        response = opensearch_client.search(index=index_name, body=search_body)
        hits = response.get('hits', {}).get('hits', [])
        print(f"   📊 동의어 검색 결과: {len(hits)}개")
        
        # 검색 결과 출력
        for i, hit in enumerate(hits):
            entity = hit['_source'].get('entity', {})
            name = entity.get('name', '')
            synonyms = entity.get('synonym', '')
            etype = entity.get('entity_type', '')
            score = hit['_score']
            print(f"      {i+1}. {name} ({etype}) - 점수: {score:.4f}")
            print(f"         동의어: {synonyms}")
        
        # 가장 높은 점수의 결과 반환
        if hits:
            best_hit = hits[0]
            entity = best_hit['_source'].get('entity', {})
            entity_real_name = entity.get('name', '').strip()
            score = best_hit['_score']
            print(f"   ✅ 동의어 매칭: '{entity_name}' → '{entity_real_name}' (점수: {score:.4f})")
            return entity_real_name
        
        # 2. 유연한 검색 시도
        print(f"   🔄 유연한 검색 시도...")
        flexible_search_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "wildcard": {
                                "entity.synonym": f"*{entity_name}*"
                            }
                        },
                        {
                            "match": {
                                "entity.synonym": {
                                    "query": entity_name,
                                    "fuzziness": "AUTO"
                                }
                            }
                        }
                    ]
                }
            },
            "size": 5,
            "_source": ["entity.name", "entity.synonym", "entity.entity_type"]
        }
        
        flexible_response = opensearch_client.search(index=index_name, body=flexible_search_body)
        flexible_hits = flexible_response.get('hits', {}).get('hits', [])
        print(f"   📊 유연한 검색 결과: {len(flexible_hits)}개")
        
        for i, hit in enumerate(flexible_hits):
            entity = hit['_source'].get('entity', {})
            name = entity.get('name', '')
            synonyms = entity.get('synonym', '')
            etype = entity.get('entity_type', '')
            score = hit['_score']
            print(f"      {i+1}. {name} ({etype}) - 점수: {score:.4f}")
            print(f"         동의어: {synonyms}")
        
        if flexible_hits:
            best_hit = flexible_hits[0]
            
            # 정확한 동의어 매칭 우선
            for hit in flexible_hits:
                entity = hit['_source'].get('entity', {})
                synonyms = entity.get('synonym', '')
                
                if isinstance(synonyms, str):
                    synonym_list = [s.strip() for s in synonyms.split(',') if s.strip()]
                    if entity_name in synonym_list:
                        best_hit = hit
                        break
            
            entity = best_hit['_source'].get('entity', {})
            entity_real_name = entity.get('name', '').strip()
            score = best_hit['_score']
            print(f"   ✅ 유연한 매칭: '{entity_name}' → '{entity_real_name}' (점수: {score:.4f})")
            return entity_real_name
        
        print(f"   📝 매칭 없음: '{entity_name}' - 원본 이름 그대로 사용")
        return entity_name
        
    except Exception as e:
        print(f"   ❌ OpenSearch 검색 오류: {e} - 원본 이름 사용")
        return entity_name


def resolve_entities_with_opensearch(entities: list, opensearch_client=None) -> dict:
    """
    엔티티 리스트를 OpenSearch를 통해 정확한 이름으로 변환합니다.
    
    Args:
        entities: 엔티티 이름 리스트
        opensearch_client: OpenSearch 클라이언트
        
    Returns:
        dict: 원본 이름 → 해결된 이름 매핑
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not entities:
        return {}
    
    print(f"🔍 {len(entities)}개 엔티티의 정확한 이름 검색 중...")
    
    resolved_mapping = {}
    
    for entity_name in entities:
        resolved_name = search_entity_in_opensearch(entity_name, opensearch_client)
        resolved_mapping[entity_name] = resolved_name
        
        if resolved_name != entity_name:
            print(f"   📝 이름 변경: '{entity_name}' → '{resolved_name}'")
        else:
            print(f"   ✓ '{entity_name}' - 변경 없음")
    
    return resolved_mapping
