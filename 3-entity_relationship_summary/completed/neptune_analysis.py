"""
OpenSearch 엔티티 분석 - neptune_id가 없는 엔티티 찾기
"""
from opensearch.opensearch_con import get_opensearch_client
from neptune.cyper_queries import execute_cypher


def get_all_opensearch_entities(index_name="entities"):
    """OpenSearch에서 모든 엔티티 조회"""
    client = get_opensearch_client()
    
    query = {
        "size": 1000,
        "query": {"match_all": {}},
        "_source": ["entity.name", "entity.entity_type", "entity.neptune_id", "entity.summary"]
    }
    
    response = client.search(index=index_name, body=query)
    return response.get('hits', {}).get('hits', [])


def get_all_neptune_entities():
    """Neptune에서 모든 엔티티 조회"""
    query = """
    MATCH (n)
    WHERE n.name IS NOT NULL 
      AND n.summary IS NOT NULL 
      AND NOT n:__Chunk__
    RETURN n.name AS name, labels(n) AS entity_type, n.summary AS summary, n.neptune_id AS neptune_id
    """
    result = execute_cypher(query)
    return result.get('results', []) if result else []


def analyze_missing_neptune_ids():
    """neptune_id가 없는 OpenSearch 엔티티 분석"""
    print("=" * 60)
    print("🔍 OpenSearch 엔티티 분석")
    print("=" * 60)
    
    # OpenSearch 엔티티 조회
    os_entities = get_all_opensearch_entities()
    print(f"📊 OpenSearch 총 엔티티: {len(os_entities)}개")
    
    # neptune_id 유무 분류
    with_neptune_id = []
    without_neptune_id = []
    
    for hit in os_entities:
        entity = hit['_source'].get('entity', {})
        name = entity.get('name')
        entity_type = entity.get('entity_type')
        neptune_id = entity.get('neptune_id')
        has_summary = bool(entity.get('summary'))
        
        info = {
            'doc_id': hit['_id'],
            'name': name,
            'entity_type': entity_type,
            'neptune_id': neptune_id,
            'has_summary': has_summary
        }
        
        if neptune_id:
            with_neptune_id.append(info)
        else:
            without_neptune_id.append(info)
    
    print(f"✅ neptune_id 있음: {len(with_neptune_id)}개")
    print(f"❌ neptune_id 없음: {len(without_neptune_id)}개")
    
    # neptune_id 없는 엔티티 상세
    print("\n" + "=" * 60)
    print("❌ neptune_id가 없는 엔티티 목록:")
    print("=" * 60)
    
    for i, e in enumerate(without_neptune_id, 1):
        print(f"{i}. {e['name']} ({e['entity_type']}) - summary: {e['has_summary']}")
    
    # Neptune 엔티티 조회
    print("\n" + "=" * 60)
    print("🔍 Neptune 엔티티 분석")
    print("=" * 60)
    
    neptune_entities = get_all_neptune_entities()
    print(f"📊 Neptune 총 엔티티: {len(neptune_entities)}개")
    
    # Neptune에서 이름+타입으로 매핑 생성
    neptune_map = {}
    for e in neptune_entities:
        name = e.get('name')
        entity_type = e.get('entity_type', [])
        if entity_type:
            entity_type = entity_type[0] if isinstance(entity_type, list) else entity_type
        key = f"{name}_{entity_type}"
        neptune_map[key] = e
    
    # OpenSearch에는 있지만 Neptune에는 없는 엔티티
    print("\n" + "=" * 60)
    print("🔍 OpenSearch에만 있고 Neptune에 없는 엔티티:")
    print("=" * 60)
    
    only_in_opensearch = []
    for e in without_neptune_id:
        key = f"{e['name']}_{e['entity_type']}"
        if key not in neptune_map:
            only_in_opensearch.append(e)
            print(f"  - {e['name']} ({e['entity_type']})")
    
    print(f"\n총 {len(only_in_opensearch)}개")
    
    # Neptune에는 있지만 OpenSearch에서 못 찾는 엔티티
    print("\n" + "=" * 60)
    print("🔍 Neptune에 있지만 OpenSearch에서 매칭 안 되는 엔티티:")
    print("=" * 60)
    
    os_map = {}
    for hit in os_entities:
        entity = hit['_source'].get('entity', {})
        key = f"{entity.get('name')}_{entity.get('entity_type')}"
        os_map[key] = entity
    
    not_matched = []
    for e in neptune_entities:
        name = e.get('name')
        entity_type = e.get('entity_type', [])
        if entity_type:
            entity_type = entity_type[0] if isinstance(entity_type, list) else entity_type
        key = f"{name}_{entity_type}"
        if key not in os_map:
            not_matched.append({'name': name, 'entity_type': entity_type})
            print(f"  - {name} ({entity_type})")
    
    print(f"\n총 {len(not_matched)}개")
    
    return {
        'os_total': len(os_entities),
        'with_neptune_id': len(with_neptune_id),
        'without_neptune_id': len(without_neptune_id),
        'neptune_total': len(neptune_entities),
        'only_in_opensearch': len(only_in_opensearch),
        'not_matched': len(not_matched)
    }


if __name__ == "__main__":
    result = analyze_missing_neptune_ids()
    
    print("\n" + "=" * 60)
    print("📊 요약")
    print("=" * 60)
    print(f"OpenSearch 총: {result['os_total']}개")
    print(f"  - neptune_id 있음: {result['with_neptune_id']}개")
    print(f"  - neptune_id 없음: {result['without_neptune_id']}개")
    print(f"Neptune 총: {result['neptune_total']}개")
    print(f"OpenSearch에만 있음: {result['only_in_opensearch']}개")
    print(f"Neptune에만 있음 (매칭 안됨): {result['not_matched']}개")
