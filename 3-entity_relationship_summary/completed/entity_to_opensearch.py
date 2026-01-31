"""
3단계: Entity to OpenSearch
- Neptune에서 요약된 엔티티 조회
- OpenSearch에서 name으로 exact match 검색
- 존재하는 엔티티만 summary, summary_vec 업데이트
"""
from neptune.cyper_queries import execute_cypher
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import (
    validate_opensearch_index,
    refresh_opensearch_index
)
from utils.bedrock_embedding import BedrockEmbedding


def get_summarized_entities_from_neptune():
    """Neptune에서 요약이 완료된 엔티티들 조회 (모든 엔티티)"""
    query = """
    MATCH (n)
    WHERE n.name IS NOT NULL 
      AND n.summary IS NOT NULL 
      AND NOT n:__Chunk__
    RETURN n.name AS name, labels(n) AS entity_type, n.summary AS summary, n.neptune_id AS neptune_id
    ORDER BY n.name
    """
    return execute_cypher(query)


def find_entity_by_name_exact(opensearch_client, index_name: str, entity_name: str, entity_type: str):
    """
    OpenSearch에서 entity.name과 entity_type으로 검색
    
    Returns:
        dict: {'id': doc_id, 'entity': entity_data} or None
    """
    try:
        query = {
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
            "min_score": 3.4,
            "_source": ["entity.name"]
        }
        response = opensearch_client.search(index=index_name, body=query)
        
        hits = response.get('hits', {}).get('hits', [])
        print(f"   🔍 검색: {entity_name} ({entity_type}) -> {len(hits)}개 결과")
        
        if hits:
            # 정확히 일치하는 것만 반환
            for hit in hits:
                os_name = hit['_source'].get('entity', {}).get('name')
                print(f"      비교: '{os_name}' == '{entity_name}' -> {os_name == entity_name}")
                if os_name == entity_name:
                    return {
                        'id': hit['_id'],
                        'entity': hit['_source'].get('entity', {})
                    }
        return None
    except Exception as e:
        print(f"   ❌ 검색 오류 ({entity_name}): {e}")
        return None


def update_entity_summary(opensearch_client, index_name: str, doc_id: str, summary: str, summary_vec: list, neptune_id: str) -> bool:
    """
    OpenSearch 엔티티의 summary, summary_vec, neptune_id 업데이트
    """
    try:
        update_body = {
            "doc": {
                "entity": {
                    "summary": summary,
                    "summary_vec": summary_vec,
                    "neptune_id": neptune_id
                }
            }
        }
        response = opensearch_client.update(
            index=index_name,
            id=doc_id,
            body=update_body,
            refresh=False
        )
        return response.get('result') in ['updated', 'noop']
    except Exception as e:
        print(f"   ❌ 업데이트 오류: {e}")
        return False


def run_entity_to_opensearch(index_name="entities", validate_index=True):
    """
    Entity to OpenSearch 실행
    1. Neptune에서 요약된 엔티티 조회
    2. OpenSearch에서 name으로 exact match 검색
    3. 존재하는 엔티티만 summary, summary_vec 업데이트
    """
    print("=" * 60)
    print("🚀 Entity to OpenSearch Start")
    print("=" * 60)
    
    # OpenSearch 클라이언트 초기화
    opensearch_client = get_opensearch_client()
    
    # 인덱스 검증
    if validate_index:
        if not validate_opensearch_index(opensearch_client, index_name):
            return
    
    # Bedrock 임베딩 클라이언트 초기화
    embedder = BedrockEmbedding()
    print("✅ Bedrock Embedding 클라이언트 초기화 완료")
    
    # Neptune에서 요약된 엔티티 조회
    print("📊 Neptune에서 엔티티 데이터 조회 중...")
    result = get_summarized_entities_from_neptune()
    
    if not result or 'results' not in result or not result['results']:
        print("❌ Neptune에서 요약된 엔티티를 찾을 수 없습니다")
        return
    
    entities = result['results']
    total = len(entities)
    print(f"📋 총 {total}개 엔티티 발견")
    
    # 엔티티 저장
    updated_count = 0
    not_found_count = 0
    failed_count = 0
    
    for i, entity in enumerate(entities, 1):
        name = entity['name']
        entity_type = entity['entity_type'][0] if entity['entity_type'] else 'UNKNOWN'
        summary = entity['summary']
        neptune_id = entity['neptune_id']
        
        # 진행률 표시
        if i % 10 == 0 or i == total:
            print(f"📈 진행률: {i}/{total} ({i/total*100:.1f}%)")
        
        # OpenSearch에서 name과 entity_type으로 exact match 검색
        existing = find_entity_by_name_exact(opensearch_client, index_name, name, entity_type)
        
        if not existing:
            print(f"   ⏭️ 존재하지 않음 (건너뜀): {name} ({entity_type})")
            not_found_count += 1
            continue
        
        # 임베딩 생성
        summary_vec = embedder.embed_text(summary)
        
        # 벡터 검증
        if not isinstance(summary_vec, list) or len(summary_vec) != 1024:
            print(f"   ❌ 벡터 오류: {name}")
            failed_count += 1
            continue
        
        # 업데이트
        success = update_entity_summary(
            opensearch_client, index_name, existing['id'], 
            summary, summary_vec, neptune_id
        )
        
        if success:
            updated_count += 1
        else:
            failed_count += 1
    
    # 최종 refresh
    refresh_opensearch_index(opensearch_client, index_name)
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("🎉 Entity to OpenSearch Complete!")
    print("=" * 60)
    print(f"✅ 업데이트 성공: {updated_count}개")
    print(f"⏭️ 존재하지 않아 건너뜀: {not_found_count}개")
    print(f"❌ 실패: {failed_count}개")
    print(f"📊 총 처리: {total}개")
    
    return {
        "updated": updated_count,
        "not_found": not_found_count,
        "failed": failed_count,
        "total": total
    }


if __name__ == "__main__":
    run_entity_to_opensearch()
