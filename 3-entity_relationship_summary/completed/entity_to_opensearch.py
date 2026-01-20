"""
3단계: Entity to OpenSearch
- Neptune에서 요약된 엔티티 조회
- Bedrock으로 임베딩 생성
- OpenSearch에 저장
"""
from neptune.cyper_queries import execute_cypher
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import (
    validate_opensearch_index,
    save_entity_to_opensearch,
    refresh_opensearch_index
)
from utils.bedrock_embedding import BedrockEmbedding


def get_summarized_entities_from_neptune():
    """Neptune에서 요약이 완료된 엔티티들 조회"""
    query = """
    MATCH (n)
    WHERE n.name IS NOT NULL 
      AND n.summary IS NOT NULL 
      AND n.neptune_id IS NOT NULL
      AND NOT n:__Chunk__ 
      AND NOT n:MOVIE 
      AND NOT n:REVIEWER
    RETURN n.name AS name, labels(n) AS entity_type, n.summary AS summary, n.neptune_id AS neptune_id
    ORDER BY n.name
    """
    return execute_cypher(query)


def run_entity_to_opensearch(index_name="entities", validate_index=True):
    """
    Entity to OpenSearch 실행
    1. Neptune에서 요약된 엔티티 조회
    2. Bedrock으로 임베딩 생성
    3. OpenSearch에 저장
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
    saved_count = 0
    failed_count = 0
    skipped_count = 0
    
    for i, entity in enumerate(entities, 1):
        name = entity['name']
        entity_type = entity['entity_type'][0] if entity['entity_type'] else 'UNKNOWN'
        summary = entity['summary']
        neptune_id = entity['neptune_id']
        
        # 진행률 표시
        if i % 10 == 0 or i == total:
            print(f"📈 진행률: {i}/{total} ({i/total*100:.1f}%)")
        
        # OpenSearch에 저장
        result = save_entity_to_opensearch(
            opensearch_client=opensearch_client,
            index_name=index_name,
            embedder=embedder,
            name=name,
            entity_type=entity_type,
            summary=summary,
            neptune_id=neptune_id
        )
        
        if result == "saved":
            saved_count += 1
        elif result == "skipped":
            skipped_count += 1
        else:
            failed_count += 1
    
    # 최종 refresh
    refresh_opensearch_index(opensearch_client, index_name)
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("🎉 Entity to OpenSearch Complete!")
    print("=" * 60)
    print(f"✅ 성공적으로 저장: {saved_count}개")
    print(f"⏭️ 이미 존재하여 건너뜀: {skipped_count}개")
    print(f"❌ 실패: {failed_count}개")
    print(f"📊 총 처리: {total}개")
    
    return {
        "saved": saved_count,
        "failed": failed_count,
        "skipped": skipped_count,
        "total": total
    }


if __name__ == "__main__":
    run_entity_to_opensearch()
