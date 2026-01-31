"""
OpenSearch 벡터 검색 유틸리티
"""
from typing import List, Optional
from opensearch.opensearch_con import get_opensearch_client
from utils.bedrock_embedding import BedrockEmbedding


# 전역 임베딩 클라이언트
_embedder = None


def get_embedder():
    """Bedrock 임베딩 클라이언트 싱글톤"""
    global _embedder
    if _embedder is None:
        _embedder = BedrockEmbedding()
    return _embedder


def knn_search(
    query_text: str,
    index_name: str = "entities",
    k: int = 10,
    entity_type: Optional[str] = None
) -> List[dict]:
    """
    텍스트를 임베딩하여 KNN 검색
    
    Args:
        query_text: 검색할 텍스트
        index_name: 검색할 인덱스
        k: 반환할 결과 수
        entity_type: 필터링할 엔티티 타입 (선택)
        
    Returns:
        검색 결과 리스트
    """
    # 텍스트 임베딩
    embedder = get_embedder()
    query_vector = embedder.embed_text(query_text)
    
    # OpenSearch 클라이언트
    client = get_opensearch_client()
    
    # KNN 쿼리 구성
    knn_query = {
        "vector": query_vector,
        "k": k
    }
    
    # entity_type 필터
    if entity_type:
        knn_query["filter"] = {
            "term": {"entity.entity_type": entity_type}
        }
    
    search_body = {
        "size": k,
        "query": {
            "knn": {
                "entity.summary_vec": knn_query
            }
        },
        "_source": ["entity.name", "entity.entity_type", "entity.summary", "entity.neptune_id"]
    }
    
    try:
        response = client.search(index=index_name, body=search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        results = []
        for hit in hits:
            entity = hit['_source'].get('entity', {})
            results.append({
                'name': entity.get('name'),
                'entity_type': entity.get('entity_type'),
                'summary': entity.get('summary'),
                'neptune_id': entity.get('neptune_id'),
                'score': hit.get('_score', 0)
            })
        
        return results
    except Exception as e:
        print(f"❌ KNN 검색 오류: {e}")
        return []
