"""
OpenSearch 검색 유틸리티
"""
from typing import List, Optional, Tuple
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
    """텍스트를 임베딩하여 KNN 검색"""
    embedder = get_embedder()
    query_vector = embedder.embed_text(query_text)
    client = get_opensearch_client()
    
    knn_query = {"vector": query_vector, "k": k}
    if entity_type:
        knn_query["filter"] = {"term": {"entity.entity_type": entity_type}}
    
    search_body = {
        "size": k,
        "query": {"knn": {"entity.summary_vec": knn_query}},
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


def search_entity_in_opensearch(
    entity_name: str, 
    opensearch_client=None, 
    index_name: str = "entities"
) -> str:
    """OpenSearch에서 엔티티를 검색하여 정확한 이름을 찾습니다."""
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    entity_name = entity_name.strip() if entity_name else ""
    if not entity_name:
        return entity_name
    
    try:
        # 1. 동의어 필드에서 검색
        search_body = {
            "query": {"match": {"entity.synonym.text": entity_name}},
            "size": 10,
            "_source": ["entity.name", "entity.synonym", "entity.entity_type"]
        }
        
        response = opensearch_client.search(index=index_name, body=search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        if hits:
            return hits[0]['_source'].get('entity', {}).get('name', entity_name).strip()
        
        # 2. 유연한 검색
        flexible_search_body = {
            "query": {
                "bool": {
                    "should": [
                        {"wildcard": {"entity.synonym": f"*{entity_name}*"}},
                        {"match": {"entity.synonym": {"query": entity_name, "fuzziness": "AUTO"}}}
                    ]
                }
            },
            "size": 5,
            "_source": ["entity.name", "entity.synonym"]
        }
        
        response = opensearch_client.search(index=index_name, body=flexible_search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        if hits:
            return hits[0]['_source'].get('entity', {}).get('name', entity_name).strip()
        
        return entity_name
        
    except Exception as e:
        print(f"❌ OpenSearch 검색 오류: {e}")
        return entity_name


def resolve_entities_with_opensearch(entities: list, opensearch_client=None) -> dict:
    """엔티티 리스트를 OpenSearch를 통해 정확한 이름으로 변환합니다."""
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not entities:
        return {}
    
    resolved_mapping = {}
    for entity_name in entities:
        resolved_name = search_entity_in_opensearch(entity_name, opensearch_client)
        resolved_mapping[entity_name] = resolved_name
    
    return resolved_mapping
