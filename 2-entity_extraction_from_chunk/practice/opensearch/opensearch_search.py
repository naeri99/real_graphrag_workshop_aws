"""
OpenSearch 엔티티 검색 유틸리티
"""
from typing import Dict, List, Tuple
from opensearch.opensearch_con import get_opensearch_client
from utils.bedrock_embedding import BedrockEmbedding


# 전역 임베딩 클라이언트
_embedder = None


def delete_chunk_index_opensearch(index_name: str = "chunks"):
    """
    chunks 인덱스의 모든 문서 삭제 (인덱스는 유지)
    """
    try:
        client = get_opensearch_client()
        
        # delete_by_query로 모든 문서 삭제
        response = client.delete_by_query(
            index=index_name,
            body={"query": {"match_all": {}}},
            refresh=True
        )
        
        deleted = response.get('deleted', 0)
        print(f"🗑️ OpenSearch chunks 삭제: {deleted}개")
        return response
        
    except Exception as e:
        print(f"❌ Chunks 삭제 오류: {e}")
        return None

def get_embedder():
    """Bedrock 임베딩 클라이언트 싱글톤"""
    global _embedder
    if _embedder is None:
        _embedder = BedrockEmbedding()
    return _embedder


def save_chunk_to_opensearch(chunk_hash: str, chunk_id: str, text: str, index_name: str = "chunks"):
    """
    청크를 OpenSearch에 저장 (텍스트 + 벡터)
    
    Args:
        chunk_id: 청크 ID (neptune_id로 사용)
        text: 청크 텍스트
        index_name: 인덱스 이름
    """
    try:
        client = get_opensearch_client()
        embedder = get_embedder()
        
        # 텍스트를 벡터로 변환
        context_vec = embedder.embed_text(text)
        
        # 문서 생성
        doc = {
            "chunk": {
                "context": text,
                "context_vec": context_vec,
                "neptune_id": chunk_id
            }
        }
        
        # OpenSearch에 저장 (chunk_id를 문서 ID로 사용)
        response = client.index(
            index=index_name,
            id=chunk_hash,
            body=doc,
            refresh=False
        )
        
        print(f"   📦 Chunk saved to OpenSearch: {chunk_hash}")
        return response
        
    except Exception as e:
        print(f"   ❌ Chunk 저장 오류: {e}")
        return None


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
        # 1. 정확한 이름 매칭 시도
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
            "min_score": 3.4,
            "_source": ["entity.name"]
        }
        
        response = opensearch_client.search(index=index_name, body=exact_search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        if hits:
            exact_name = hits[0]['_source'].get('entity', {}).get('name', entity_name).strip()
            return exact_name, True, 'name_exact'


        # 2. 해당 타입의 모든 엔티티에서 동의어 검색
        exact_synonym_search = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"entity.entity_type": entity_type}},
                        {"term": {"entity.synonym": entity_name}}
                    ]
                }
            },
            "size": 1,
            "_source": {
                "excludes": ["entity.summary", "entity.summary_vec"]
            }
        }

        response = opensearch_client.search(index=index_name, body=exact_synonym_search)
        hits = response.get('hits', {}).get('hits', [])

        if hits:
            entity_real_name = hits[0]['_source']['entity']['name'].strip()
            return entity_real_name, True, 'synonym_exact'
        
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


def resolve_relationships(relationships: List[Dict], opensearch_client=None, index_name: str = "entities") -> Tuple[List[Dict], Dict]:
    """
    관계 리스트의 엔티티 이름들을 OpenSearch를 통해 정확한 이름으로 변환합니다.
    
    Args:
        relationships: 관계 리스트
        opensearch_client: OpenSearch 클라이언트
        index_name: 검색할 인덱스 이름
        
    Returns:
        tuple: (해결된 관계 리스트, 메트릭)
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not relationships:
        return [], {'source_matched': 0, 'target_matched': 0, 'source_new': 0, 'target_new': 0}
    
    resolved = []
    metrics = {'source_matched': 0, 'target_matched': 0, 'source_new': 0, 'target_new': 0}
    
    for rel in relationships:
        updated = rel.copy()
        
        # source_entity 처리
        source_name = rel.get('source_entity', '').strip()
        source_type = rel.get('source_type', '').strip()
        
        if source_name and source_type:
            resolved_source, found, _ = search_entity_in_opensearch(
                source_name, source_type, opensearch_client, index_name
            )
            updated['source_entity'] = resolved_source
            updated['_source_original'] = source_name
            updated['_source_matched'] = found
            
            if found:
                metrics['source_matched'] += 1
            else:
                metrics['source_new'] += 1
        
        # target_entity 처리
        target_name = rel.get('target_entity', '').strip()
        target_type = rel.get('target_type', '').strip()
        
        if target_name and target_type:
            resolved_target, found, _ = search_entity_in_opensearch(
                target_name, target_type, opensearch_client, index_name
            )
            updated['target_entity'] = resolved_target
            updated['_target_original'] = target_name
            updated['_target_matched'] = found
            
            if found:
                metrics['target_matched'] += 1
            else:
                metrics['target_new'] += 1
        
        resolved.append(updated)
    
    return resolved, metrics
