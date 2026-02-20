"""
Chunk 기반 엔티티 검색 + 1-hop 관계 탐색 + Strands Agent 답변 생성
"""
from neptune.neptune_con import execute_cypher
from opensearch.opensearch_con import get_opensearch_client
from utils.bedrock_embedding import BedrockEmbedding
from strands import Agent


# 전역 임베딩 클라이언트
_embedder = None


def get_embedder():
    """Bedrock 임베딩 클라이언트 싱글톤"""
    global _embedder
    if _embedder is None:
        _embedder = BedrockEmbedding()
    return _embedder


def search_chunks_by_query(query: str, k: int = 5) -> list:
    """유저 질의를 벡터화하여 OpenSearch chunks 인덱스에서 유사한 청크 검색"""
    embedder = get_embedder()
    query_vector = embedder.embed_text(query)
    client = get_opensearch_client()
    
    search_body = {
        "size": k,
        "query": {
            "knn": {
                "chunk.context_vec": {
                    "vector": query_vector,
                    "k": k
                }
            }
        },
        "_source": ["chunk.context", "chunk.neptune_id"]
    }
    
    try:
        response = client.search(index="chunks", body=search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        results = []
        for hit in hits:
            chunk = hit['_source'].get('chunk', {})
            results.append({
                'doc_id': hit['_id'],
                'neptune_id': chunk.get('neptune_id'),
                'context': chunk.get('context'),
                'score': hit.get('_score', 0)
            })
        return results
    except Exception as e:
        print(f"❌ 청크 검색 오류: {e}")
        return []


def get_entities_by_chunk_id(chunk_id: str) -> list:
    """청크 ID로 연결된 엔티티들 조회"""
    query = """
    MATCH (c:__Chunk__ {id: $chunk_id})-[:MENTIONS]->(e)
    RETURN e.name AS name, labels(e) AS entity_type, e.description AS description, e.neptune_id AS neptune_id
    """
    result = execute_cypher(query, chunk_id=chunk_id)
    return result.get('results', []) if result else []


def get_1hop_relationships(entity_name: str) -> list:
    """엔티티의 1-hop 관계 조회"""
    query = """
    MATCH (e {name: $entity_name})-[r]-(related)
    WHERE NOT related:__Chunk__ AND NOT related:REVIEWER
    RETURN e.name AS source, type(r) AS relationship, related.name AS target, 
           labels(related) AS target_type, related.description AS target_description,
           r.description AS rel_description
    """
    result = execute_cypher(query, entity_name=entity_name)
    return result.get('results', []) if result else []


def build_context_for_agent(query: str, chunks: list, entities: list, relationships: list) -> str:
    """Agent에게 전달할 컨텍스트 구성"""
    context = f"## 사용자 질문\n{query}\n\n"
    
    # 청크 컨텍스트
    context += "## 관련 텍스트\n"
    for i, chunk in enumerate(chunks[:3], 1):
        ctx = chunk.get('context', '')[:500]
        context += f"{i}. {ctx}...\n\n"
    
    # 엔티티 정보
    context += "## 관련 엔티티\n"
    for e in entities:
        etype = e.get('entity_type', [])
        etype = etype[0] if isinstance(etype, list) and etype else etype
        desc = e.get('description', '')
        if desc:
            context += f"- {e.get('name')} ({etype}): {desc[:200]}\n"
        else:
            context += f"- {e.get('name')} ({etype})\n"
    
    # 관계 정보
    if relationships:
        context += "\n## 엔티티 관계\n"
        for rel in relationships[:20]:
            target_type = rel.get('target_type', [])
            target_type = target_type[0] if isinstance(target_type, list) and target_type else target_type
            rel_desc = rel.get('rel_description', '')
            if rel_desc:
                context += f"- {rel.get('source')} --[{rel.get('relationship')}]--> {rel.get('target')} ({target_type}): {rel_desc[:100]}\n"
            else:
                context += f"- {rel.get('source')} --[{rel.get('relationship')}]--> {rel.get('target')} ({target_type})\n"
    
    return context

