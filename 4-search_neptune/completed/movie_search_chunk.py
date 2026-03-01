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
    for i, chunk in enumerate(chunks, 1):
        ctx = chunk.get('context', '')[:1000]
        context += f"{i}. {ctx}\n\n"
    
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
        for rel in relationships[:40]:
            target_type = rel.get('target_type', [])
            target_type = target_type[0] if isinstance(target_type, list) and target_type else target_type
            rel_desc = rel.get('rel_description', '')
            if rel_desc:
                context += f"- {rel.get('source')} --[{rel.get('relationship')}]--> {rel.get('target')} ({target_type}): {rel_desc[:100]}\n"
            else:
                context += f"- {rel.get('source')} --[{rel.get('relationship')}]--> {rel.get('target')} ({target_type})\n"
    
    return context


def search_and_answer(query: str, k: int = 5):
    """
    유저 질의로 청크 검색 → 엔티티 조회 → 1-hop 관계 탐색 → Agent 답변 생성
    """
    print(f"🔍 질의: {query}")
    print("=" * 60)
    
    # 1. OpenSearch에서 유사한 청크 검색
    print("📦 1단계: 유사한 청크 검색...")
    chunks = search_chunks_by_query(query, k)
    
    if not chunks:
        print("❌ 관련 청크를 찾을 수 없습니다.")
        return None
    
    print(f"✅ {len(chunks)}개 청크 발견")
    
    # 2. 청크에서 엔티티 조회
    print("\n📊 2단계: 엔티티 조회...")
    all_entities = []
    
    for chunk in chunks:
        chunk_id = chunk['neptune_id']
        entities = get_entities_by_chunk_id(chunk_id)
        all_entities.extend(entities)
    
    # 중복 제거
    unique_entities = {e['name']: e for e in all_entities}
    entities_list = list(unique_entities.values())
    print(f"✅ {len(entities_list)}개 고유 엔티티 발견")
    
    # 3. 1-hop 관계 탐색
    print("\n🔗 3단계: 1-hop 관계 탐색...")
    all_relationships = []
    
    for entity in entities_list[:10]:  # 상위 10개 엔티티만
        entity_name = entity.get('name')
        if entity_name:
            rels = get_1hop_relationships(entity_name)
            all_relationships.extend(rels)
    
    # 중복 제거
    unique_rels = {}
    for rel in all_relationships:
        key = f"{rel.get('source')}_{rel.get('target')}_{rel.get('relationship')}"
        unique_rels[key] = rel
    
    relationships_list = list(unique_rels.values())
    print(f"✅ {len(relationships_list)}개 관계 발견")
    
    # 4. Agent로 답변 생성
    print("\n🤖 4단계: 답변 생성...")
    context = build_context_for_agent(query, chunks, entities_list, relationships_list)
    
    agent = Agent(
            system_prompt="""당신은 영화 정보 전문가입니다. 
    주어진 컨텍스트를 바탕으로 사용자의 질문에 정확하고 상세하게 답변해주세요.
    컨텍스트에 없는 정보는 추측하지 마세요.
    추천 질문의 경우, 컨텍스트에서 찾을 수 있는 모든 관련 영화를 빠짐없이 소개해주세요.
    각 영화별로 제목, 장르, 간단한 줄거리, 출연 배우 등을 정리해주세요.
    한국어로 답변해주세요."""
    )
    
    prompt = f"""다음 컨텍스트를 바탕으로 질문에 답변해주세요.

    {context}

    질문: {query}

    답변:"""
    
    response = agent(prompt)
    
    print("\n" + "=" * 60)
    print("📝 답변:")
    print("=" * 60)
    print(response.message)
    
    return {
        'query': query,
        'chunks': chunks,
        'entities': entities_list,
        'relationships': relationships_list,
        'answer': response.message
    }


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python movie_search_chunk.py <질의>")
        print("예시: python movie_search_chunk.py '암살에서 안옥윤은 누구야?'")
        sys.exit(1)
    
    query = sys.argv[1]
    search_and_answer(query)

