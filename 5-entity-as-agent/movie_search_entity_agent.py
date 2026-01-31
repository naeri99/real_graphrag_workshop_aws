"""
Chunk 기반 엔티티 검색 + 2-hop 관계 탐색 + Agentic Entity 병렬 처리
- 청크에서 2-hop까지 한번에 조회
- prompt 있는 엔티티 → Strands Agent + tools (비동기 병렬)
- prompt 없는 엔티티 → Neptune 데이터만 사용
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from neptune.neptune_con import execute_cypher
from opensearch.opensearch_con import get_opensearch_client
from utils.bedrock_embedding import BedrockEmbedding
from actor_tools import search_neptune, search_web
from strands import Agent

_embedder = None
_executor = ThreadPoolExecutor(max_workers=5)


def get_embedder():
    global _embedder
    if _embedder is None:
        _embedder = BedrockEmbedding()
    return _embedder


def search_chunks_by_query(query: str, k: int = 5) -> list:
    embedder = get_embedder()
    query_vector = embedder.embed_text(query)
    client = get_opensearch_client()
    
    search_body = {
        "size": k,
        "query": {"knn": {"chunk.context_vec": {"vector": query_vector, "k": k}}},
        "_source": ["chunk.context", "chunk.neptune_id"]
    }
    
    try:
        response = client.search(index="chunks", body=search_body)
        hits = response.get('hits', {}).get('hits', [])
        return [{'doc_id': hit['_id'], 
                 'neptune_id': hit['_source'].get('chunk', {}).get('neptune_id'),
                 'context': hit['_source'].get('chunk', {}).get('context'),
                 'score': hit.get('_score', 0)} for hit in hits]
    except Exception as e:
        print(f"❌ 청크 검색 오류: {e}")
        return []


def get_entities_with_2hop_by_chunk_id(chunk_id: str) -> dict:
    """청크에서 엔티티 + 1-hop + 2-hop 관계를 한번에 조회"""
    query = """
    MATCH (c:__Chunk__ {id: $chunk_id})-[:MENTIONS]->(e)
    OPTIONAL MATCH (e)-[r1]-(hop1)
    WHERE NOT hop1:__Chunk__ AND NOT hop1:REVIEWER
    OPTIONAL MATCH (hop1)-[r2]-(hop2)
    WHERE NOT hop2:__Chunk__ AND NOT hop2:REVIEWER AND hop2 <> e
    RETURN e.name AS entity_name, labels(e) AS entity_type, e.description AS entity_desc,
           e.prompt AS entity_prompt,
           type(r1) AS rel1, hop1.name AS hop1_name, labels(hop1) AS hop1_type, 
           hop1.description AS hop1_desc, hop1.prompt AS hop1_prompt,
           type(r2) AS rel2, hop2.name AS hop2_name, labels(hop2) AS hop2_type,
           hop2.description AS hop2_desc, hop2.prompt AS hop2_prompt
    LIMIT 100
    """
    result = execute_cypher(query, chunk_id=chunk_id)
    rows = result.get('results', []) if result else []
    
    entities, relationships, agentic_candidates = {}, [], {}
    
    for row in rows:
        e_name = row.get('entity_name')
        if e_name and e_name not in entities:
            entities[e_name] = {'name': e_name, 'entity_type': row.get('entity_type'), 'description': row.get('entity_desc')}
            if row.get('entity_prompt'):
                agentic_candidates[e_name] = row.get('entity_prompt')
        
        hop1_name = row.get('hop1_name')
        if hop1_name and hop1_name not in entities:
            entities[hop1_name] = {'name': hop1_name, 'entity_type': row.get('hop1_type'), 'description': row.get('hop1_desc')}
            if row.get('hop1_prompt'):
                agentic_candidates[hop1_name] = row.get('hop1_prompt')
        
        hop2_name = row.get('hop2_name')
        if hop2_name and hop2_name not in entities:
            entities[hop2_name] = {'name': hop2_name, 'entity_type': row.get('hop2_type'), 'description': row.get('hop2_desc')}
            if row.get('hop2_prompt'):
                agentic_candidates[hop2_name] = row.get('hop2_prompt')
        
        if e_name and hop1_name and row.get('rel1'):
            relationships.append({'source': e_name, 'rel': row.get('rel1'), 'target': hop1_name, 'target_type': row.get('hop1_type')})
        if hop1_name and hop2_name and row.get('rel2'):
            relationships.append({'source': hop1_name, 'rel': row.get('rel2'), 'target': hop2_name, 'target_type': row.get('hop2_type')})
    
    return {'entities': list(entities.values()), 'relationships': relationships, 'agentic_candidates': agentic_candidates}


def process_agentic_entity(entity_name: str, entity_prompt: str) -> dict:
    """prompt가 있는 엔티티를 Strands Agent로 처리"""
    prompt_filled = entity_prompt.replace('{name}', entity_name)
    try:
        agent = Agent(
            system_prompt=f"당신은 배우 정보 전문가입니다.\n{prompt_filled}\n배우의 최신 정보만 간단히 검색하세요. 한국어로 답변해주세요.",
            tools=[search_neptune, search_web]
        )
        response = agent(f"배우 '{entity_name}'의 최신 근황, 출연작, 수상 이력을 알려줘")
        result = response.message if hasattr(response, 'message') else str(response)
        return {'entity': entity_name, 'result': result, 'success': True}
    except Exception as e:
        return {'entity': entity_name, 'result': f"오류: {e}", 'success': False}


async def process_agentic_entities_parallel(agentic_candidates: dict) -> list:
    """여러 Agentic 엔티티를 병렬로 처리"""
    if not agentic_candidates:
        return []
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(_executor, process_agentic_entity, name, prompt) for name, prompt in agentic_candidates.items()]
    print(f"    ⚡ {len(tasks)}개 Agentic 엔티티 병렬 처리 시작...")
    results = await asyncio.gather(*tasks)
    successful = [r for r in results if r.get('success')]
    print(f"    ✅ {len(successful)}/{len(results)}개 완료")
    return list(results)


def build_context(query: str, chunks: list, entities: list, relationships: list, agentic_results: list) -> str:
    context = f"## 사용자 질문\n{query}\n\n## 관련 텍스트\n"
    for i, chunk in enumerate(chunks[:3], 1):
        context += f"{i}. {chunk.get('context', '')[:500]}...\n\n"
    
    context += "## 관련 엔티티\n"
    for e in entities:
        etype = e.get('entity_type', [])
        etype = etype[0] if isinstance(etype, list) and etype else etype
        desc = e.get('description', '')
        context += f"- {e.get('name')} ({etype}){': ' + desc[:200] if desc else ''}\n"
    
    if relationships:
        context += "\n## 엔티티 관계\n"
        seen = set()
        for rel in relationships[:30]:
            key = f"{rel.get('source')}_{rel.get('target')}"
            if key in seen:
                continue
            seen.add(key)
            ttype = rel.get('target_type', [])
            ttype = ttype[0] if isinstance(ttype, list) and ttype else ttype
            context += f"- {rel.get('source')} --[{rel.get('rel')}]--> {rel.get('target')} ({ttype})\n"
    
    if agentic_results:
        context += "\n## 실시간 정보 (배우 최신 정보)\n"
        for ar in agentic_results:
            if ar.get('success'):
                context += f"### {ar['entity']}\n{ar['result']}\n\n"
    return context


async def search_and_answer_async(query: str, k: int = 5):
    print(f"🔍 질의: {query}")
    print("=" * 60)
    
    print("📦 1단계: 유사한 청크 검색...")
    chunks = search_chunks_by_query(query, k)
    if not chunks:
        print("❌ 관련 청크를 찾을 수 없습니다.")
        return None
    print(f"✅ {len(chunks)}개 청크 발견")
    
    print("\n📊 2단계: 엔티티 + 2-hop 관계 조회...")
    all_entities, all_relationships, all_agentic = {}, [], {}
    for chunk in chunks:
        data = get_entities_with_2hop_by_chunk_id(chunk['neptune_id'])
        for e in data['entities']:
            all_entities[e['name']] = e
        all_relationships.extend(data['relationships'])
        all_agentic.update(data['agentic_candidates'])
    
    entities_list = list(all_entities.values())
    print(f"✅ {len(entities_list)}개 엔티티, {len(all_relationships)}개 관계")
    print(f"   Agentic 후보: {len(all_agentic)}개 (prompt 있는 엔티티)")
    
    print("\n🤖 3단계: Agentic 엔티티 병렬 처리...")
    agentic_results = await process_agentic_entities_parallel(all_agentic)
    
    print("\n📝 4단계: 답변 생성...")
    context = build_context(query, chunks, entities_list, all_relationships, agentic_results)
    agent = Agent(system_prompt="당신은 영화 정보 전문가입니다. 주어진 컨텍스트를 바탕으로 사용자의 질문에 정확하고 상세하게 답변해주세요. 실시간 정보가 있다면 우선적으로 활용하세요. 한국어로 답변해주세요.")
    response = agent(f"{context}\n\n질문: {query}\n\n답변:")
    
    print("\n" + "=" * 60)
    print("📝 답변:")
    print("=" * 60)
    print(response.message)
    return {'query': query, 'chunks': chunks, 'entities': entities_list, 'relationships': all_relationships, 'agentic_results': agentic_results, 'answer': response.message}


def search_and_answer(query: str, k: int = 5):
    return asyncio.run(search_and_answer_async(query, k))


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("사용법: python movie_search_entity_agent.py <질의>")
        sys.exit(1)
    search_and_answer(sys.argv[1])
