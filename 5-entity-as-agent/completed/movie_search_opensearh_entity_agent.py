"""
OpenSearch Chunk 기반 엔티티 검색 + Agentic Entity
순서: 청크 검색 → 엔티티 추출 → Cypher 쿼리 → prompt 확인 → Agent or 데이터 리턴
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
    """OpenSearch에서 유사한 청크 검색"""
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


def get_entities_from_chunk(chunk_id: str) -> list:
    """청크에서 엔티티만 추출 (1단계용)"""
    query = """
    MATCH (c:__Chunk__ {id: $chunk_id})-[:MENTIONS]->(e)
    RETURN DISTINCT e.name AS entity_name, labels(e) AS entity_type, e.description AS entity_desc
    """
    result = execute_cypher(query, chunk_id=chunk_id)
    rows = result.get('results', []) if result else []
    
    entities = []
    for row in rows:
        e_name = row.get('entity_name')
        if e_name:
            entities.append({
                'name': e_name,
                'entity_type': row.get('entity_type'),
                'description': row.get('entity_desc')
            })
    return entities


def get_2hop_relationships(entity_names: list) -> dict:
    """엔티티들의 2-hop 관계 조회 (2단계용)"""
    if not entity_names:
        return {'entities': [], 'relationships': []}
    
    query = """
    MATCH (e)
    WHERE e.name IN $names
    OPTIONAL MATCH (e)-[r1]-(hop1)
    WHERE NOT hop1:__Chunk__ AND NOT hop1:REVIEWER
    OPTIONAL MATCH (hop1)-[r2]-(hop2)
    WHERE NOT hop2:__Chunk__ AND NOT hop2:REVIEWER AND hop2 <> e
    RETURN e.name AS entity_name, labels(e) AS entity_type, e.description AS entity_desc,
           type(r1) AS rel1, hop1.name AS hop1_name, labels(hop1) AS hop1_type, 
           hop1.description AS hop1_desc,
           type(r2) AS rel2, hop2.name AS hop2_name, labels(hop2) AS hop2_type,
           hop2.description AS hop2_desc
    LIMIT 200
    """
    result = execute_cypher(query, names=entity_names)
    rows = result.get('results', []) if result else []
    
    entities, relationships = {}, []
    
    for row in rows:
        e_name = row.get('entity_name')
        if e_name and e_name not in entities:
            entities[e_name] = {
                'name': e_name, 
                'entity_type': row.get('entity_type'), 
                'description': row.get('entity_desc')
            }
        
        hop1_name = row.get('hop1_name')
        if hop1_name and hop1_name not in entities:
            entities[hop1_name] = {
                'name': hop1_name, 
                'entity_type': row.get('hop1_type'), 
                'description': row.get('hop1_desc')
            }
        
        hop2_name = row.get('hop2_name')
        if hop2_name and hop2_name not in entities:
            entities[hop2_name] = {
                'name': hop2_name, 
                'entity_type': row.get('hop2_type'), 
                'description': row.get('hop2_desc')
            }
        
        if e_name and hop1_name and row.get('rel1'):
            relationships.append({
                'source': e_name, 
                'rel': row.get('rel1'), 
                'target': hop1_name, 
                'target_type': row.get('hop1_type')
            })
        if hop1_name and hop2_name and row.get('rel2'):
            relationships.append({
                'source': hop1_name, 
                'rel': row.get('rel2'), 
                'target': hop2_name, 
                'target_type': row.get('hop2_type')
            })
    
    return {'entities': list(entities.values()), 'relationships': relationships}


def get_entities_with_prompt(entity_names: list) -> dict:
    """여러 엔티티의 prompt 조회"""
    if not entity_names:
        return {}
    
    query = """
    MATCH (e)
    WHERE e.name IN $names
    RETURN e.name AS name, labels(e) AS entity_type, e.prompt AS prompt
    """
    result = execute_cypher(query, names=entity_names)
    
    entities_info = {}
    if result and result.get('results'):
        for row in result['results']:
            name = row.get('name')
            entity_type = row.get('entity_type', [])
            entity_type = entity_type[0] if isinstance(entity_type, list) and entity_type else entity_type
            entities_info[name] = {
                'type': entity_type,
                'prompt': row.get('prompt')
            }
    return entities_info


def process_agentic_entity(entity_name: str, entity_prompt: str, user_query: str) -> dict:
    """prompt가 있는 엔티티를 Strands Agent로 처리"""
    prompt_filled = entity_prompt.replace('{name}', entity_name)
    print(f"    🤖 [Agentic] {entity_name}")
    try:
        agent = Agent(
            system_prompt=f"당신은 배우 정보 전문가입니다.\n{prompt_filled}\n한국어로 답변해주세요.",
            tools=[search_neptune, search_web]
        )
        response = agent(f"배우 '{entity_name}'에 대해 답변해주세요. 유저 질문: {user_query}")
        result = response.message if hasattr(response, 'message') else str(response)
        return {'entity': entity_name, 'result': result, 'success': True}
    except Exception as e:
        return {'entity': entity_name, 'result': f"오류: {e}", 'success': False}


async def process_agentic_entities_parallel(agentic_list: list, user_query: str) -> list:
    """여러 Agentic 엔티티를 병렬로 처리"""
    if not agentic_list:
        return []
    
    print(f"    📋 Agentic 대상 목록:")
    for item in agentic_list:
        print(f"       - {item['name']}")
    
    loop = asyncio.get_event_loop()
    tasks = [
        loop.run_in_executor(_executor, process_agentic_entity, item['name'], item['prompt'], user_query)
        for item in agentic_list
    ]
    print(f"    ⚡ {len(tasks)}개 Agentic 엔티티 병렬 처리 시작...")
    results = await asyncio.gather(*tasks)
    successful = [r for r in results if r.get('success')]
    print(f"    ✅ {len(successful)}/{len(results)}개 완료")
    return list(results)


def build_context(query: str, chunks: list, entities: list, relationships: list, agentic_results: list) -> str:
    """최종 답변용 컨텍스트 구성"""
    context = f"## 사용자 질문\n{query}\n\n"
    
    # 관련 텍스트
    context += "## 관련 텍스트\n"
    for i, chunk in enumerate(chunks[:3], 1):
        context += f"{i}. {chunk.get('context', '')[:500]}...\n\n"
    
    # 엔티티 정보 (prompt 없는 것들)
    context += "## 관련 엔티티\n"
    for e in entities:
        etype = e.get('entity_type', [])
        etype = etype[0] if isinstance(etype, list) and etype else etype
        desc = e.get('description', '')
        context += f"- {e.get('name')} ({etype}){': ' + desc[:200] if desc else ''}\n"
    
    # 관계 정보
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
    
    # Agentic 결과 (배우 최신 정보)
    if agentic_results:
        context += "\n## 배우 정보 (Agentic)\n"
        for ar in agentic_results:
            if ar.get('success'):
                context += f"### {ar['entity']}\n{ar['result']}\n\n"
    
    return context



async def search_and_answer_async(query: str, k: int = 5):
    """
    새로운 순서:
    1. 청크 검색 → 엔티티 추출
    2. Cypher 쿼리 (2-hop 관계)
    3. 엔티티 prompt 확인 → prompt 있으면 Agent, 없으면 데이터만 리턴
    """
    print(f"\n{'='*80}")
    print(f"🔍 질의: {query}")
    print('='*80)
    
    # 1단계: OpenSearch 청크 검색 → 엔티티 추출
    print("\n📦 1단계: OpenSearch 청크 검색 + 엔티티 추출...")
    chunks = search_chunks_by_query(query, k)
    if not chunks:
        print("❌ 관련 청크를 찾을 수 없습니다.")
        return None
    print(f"✅ {len(chunks)}개 청크 발견")
    
    # 청크에서 엔티티 추출
    chunk_entities = {}
    for chunk in chunks:
        entities = get_entities_from_chunk(chunk['neptune_id'])
        for e in entities:
            chunk_entities[e['name']] = e
    
    chunk_entity_names = list(chunk_entities.keys())
    print(f"✅ 청크에서 {len(chunk_entity_names)}개 엔티티 추출")
    for name in chunk_entity_names[:10]:
        print(f"   - {name}")
    if len(chunk_entity_names) > 10:
        print(f"   ... 외 {len(chunk_entity_names) - 10}개")
    
    # 2단계: Cypher 쿼리 (2-hop 관계)
    print("\n🚀 2단계: Cypher 쿼리 (2-hop 관계)...")
    hop_data = get_2hop_relationships(chunk_entity_names)
    all_entities = {e['name']: e for e in hop_data['entities']}
    all_relationships = hop_data['relationships']
    
    entities_list = list(all_entities.values())
    print(f"✅ {len(entities_list)}개 엔티티, {len(all_relationships)}개 관계")
    
    # 3단계: 엔티티 prompt 확인
    print("\n📊 3단계: 엔티티 prompt 확인...")
    entity_names = [e['name'] for e in entities_list]
    entities_info = get_entities_with_prompt(entity_names)
    
    # prompt 있는 것과 없는 것 분류
    agentic_list = []
    normal_entities = []
    
    for e in entities_list:
        name = e['name']
        info = entities_info.get(name, {})
        if info.get('prompt'):
            print(f"   {name}: {info.get('type', '?')} (Agentic ✓)")
            agentic_list.append({'name': name, 'prompt': info['prompt']})
        else:
            normal_entities.append(e)
    
    print(f"   → Agentic: {len(agentic_list)}개, Normal: {len(normal_entities)}개")
    
    # prompt가 없으면 데이터만 리턴 (Agent 호출 없음)
    if not agentic_list:
        print("\n✅ prompt가 있는 엔티티 없음 → 데이터만 리턴")
        return {
            'query': query,
            'chunks': chunks,
            'entities': entities_list,
            'relationships': all_relationships,
            'agentic_results': [],
            'answer': None,
            'mode': 'data_only'
        }
    
    # 4단계: Agentic 엔티티 처리 (병렬) - prompt가 있는 경우만
    print(f"\n🤖 4단계: Agentic 엔티티 처리...")
    agentic_results = await process_agentic_entities_parallel(agentic_list, query)
    
    # 5단계: 최종 답변 생성
    print(f"\n📝 5단계: 최종 답변 생성...")
    context = build_context(query, chunks, normal_entities, all_relationships, agentic_results)
    
    final_agent = Agent(
        system_prompt="당신은 영화 정보 전문가입니다. 주어진 컨텍스트를 바탕으로 사용자의 질문에 정확하고 상세하게 답변해주세요. 한국어로 답변해주세요."
    )
    response = final_agent(f"{context}\n\n질문: {query}\n\n답변:")
    
    print("\n" + "=" * 80)
    print("📝 답변:")
    print("=" * 80)
    print(response.message)
    
    return {
        'query': query,
        'chunks': chunks,
        'entities': entities_list,
        'relationships': all_relationships,
        'agentic_results': agentic_results,
        'answer': response.message,
        'mode': 'agentic'
    }


def search_and_answer(query: str, k: int = 5):
    """동기 래퍼"""
    return asyncio.run(search_and_answer_async(query, k))


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("사용법: python movie_search_opensearh_entity_agent.py <질의>")
        print("예시: python movie_search_opensearh_entity_agent.py '전지현의 최신 근황과 암살에서 역할은?'")
        sys.exit(1)
    
    result = search_and_answer(sys.argv[1])
    
    if result:
        print(f"\n📊 결과 모드: {result.get('mode', 'unknown')}")
        if result['mode'] == 'data_only':
            print("   → Agent 호출 없이 데이터만 리턴됨")
            print(f"   → 엔티티 수: {len(result['entities'])}")
            print(f"   → 관계 수: {len(result['relationships'])}")
