"""
Movie Search Neptune + Agentic Entity
- 사용자 쿼리에서 엔티티 추출
- OpenSearch로 엔티티 이름 해결
- ACTOR이고 prompt 있으면 → Strands Agent 사용
- 아니면 → 기존 Cypher 쿼리 실행
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from utils.smart_search_llm import SmartGraphSearchLLM
from utils.generate_entity import extract_entity_from_search
from utils.parse_utils import parse_search_context
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import resolve_entities_with_opensearch
from neptune.neptune_con import execute_cypher
from actor_tools import search_neptune, search_web
from strands import Agent

_executor = ThreadPoolExecutor(max_workers=5)


def get_entity_info(entity_name: str) -> dict:
    """엔티티의 타입과 prompt 조회"""
    query = """
    MATCH (e {name: $name})
    RETURN e.name AS name, labels(e) AS entity_type, e.prompt AS prompt, e.description AS description
    LIMIT 1
    """
    result = execute_cypher(query, name=entity_name)
    if result and result.get('results'):
        return result['results'][0]
    return None


def process_agentic_entity(entity_name: str, entity_prompt: str, user_query: str) -> dict:
    """prompt가 있는 ACTOR를 Strands Agent로 처리"""
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


async def search_with_entity_agent_async(query: str):
    """엔티티 추출 → ACTOR면 Agent, 아니면 Cypher"""
    search = SmartGraphSearchLLM()
    opensearch_client = get_opensearch_client()
    
    print(f"\n{'='*80}")
    print(f"🔍 질의: {query}")
    print('='*80)
    
    # 1단계: 쿼리에서 엔티티 추출
    print("\n� 1단계: 엔티티 추출...")
    try:
        result = extract_entity_from_search({"user_query": query})
        entities = parse_search_context(result)
        print(f"✅ 추출된 엔티티: {entities}")
        
        if not entities:
            print("⚠️ 추출된 엔티티가 없습니다.")
            return None
    except Exception as e:
        print(f"❌ 엔티티 추출 오류: {e}")
        return None
    
    # 2단계: OpenSearch로 엔티티 이름 해결
    print(f"\n🔄 2단계: OpenSearch로 엔티티 이름 해결...")
    try:
        resolved_mapping = resolve_entities_with_opensearch(entities, opensearch_client)
        print(f"✅ 이름 해결 완료:")
        for original, resolved in resolved_mapping.items():
            print(f"   '{original}' → '{resolved}'")
    except Exception as e:
        print(f"❌ 이름 해결 오류: {e}")
        resolved_mapping = {entity: entity for entity in entities}
    
    # 3단계: 엔티티 타입 확인 및 분류
    print(f"\n📊 3단계: 엔티티 타입 확인...")
    agentic_list = []  # ACTOR + prompt 있는 것
    normal_entities = []  # 나머지
    
    for original, resolved in resolved_mapping.items():
        info = get_entity_info(resolved)
        if info:
            entity_type = info.get('entity_type', [])
            entity_type = entity_type[0] if isinstance(entity_type, list) and entity_type else entity_type
            prompt = info.get('prompt')
            
            print(f"   {resolved}: {entity_type}" + (" (Agentic)" if prompt else ""))
            
            if entity_type == 'ACTOR' and prompt:
                agentic_list.append({'name': resolved, 'prompt': prompt})
            else:
                normal_entities.append(resolved)
        else:
            print(f"   {resolved}: 정보 없음")
            normal_entities.append(resolved)
    
    # 4단계: Agentic 엔티티 처리 (병렬)
    agentic_results = []
    if agentic_list:
        print(f"\n🤖 4단계: Agentic 엔티티 처리...")
        agentic_results = await process_agentic_entities_parallel(agentic_list, query)
    
    # 5단계: 나머지 엔티티는 Cypher 쿼리로 처리
    cypher_result = None
    if normal_entities:
        print(f"\n🚀 5단계: Cypher 쿼리 실행...")
        updated_query = query
        for original, resolved in resolved_mapping.items():
            if original != resolved:
                updated_query = updated_query.replace(original, resolved)
        
        try:
            cypher_result = search.smart_search(updated_query)
            if cypher_result['success']:
                print(f"✅ Cypher 결과: {cypher_result['results_count']}개")
            else:
                print(f"❌ Cypher 오류: {cypher_result.get('error')}")
        except Exception as e:
            print(f"❌ Cypher 실행 오류: {e}")
    
    # 6단계: 최종 답변 생성
    print(f"\n📝 6단계: 최종 답변 생성...")
    
    context = f"## 사용자 질문\n{query}\n\n"
    
    if agentic_results:
        context += "## 배우 정보 (Agentic)\n"
        for ar in agentic_results:
            if ar.get('success'):
                context += f"### {ar['entity']}\n{ar['result']}\n\n"
    
    if cypher_result and cypher_result.get('success'):
        context += f"## 그래프 검색 결과\n{cypher_result.get('summary', '')}\n"
    
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
        'entities': list(resolved_mapping.values()),
        'agentic_results': agentic_results,
        'cypher_result': cypher_result,
        'answer': response.message
    }


def search_with_entity_agent(query: str):
    """동기 래퍼"""
    return asyncio.run(search_with_entity_agent_async(query))


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("사용법: python movie_search_neptune_entity_agent.py <질의>")
        print("예시: python movie_search_neptune_entity_agent.py '전지현의 최신 근황과 암살에서 역할은?'")
        sys.exit(1)
    
    search_with_entity_agent(sys.argv[1])
