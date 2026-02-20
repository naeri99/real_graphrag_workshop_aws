"""
Movie Search Neptune + Agentic Entity
- 사용자 쿼리에서 엔티티 추출
- OpenSearch로 엔티티 이름 해결
- Cypher 쿼리 실행
- 결과에서 엔티티 타입 확인 → prompt 있으면 Agent, 없으면 데이터 리턴
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


def get_entities_with_prompt(entity_names: list) -> dict:
    """여러 엔티티의 타입과 prompt 조회"""
    if not entity_names:
        return {}
    
    # IN 절로 한번에 조회
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


def extract_entity_names_from_cypher_result(cypher_result: dict) -> list:
    """Cypher 결과에서 엔티티 이름들 추출"""
    entity_names = set()
    if cypher_result and cypher_result.get('results'):
        for row in cypher_result['results']:
            for key, value in row.items():
                # actor_name, character 등 이름 관련 필드만 추출
                if isinstance(value, str) and len(value) > 1:
                    # description, summary 등 긴 텍스트는 제외
                    if 'description' not in key and 'summary' not in key and 'action' not in key:
                        entity_names.add(value)
    return list(entity_names)


def get_actors_for_characters(character_names: list) -> list:
    """캐릭터와 연결된 ACTOR 이름들 조회"""
    if not character_names:
        return []
    
    # 방향 무관하게 조회 (ACTOR-MOVIE_CHARACTER 관계)
    query = """
    MATCH (actor:ACTOR)-[:RELATIONSHIP]-(char:MOVIE_CHARACTER)
    WHERE char.name IN $names
    RETURN DISTINCT actor.name AS actor_name
    """
    result = execute_cypher(query, names=character_names)
    
    actors = []
    if result and result.get('results'):
        for row in result['results']:
            actor_name = row.get('actor_name')
            if actor_name:
                actors.append(actor_name)
    return actors


