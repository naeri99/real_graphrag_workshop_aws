"""
ACTOR 엔티티에 prompt 프로퍼티 추가
- 1-hop 검색 시 Strands Agent에 직접 주입할 프롬프트
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neptune.neptune_con import execute_cypher


def get_all_actors() -> list:
    """모든 ACTOR 엔티티 조회"""
    query = """
    MATCH (a:ACTOR)
    RETURN a.name AS name, a.neptune_id AS neptune_id, a.prompt AS prompt
    """
    result = execute_cypher(query)
    return result.get('results', []) if result else []


DEFAULT_ACTOR_PROMPT = """이 배우({name})에 대한 질문 처리 규칙:
- 출연작, 배역, 영화 관계 질문 → search_neptune 도구만 사용
- "최신", "근황", "수상", "뉴스", "현재", "요즘" 키워드가 있을 때만 → search_web 도구 사용
- 키워드가 없으면 절대 웹 검색하지 말고 Neptune 데이터만 사용"""


def add_prompt_to_actor(actor_name: str, prompt: str = None):
    """
    ACTOR 엔티티에 prompt 프로퍼티 추가
    
    Args:
        actor_name: 배우 이름
        prompt: Agent에 주입할 프롬프트 ({name}은 배우 이름으로 치환됨)
    """
    if prompt is None:
        prompt = DEFAULT_ACTOR_PROMPT
    
    query = """
    MATCH (a:ACTOR {name: $actor_name})
    SET a.prompt = $prompt
    RETURN a.name AS name, a.prompt AS prompt
    """
    result = execute_cypher(query, actor_name=actor_name, prompt=prompt)
    return result


def add_prompt_to_all_actors(prompt: str = None, force_update: bool = False):
    """모든 ACTOR에 prompt 프로퍼티 추가
    
    Args:
        prompt: 사용할 프롬프트 (None이면 DEFAULT_ACTOR_PROMPT 사용)
        force_update: True면 기존 prompt도 덮어쓰기
    """
    actors = get_all_actors()
    print(f"📊 총 {len(actors)}명의 배우 발견")
    
    updated = 0
    for actor in actors:
        name = actor.get('name')
        existing_prompt = actor.get('prompt')
        
        if existing_prompt and not force_update:
            print(f"  ⏭️ {name}: 이미 prompt 있음")
            continue
        
        result = add_prompt_to_actor(name, prompt)
        if result:
            print(f"  ✅ {name}: prompt 추가됨")
            updated += 1
        else:
            print(f"  ❌ {name}: 실패")
    
    print(f"\n🎉 완료: {updated}명 업데이트")
    return updated


def update_actor_prompt(actor_name: str, prompt: str):
    """특정 ACTOR의 prompt 업데이트 (덮어쓰기)"""
    query = """
    MATCH (a:ACTOR {name: $actor_name})
    SET a.prompt = $prompt
    RETURN a.name AS name, a.prompt AS prompt
    """
    result = execute_cypher(query, actor_name=actor_name, prompt=prompt)
    if result and result.get('results'):
        print(f"✅ {actor_name}: prompt 업데이트됨")
    else:
        print(f"❌ {actor_name}: 업데이트 실패")
    return result


def verify_actor_prompts():
    """ACTOR의 prompt 프로퍼티 확인"""
    query = """
    MATCH (a:ACTOR)
    WHERE a.prompt IS NOT NULL
    RETURN a.name AS name, a.prompt AS prompt
    LIMIT 5
    """
    result = execute_cypher(query)
    actors = result.get('results', []) if result else []
    
    print(f"📋 prompt가 있는 ACTOR (상위 5명):")
    for actor in actors:
        print(f"  - {actor.get('name')}")
        print(f"    prompt: {actor.get('prompt')[:80]}...")
    
    return actors


if __name__ == "__main__":
    print("=" * 60)
    print("🎬 ACTOR 엔티티에 prompt 프로퍼티 업데이트")
    print("=" * 60)
    
    # 모든 ACTOR에 새 프롬프트로 강제 업데이트
    add_prompt_to_all_actors(force_update=True)
    
    print("\n" + "=" * 60)
    print("🔍 검증")
    print("=" * 60)
    verify_actor_prompts()
