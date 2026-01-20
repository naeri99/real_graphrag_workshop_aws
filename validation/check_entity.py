"""
Neptune Entity 조회 유틸리티
- 전체 엔티티 조회
- 타입별 엔티티 조회
- 엔티티 상세 정보 조회
"""
import json
from neptune.neptune_con import execute_cypher


def get_all_entities():
    """모든 엔티티 조회 (Chunk, Movie, Reviewer 제외)"""
    query = """
    MATCH (n)
    WHERE NOT n:__Chunk__ AND NOT n:MOVIE AND NOT n:REVIEWER
    RETURN n.name AS name, 
           labels(n) AS entity_type, 
           n.description AS description,
           n.summary AS summary,
           n.neptune_id AS neptune_id
    ORDER BY labels(n), n.name
    """
    return execute_cypher(query)


def get_entities_by_type(entity_type: str):
    """특정 타입의 엔티티만 조회"""
    query = f"""
    MATCH (n:{entity_type})
    RETURN n.name AS name, 
           labels(n) AS entity_type, 
           n.description AS description,
           n.summary AS summary,
           n.neptune_id AS neptune_id
    ORDER BY n.name
    """
    return execute_cypher(query)


def get_entity_by_name(name: str):
    """이름으로 엔티티 상세 조회"""
    query = """
    MATCH (n)
    WHERE n.name = $name
    RETURN n.name AS name, 
           labels(n) AS entity_type, 
           n.description AS description,
           n.summary AS summary,
           n.neptune_id AS neptune_id
    """
    return execute_cypher(query, name=name)


def count_entities_by_type():
    """타입별 엔티티 개수 조회"""
    query = """
    MATCH (n)
    WHERE NOT n:__Chunk__ AND NOT n:MOVIE AND NOT n:REVIEWER
    RETURN labels(n) AS entity_type, count(n) AS count
    ORDER BY count DESC
    """
    return execute_cypher(query)


def get_entity_stats():
    """엔티티 통계 조회"""
    # 전체 개수
    total_query = """
    MATCH (n)
    WHERE NOT n:__Chunk__ AND NOT n:MOVIE AND NOT n:REVIEWER
    RETURN count(n) AS total
    """
    total_result = execute_cypher(total_query)
    
    # 요약 완료된 개수
    summarized_query = """
    MATCH (n)
    WHERE NOT n:__Chunk__ AND NOT n:MOVIE AND NOT n:REVIEWER
      AND n.summary IS NOT NULL AND n.summary <> ''
    RETURN count(n) AS summarized
    """
    summarized_result = execute_cypher(summarized_query)
    
    # 타입별 개수
    type_result = count_entities_by_type()
    
    return {
        "total": total_result,
        "summarized": summarized_result,
        "by_type": type_result
    }


def print_entities(entities, show_description=False, show_summary=False):
    """엔티티 목록 출력"""
    if not entities or 'results' not in entities:
        print("❌ 엔티티가 없습니다.")
        return
    
    results = entities['results']
    print(f"\n📋 총 {len(results)}개 엔티티\n")
    print("-" * 80)
    
    for i, entity in enumerate(results, 1):
        name = entity.get('name', 'N/A')
        entity_type = entity.get('entity_type', ['UNKNOWN'])
        entity_type_str = entity_type[0] if entity_type else 'UNKNOWN'
        neptune_id = entity.get('neptune_id', 'N/A')
        summary = entity.get('summary', '')
        
        print(f"{i:3}. [{entity_type_str:20}] {name}")
        print(f"     Neptune ID: {neptune_id}")
        
        if show_description:
            desc = entity.get('description', '')
            if desc:
                if isinstance(desc, str):
                    try:
                        desc_list = json.loads(desc)
                        desc = desc_list[0] if desc_list else desc
                    except:
                        pass
                print(f"     Description: {str(desc)[:100]}...")
        
        if show_summary and summary:
            print(f"     Summary: {summary[:100]}...")
        
        print()
    
    print("-" * 80)


def run_check_entity():
    """엔티티 전체 조회 실행"""
    print("=" * 60)
    print("🔍 Neptune Entity Check")
    print("=" * 60)
    
    # 통계 조회
    stats = get_entity_stats()
    
    total = 0
    if stats['total'] and 'results' in stats['total']:
        total = stats['total']['results'][0].get('total', 0)
    
    summarized = 0
    if stats['summarized'] and 'results' in stats['summarized']:
        summarized = stats['summarized']['results'][0].get('summarized', 0)
    
    print(f"\n📊 엔티티 통계:")
    print(f"   - 전체: {total}개")
    print(f"   - 요약 완료: {summarized}개")
    print(f"   - 요약 미완료: {total - summarized}개")
    
    # 타입별 개수
    if stats['by_type'] and 'results' in stats['by_type']:
        print(f"\n📊 타입별 개수:")
        for item in stats['by_type']['results']:
            entity_type = item.get('entity_type', ['UNKNOWN'])
            entity_type_str = entity_type[0] if entity_type else 'UNKNOWN'
            count = item.get('count', 0)
            print(f"   - {entity_type_str}: {count}개")
    
    # 전체 엔티티 목록
    entities = get_all_entities()
    print_entities(entities, show_description=True, show_summary=True)
    
    return entities


if __name__ == "__main__":
    run_check_entity()
