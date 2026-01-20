"""
Neptune Relationship 조회 유틸리티
- 전체 관계 조회
- 엔티티별 관계 조회
- 관계 상세 정보 조회
"""
import json
from neptune.neptune_con import execute_cypher


def get_all_relationships():
    """모든 관계 조회 (중복 제거)"""
    query = """
    MATCH (s)-[r:RELATIONSHIP]-(t)
    WHERE id(s) < id(t)
    RETURN labels(s) AS source_type, 
           s.name AS source_name,
           labels(t) AS target_type, 
           t.name AS target_name,
           r.description AS description, 
           r.summary AS summary,
           r.strength AS strength
    ORDER BY s.name, t.name
    """
    return execute_cypher(query)


def get_relationships_by_entity(entity_name: str):
    """특정 엔티티와 연결된 모든 관계 조회"""
    query = """
    MATCH (s)-[r:RELATIONSHIP]-(t)
    WHERE s.name = $entity_name OR t.name = $entity_name
    RETURN labels(s) AS source_type, 
           s.name AS source_name,
           labels(t) AS target_type, 
           t.name AS target_name,
           r.description AS description, 
           r.summary AS summary,
           r.strength AS strength
    ORDER BY s.name, t.name
    """
    return execute_cypher(query, entity_name=entity_name)


def get_relationships_by_type(source_type: str, target_type: str = None):
    """특정 타입 간의 관계 조회"""
    if target_type:
        query = f"""
        MATCH (s:{source_type})-[r:RELATIONSHIP]-(t:{target_type})
        WHERE id(s) < id(t)
        RETURN labels(s) AS source_type, 
               s.name AS source_name,
               labels(t) AS target_type, 
               t.name AS target_name,
               r.description AS description, 
               r.summary AS summary,
               r.strength AS strength
        ORDER BY s.name, t.name
        """
    else:
        query = f"""
        MATCH (s:{source_type})-[r:RELATIONSHIP]-(t)
        WHERE id(s) < id(t)
        RETURN labels(s) AS source_type, 
               s.name AS source_name,
               labels(t) AS target_type, 
               t.name AS target_name,
               r.description AS description, 
               r.summary AS summary,
               r.strength AS strength
        ORDER BY s.name, t.name
        """
    return execute_cypher(query)


def count_relationships():
    """전체 관계 개수 조회"""
    query = """
    MATCH ()-[r:RELATIONSHIP]->()
    RETURN count(r) AS total
    """
    return execute_cypher(query)


def get_relationship_stats():
    """관계 통계 조회"""
    # 전체 개수
    total_query = """
    MATCH ()-[r:RELATIONSHIP]->()
    RETURN count(r) AS total
    """
    total_result = execute_cypher(total_query)
    
    # 요약 완료된 개수
    summarized_query = """
    MATCH ()-[r:RELATIONSHIP]->()
    WHERE r.summary IS NOT NULL AND r.summary <> ''
    RETURN count(r) AS summarized
    """
    summarized_result = execute_cypher(summarized_query)
    
    # 타입 조합별 개수
    type_query = """
    MATCH (s)-[r:RELATIONSHIP]->(t)
    RETURN labels(s) AS source_type, labels(t) AS target_type, count(r) AS count
    ORDER BY count DESC
    """
    type_result = execute_cypher(type_query)
    
    return {
        "total": total_result,
        "summarized": summarized_result,
        "by_type": type_result
    }


def print_relationships(relationships, show_description=False, show_summary=False):
    """관계 목록 출력"""
    if not relationships or 'results' not in relationships:
        print("❌ 관계가 없습니다.")
        return
    
    results = relationships['results']
    print(f"\n🔗 총 {len(results)}개 관계\n")
    print("-" * 100)
    
    for i, rel in enumerate(results, 1):
        source_name = rel.get('source_name', 'N/A')
        target_name = rel.get('target_name', 'N/A')
        source_type = rel.get('source_type', ['UNKNOWN'])
        target_type = rel.get('target_type', ['UNKNOWN'])
        source_type_str = source_type[0] if source_type else 'UNKNOWN'
        target_type_str = target_type[0] if target_type else 'UNKNOWN'
        strength = rel.get('strength', 0)
        summary = rel.get('summary', '')
        
        print(f"{i:3}. {source_name} ({source_type_str}) ──[{strength}]──> {target_name} ({target_type_str})")
        
        if show_description:
            desc = rel.get('description', '')
            if desc:
                if isinstance(desc, str):
                    try:
                        desc_list = json.loads(desc)
                        if isinstance(desc_list, list):
                            desc = '\n              '.join(desc_list)
                        else:
                            desc = str(desc_list)
                    except:
                        pass
                print(f"     Description: {desc}")
        
        if show_summary and summary:
            print(f"     Summary: {summary}")
        
        print()
    
    print("-" * 100)


def run_check_relationship():
    """관계 전체 조회 실행"""
    print("=" * 60)
    print("🔍 Neptune Relationship Check")
    print("=" * 60)
    
    # 통계 조회
    stats = get_relationship_stats()
    
    total = 0
    if stats['total'] and 'results' in stats['total']:
        total = stats['total']['results'][0].get('total', 0)
    
    summarized = 0
    if stats['summarized'] and 'results' in stats['summarized']:
        summarized = stats['summarized']['results'][0].get('summarized', 0)
    
    print(f"\n📊 관계 통계:")
    print(f"   - 전체: {total}개")
    print(f"   - 요약 완료: {summarized}개")
    print(f"   - 요약 미완료: {total - summarized}개")
    
    # 타입 조합별 개수
    if stats['by_type'] and 'results' in stats['by_type']:
        print(f"\n📊 타입 조합별 개수:")
        for item in stats['by_type']['results']:
            source_type = item.get('source_type', ['UNKNOWN'])
            target_type = item.get('target_type', ['UNKNOWN'])
            source_str = source_type[0] if source_type else 'UNKNOWN'
            target_str = target_type[0] if target_type else 'UNKNOWN'
            count = item.get('count', 0)
            print(f"   - {source_str} → {target_str}: {count}개")
    
    # 전체 관계 목록
    relationships = get_all_relationships()
    print_relationships(relationships, show_description=True, show_summary=True)
    
    return relationships


if __name__ == "__main__":
    run_check_relationship()
