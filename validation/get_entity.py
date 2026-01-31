"""
Neptune에서 엔티티 검색
"""
from neptune.neptune_con import execute_cypher


def get_entity_by_name(entity_name: str):
    """이름으로 Neptune에서 엔티티 검색"""
    query = """
    MATCH (n)
    WHERE n.name = $name
    RETURN n.name AS name, labels(n) AS entity_type, n.summary AS summary, n.neptune_id AS neptune_id
    """
    result = execute_cypher(query, name=entity_name)
    return result.get('results', []) if result else []


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python get_entity.py <엔티티이름>")
        sys.exit(1)
    
    name = sys.argv[1]
    results = get_entity_by_name(name)
    
    if results:
        for r in results:
            print(f"이름: {r.get('name')}")
            print(f"타입: {r.get('entity_type')}")
            print(f"neptune_id: {r.get('neptune_id')}")
            summary = r.get('summary') or ''
            print(f"summary: {summary[:100]}..." if summary else "summary: 없음")
    else:
        print(f"'{name}' 엔티티를 찾을 수 없습니다.")
