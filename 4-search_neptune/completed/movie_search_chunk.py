"""
Chunk 기반 엔티티 검색
"""
from neptune.neptune_con import execute_cypher


def get_entities_by_chunk_id(chunk_id: str) -> list:
    """청크 ID로 연결된 엔티티들 조회"""
    query = """
    MATCH (c:__Chunk__ {id: $chunk_id})-[:MENTIONS]->(e)
    RETURN e.name AS name, labels(e) AS entity_type, e.description AS description, e.neptune_id AS neptune_id
    """
    result = execute_cypher(query, chunk_id=chunk_id)
    return result.get('results', []) if result else []


def get_entities_by_neptune_id(neptune_id: str) -> list:
    """청크 neptune_id로 연결된 엔티티들 조회"""
    query = """
    MATCH (c:__Chunk__ {neptune_id: $neptune_id})-[:MENTIONS]->(e)
    RETURN e.name AS name, labels(e) AS entity_type, e.description AS description, e.neptune_id AS neptune_id
    """
    result = execute_cypher(query, neptune_id=neptune_id)
    return result.get('results', []) if result else []


def list_chunks(limit: int = 10) -> list:
    """청크 목록 조회"""
    query = """
    MATCH (c:__Chunk__)
    RETURN c.id AS id, c.neptune_id AS neptune_id
    LIMIT $limit
    """
    result = execute_cypher(query, limit=limit)
    return result.get('results', []) if result else []


def search_by_chunk(chunk_id: str):
    """청크 ID로 엔티티 검색"""
    print(f"🔍 청크 검색: {chunk_id}")
    
    # chunk_id로 검색
    entities = get_entities_by_chunk_id(chunk_id)
    
    # 없으면 neptune_id로 검색
    if not entities:
        print("   chunk_id로 못 찾음, neptune_id로 검색...")
        entities = get_entities_by_neptune_id(chunk_id)
    
    if entities:
        print(f"📊 연결된 엔티티: {len(entities)}개")
        for e in entities:
            etype = e.get('entity_type', [])
            etype = etype[0] if etype else 'UNKNOWN'
            print(f"  - {e.get('name')} ({etype})")
    else:
        print("❌ 엔티티를 찾을 수 없습니다.")
        print("\n📋 저장된 청크 목록:")
        chunks = list_chunks(5)
        for c in chunks:
            print(f"  - id: {c.get('id')}")
            print(f"    neptune_id: {c.get('neptune_id')}")
    
    return entities


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python movie_search_chunk.py <청크ID>")
        print("\n저장된 청크 목록:")
        chunks = list_chunks(10)
        for c in chunks:
            print(f"  - {c.get('id')}")
        sys.exit(1)
    
    search_by_chunk(sys.argv[1])
