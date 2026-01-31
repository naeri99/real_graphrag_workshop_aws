"""
ACTOR 엔티티용 Strands Tools
- search_neptune: 그래프DB에서 출연작, 배역, 관계 조회
- search_web: 웹에서 최신 정보 검색
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strands import tool
from neptune.neptune_con import execute_cypher


@tool
def search_neptune(actor_name: str, query_type: str = "filmography") -> str:
    """
    Neptune 그래프DB에서 배우 정보를 조회합니다.
    출연작, 배역, 영화 관계 등 저장된 정보를 검색할 때 사용하세요.
    
    Args:
        actor_name: 배우 이름
        query_type: 조회 유형 (filmography, characters, relationships)
    
    Returns:
        그래프DB 조회 결과
    """
    if query_type == "filmography":
        # 출연작 조회
        cypher = """
        MATCH (a:ACTOR {name: $name})-[:ACTED_AS]->(c:MOVIE_CHARACTER)-[:APPEARS_IN]->(m:MOVIE)
        RETURN m.name AS movie, c.name AS character, m.description AS movie_desc
        LIMIT 10
        """
    elif query_type == "characters":
        # 배역 조회
        cypher = """
        MATCH (a:ACTOR {name: $name})-[:ACTED_AS]->(c:MOVIE_CHARACTER)
        RETURN c.name AS character, c.description AS description
        LIMIT 10
        """
    else:
        # 전체 관계 조회
        cypher = """
        MATCH (a:ACTOR {name: $name})-[r]-(related)
        WHERE NOT related:__Chunk__
        RETURN type(r) AS relationship, related.name AS target, labels(related) AS target_type
        LIMIT 15
        """
    
    result = execute_cypher(cypher, name=actor_name)
    results = result.get('results', []) if result else []
    
    if not results:
        return f"'{actor_name}' 배우의 {query_type} 정보를 찾을 수 없습니다."
    
    # 결과 포맷팅
    output = f"[Neptune DB] {actor_name} - {query_type}:\n"
    for r in results:
        if query_type == "filmography":
            output += f"  - 영화: {r.get('movie')}, 배역: {r.get('character')}\n"
        elif query_type == "characters":
            desc = r.get('description', '')[:100] if r.get('description') else ''
            output += f"  - {r.get('character')}: {desc}\n"
        else:
            target_type = r.get('target_type', [])
            target_type = target_type[0] if isinstance(target_type, list) and target_type else target_type
            output += f"  - [{r.get('relationship')}] → {r.get('target')} ({target_type})\n"
    
    return output


@tool
def search_web(actor_name: str, search_type: str = "recent") -> str:
    """
    웹에서 배우의 최신 정보를 검색합니다.
    최신 근황, 수상 이력, 실시간 뉴스 등을 찾을 때 사용하세요.
    
    Args:
        actor_name: 배우 이름
        search_type: 검색 유형 (recent, awards, news)
    
    Returns:
        웹 검색 요청 (실제 검색은 Agent가 수행)
    """
    search_queries = {
        "recent": f"배우 {actor_name} 최신 근황 2024 2025",
        "awards": f"배우 {actor_name} 수상 이력 영화제",
        "news": f"배우 {actor_name} 최신 뉴스"
    }
    
    query = search_queries.get(search_type, search_queries["recent"])
    
    # 실제 웹 검색 결과 반환 (여기서는 검색 요청만)
    return f"[웹 검색 요청] '{query}' - 이 정보는 실시간 웹 검색이 필요합니다."
