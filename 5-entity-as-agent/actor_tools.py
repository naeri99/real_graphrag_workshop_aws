"""
ACTOR 엔티티용 Strands Tools
- search_neptune: 그래프DB에서 출연작, 배역, 관계 조회
- search_web: 웹에서 최신 정보 검색 (Tavily 사용)
"""
from strands import tool
from tavily import TavilyClient
from neptune.neptune_con import execute_cypher

# Tavily 클라이언트
tavily = TavilyClient(api_key="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")


@tool
def search_neptune(actor_name: str, query_type: str = "filmography") -> str:
    """
    Neptune 그래프DB에서 배우 정보를 조회합니다.
    출연작, 배역, 영화 관계 등 저장된 정보를 검색할 때 사용하세요.
    
    Args:
        actor_name: 배우 이름
        query_type: 조회 유형 (filmography, characters, relationships)
    """
    if query_type == "filmography":
        cypher = """
        MATCH (a:ACTOR {name: $name})-[:ACTED_AS]->(c:MOVIE_CHARACTER)-[:APPEARS_IN]->(m:MOVIE)
        RETURN m.name AS movie, c.name AS character, m.description AS movie_desc
        LIMIT 10
        """
    elif query_type == "characters":
        cypher = """
        MATCH (a:ACTOR {name: $name})-[:ACTED_AS]->(c:MOVIE_CHARACTER)
        RETURN c.name AS character, c.description AS description
        LIMIT 10
        """
    else:
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
    """
    search_queries = {
        "recent": f"배우 {actor_name} 최신 근황 2024 2025",
        "awards": f"배우 {actor_name} 수상 이력 영화제",
        "news": f"배우 {actor_name} 최신 뉴스"
    }
    
    query = search_queries.get(search_type, search_queries["recent"])
    
    try:
        response = tavily.search(query=query, max_results=5)
        results = response.get('results', [])
        
        if results:
            output = f"[웹 검색 결과] {actor_name} ({search_type}):\n"
            for i, r in enumerate(results, 1):
                title = r.get('title', '')
                content = r.get('content', '')[:200]
                url = r.get('url', '')
                output += f"\n{i}. {title}\n   {content}...\n   URL: {url}\n"
            return output
        else:
            return f"[웹 검색] '{query}' - 검색 결과 없음"
    except Exception as e:
        return f"[웹 검색 실패] {query}: {e}"
