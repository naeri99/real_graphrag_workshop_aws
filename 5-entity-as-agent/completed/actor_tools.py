"""
ACTOR 엔티티용 Strands Tools
- search_neptune: 그래프DB에서 출연작, 배역, 관계 조회
- search_web: 웹에서 최신 정보 검색 (Tavily 사용)
"""
from strands import tool
from tavily import TavilyClient
from neptune.neptune_con import execute_cypher
import os
# Tavily 클라이언트


value = os.environ.get('TVLY')

tavily = TavilyClient(api_key=value)


@tool
def search_neptune(actor_name: str) -> str:
    """
    Neptune 그래프DB에서 배우 정보를 조회합니다.
    배우의 기본 정보를 검색할 때 사용하세요.
    
    Args:
        actor_name: 배우 이름
    """
    print(f"    🔍 [Neptune 검색] {actor_name}")
    cypher = """
    MATCH (a:ACTOR {name: $name})
    RETURN a.name AS name, a.description AS description
    """
    result = execute_cypher(cypher, name=actor_name)
    info = result.get('results', []) if result else []
    
    if not info:
        return f"'{actor_name}' 배우 정보를 찾을 수 없습니다."
    
    desc = info[0].get('description', '')
    if desc:
        return f"[Neptune DB] {actor_name}:\n  {desc}"
    else:
        return f"[Neptune DB] {actor_name}: 설명 정보 없음"


@tool
def search_web(actor_name: str, search_type: str = "recent") -> str:
    """
    웹에서 배우의 최신 정보를 검색합니다.
    최신 근황, 수상 이력, 실시간 뉴스 등을 찾을 때 사용하세요.
    
    Args:
        actor_name: 배우 이름
        search_type: 검색 유형 (recent, awards, news)
    """
    print(f"    🌐 [웹 검색] {actor_name} ({search_type})")
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
