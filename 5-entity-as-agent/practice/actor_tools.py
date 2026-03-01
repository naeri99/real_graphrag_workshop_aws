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
