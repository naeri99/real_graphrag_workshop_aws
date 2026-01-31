"""
OpenSearch KNN 검색으로 영화/엔티티 검색
"""
import sys
from opensearch.opensearch_search import knn_search


def search_movies(query: str, k: int = 5):
    """영화 검색"""
    print(f"🎬 영화 검색: '{query}'")
    return knn_search(query, entity_type="MOVIE", k=k)


def search_actors(query: str, k: int = 5):
    """배우 검색"""
    print(f"🎭 배우 검색: '{query}'")
    return knn_search(query, entity_type="ACTOR", k=k)


def search_characters(query: str, k: int = 5):
    """캐릭터 검색"""
    print(f"👤 캐릭터 검색: '{query}'")
    return knn_search(query, entity_type="MOVIE_CHARACTER", k=k)


def search_all(query: str, k: int = 10):
    """모든 엔티티 검색"""
    print(f"🔍 전체 검색: '{query}'")
    return knn_search(query, k=k)


def print_results(results):
    """결과 출력"""
    if not results:
        print("검색 결과 없음")
        return
    
    for i, r in enumerate(results, 1):
        print(f"\n{i}. {r['name']} ({r['entity_type']})")
        print(f"   점수: {r['score']:.4f}")
        print(f"   neptune_id: {r['neptune_id']}")
        if r['summary']:
            print(f"   요약: {r['summary'][:100]}...")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python movie_search_opensearch.py <검색어> [타입]")
        print("타입: movie, actor, character, all (기본: all)")
        sys.exit(1)
    
    query = sys.argv[1]
    search_type = sys.argv[2] if len(sys.argv) > 2 else "all"
    
    if search_type == "movie":
        results = search_movies(query)
    elif search_type == "actor":
        results = search_actors(query)
    elif search_type == "character":
        results = search_characters(query)
    else:
        results = search_all(query)
    
    print_results(results)
