import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
import os
import json
import sys
from typing import List, Optional, Dict, Tuple
from opensearch.opensearch_con import get_opensearch_client
from utils.type import Document
from utils.bedrock_embedding import BedrockEmbedding


def normalize_search_results(search_results):
        hits = (search_results["hits"]["hits"])
        max_score = float(search_results["hits"]["max_score"])
        for hit in hits:
            hit["_score"] = float(hit["_score"]) / max_score
        search_results["hits"]["max_score"] = hits[0]["_score"]
        search_results["hits"]["hits"] = hits
        return search_results


def vector_search_entities(query_text, index_name="entities", k=10, entity_type=None):
    """
    OpenSearch 3.x KNN 검색
    """
    client = get_opensearch_client()
    embedder = BedrockEmbedding()
    
    try:
        # 쿼리 텍스트를 벡터로 변환
        query_vector = embedder.embed_text(query_text)
        
        # OpenSearch 3.x KNN 검색 쿼리
        if entity_type:
            vector_query = {
                "size": k,
                "_source": {
                    "excludes": ["entity.summary_vec"]
                },
                "query": {
                    "bool": {
                        "must": {
                            "knn": {
                                "entity.summary_vec": {
                                    "vector": query_vector,
                                    "k": k
                                }
                            }
                        },
                        "filter": {
                            "term": {
                                "entity.entity_type": entity_type
                            }
                        }
                    }
                }
            }
        else:
            vector_query = {
                "size": k,
                "_source": {
                    "excludes": ["entity.summary_vec"]
                },
                "query": {
                    "knn": {
                        "entity.summary_vec": {
                            "vector": query_vector,
                            "k": k
                        }
                    }
                }
            }
        
        # 검색 실행
        response = client.search(
            index=index_name,
            body=vector_query
        )
        
        documents = []
        if response.get("hits", {}).get("hits", []):
            for res in response["hits"]["hits"]:
                source = res['_source']
                page_content = {k: source[k] for k in source}
                metadata = {
                    "id": res['_id'],
                    "score": res['_score']
                }
                score = res['_score']
                documents.append((Document(page_content=json.dumps(page_content, ensure_ascii=False), metadata=metadata), score))
        
        return documents
        
    except Exception as e:
        print(f"Vector search error: {str(e)}")
        print("Falling back to text search...")
        return text_search_entities(query_text, index_name, k, entity_type)


def hybrid_search_entities(query_text, index_name="movie_graph", k=10, entity_type=None, vector_weight=0.7):
    """
    하이브리드 검색: 텍스트 검색 + 벡터 검색
    """
    client = get_opensearch_client()
    embedder = BedrockEmbedding()
    
    try:
        # 쿼리 텍스트를 벡터로 변환
        query_vector = embedder.embed_text(query_text)
        
        text_weight = 1.0 - vector_weight
        
        # 하이브리드 검색 쿼리 (간단한 방식)
        hybrid_query = {
            "size": k,
            "_source": {
                "excludes": ["entity.summary_vec"]
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": ["entity.name^2", "entity.summary"],
                                "boost": text_weight
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        
        # 엔티티 타입 필터 추가
        if entity_type:
            hybrid_query["query"]["bool"]["filter"] = [
                {
                    "term": {
                        "entity.entity_type": entity_type
                    }
                }
            ]
        
        # 검색 실행
        response = client.search(
            index=index_name,
            body=hybrid_query
        )
        
        documents = []
        if response.get("hits", {}).get("hits", []):
            for res in response["hits"]["hits"]:
                source = res['_source']
                page_content = {k: source[k] for k in source}
                metadata = {
                    "id": res['_id'],
                    "score": res['_score']
                }
                score = res['_score']
                documents.append((Document(page_content=json.dumps(page_content, ensure_ascii=False), metadata=metadata), score))
        
        return documents
        
    except Exception as e:
        print(f"Hybrid search error: {str(e)}")
        return text_search_entities(query_text, index_name, k, entity_type)


def text_search_entities(query_text, index_name="movie_graph", k=10, entity_type=None):
    """
    텍스트 기반 엔티티 검색 (기본 분석기 사용)
    
    Args:
        query_text: 검색할 텍스트
        index_name: OpenSearch 인덱스 이름
        k: 반환할 결과 수
        entity_type: 특정 엔티티 타입으로 필터링
    
    Returns:
        List of (Document, score) tuples
    """
    client = get_opensearch_client()
    
    try:
        # 텍스트 검색 쿼리 구성 (기본 분석기 사용)
        text_query = {
            "size": k,
            "_source": {
                "excludes": ["entity.summary_vec"]
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "entity.name": {
                                    "query": query_text,
                                    "boost": 2.0
                                }
                            }
                        },
                        {
                            "match": {
                                "entity.summary": {
                                    "query": query_text
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        
        # 엔티티 타입 필터 추가
        if entity_type:
            text_query["query"]["bool"]["filter"] = [
                {
                    "term": {
                        "entity.entity_type": entity_type
                    }
                }
            ]
        
        # 검색 실행
        response = client.search(
            index=index_name,
            body=text_query
        )
        
        documents = []
        if response.get("hits", {}).get("hits", []):
            search_results = normalize_search_results(response)
            for res in search_results["hits"]["hits"]:
                source = res['_source']
                page_content = {k: source[k] for k in source}
                metadata = {
                    "id": res['_id'],
                    "score": res['_score']
                }
                score = res['_score']
                documents.append((Document(page_content=json.dumps(page_content, ensure_ascii=False), metadata=metadata), score))
        
        return documents
        
    except Exception as e:
        print(f"Text search error: {str(e)}")
        return []


def search_by_neptune_id(neptune_id, index_name="movie_graph"):
    """
    Neptune ID로 엔티티 검색 (정확한 매치)
    
    Args:
        neptune_id: Neptune ID
        index_name: OpenSearch 인덱스 이름
    
    Returns:
        Document or None
    """
    client = get_opensearch_client()
    
    try:
        response = client.get(
            index=index_name,
            id=neptune_id
        )
        
        if response.get('found'):
            source = response['_source']
            page_content = {k: source[k] for k in source}
            metadata = {"id": response['_id']}
            return Document(page_content=json.dumps(page_content, ensure_ascii=False), metadata=metadata)
        
        return None
        
    except Exception as e:
        print(f"Neptune ID search error: {str(e)}")
        return None


def simple_movie_search(search_text, index_name="graph_rag", k=10):
    client = get_opensearch_client()
    try:
        text_query = {
            "size": k,
            "_source": {
                "excludes": ["movie.summary_vec"]
            },
            "query": {
                "match": {
                    "movie.title": {
                        "query": search_text,
                        "analyzer": "nori_analyzer"
                    }
                }
            }
        }

        # Execute search
        response = client.search(
            index=index_name,
            body=text_query
        )


        documents = []
        if response.get("hits", {}).get("hits", []):
            search_results = normalize_search_results(response) 
            for res in search_results["hits"]["hits"]:
                source = res['_source']
                page_content = {k: source[k] for k in source }
                metadata = {"id": res['_id']}
                score = res['_score']  # Get the score from the search result
                documents.append((Document(page_content=json.dumps(page_content, ensure_ascii=False), metadata=metadata), score))
        return documents  

    except Exception as e:
        print(f"Search error: {str(e)}")
        return None


# ============ 테스트 및 예제 함수들 ============

def test_vector_search():
    """벡터 검색 테스트"""
    print("=== Vector Search Test ===")
    
    query = "주인공 캐릭터"
    results = vector_search_entities(query, k=5, entity_type="MOVIE_CHARACTER")
    
    print(f"Query: {query}")
    print(f"Results: {len(results)}")
    
    for i, (doc, score) in enumerate(results, 1):
        content = json.loads(doc.page_content)
        entity = content.get('entity', {})
        print(f"{i}. {entity.get('name', 'Unknown')} (Score: {score:.4f})")
        print(f"   Type: {entity.get('entity_type', 'Unknown')}")
        print(f"   Summary: {entity.get('summary', 'No summary')[:100]}...")
        print()


def test_hybrid_search():
    """하이브리드 검색 테스트"""
    print("=== Hybrid Search Test ===")
    
    query = "꿈속에서 활동하는 인물"
    results = hybrid_search_entities(query, k=5)
    
    print(f"Query: {query}")
    print(f"Results: {len(results)}")
    
    for i, (doc, score) in enumerate(results, 1):
        content = json.loads(doc.page_content)
        entity = content.get('entity', {})
        print(f"{i}. {entity.get('name', 'Unknown')} (Score: {score:.4f})")
        print(f"   Type: {entity.get('entity_type', 'Unknown')}")
        print(f"   Summary: {entity.get('summary', 'No summary')[:100]}...")
        print()


def test_text_search():
    """텍스트 검색 테스트"""
    print("=== Text Search Test ===")
    
    query = "코브"
    results = text_search_entities(query, k=5)
    
    print(f"Query: {query}")
    print(f"Results: {len(results)}")
    
    for i, (doc, score) in enumerate(results, 1):
        content = json.loads(doc.page_content)
        entity = content.get('entity', {})
        print(f"{i}. {entity.get('name', 'Unknown')} (Score: {score:.4f})")
        print(f"   Type: {entity.get('entity_type', 'Unknown')}")
        print(f"   Summary: {entity.get('summary', 'No summary')[:100]}...")
        print()


def test_neptune_id_search():
    """Neptune ID 검색 테스트"""
    print("=== Neptune ID Search Test ===")
    
    # 실제 Neptune ID로 테스트 (예시)
    neptune_id = "코브_MOVIE_CHARACTER_12345678"  # 실제 ID로 변경 필요
    result = search_by_neptune_id(neptune_id)
    
    if result:
        content = json.loads(result.page_content)
        entity = content.get('entity', {})
        print(f"Found: {entity.get('name', 'Unknown')}")
        print(f"Type: {entity.get('entity_type', 'Unknown')}")
        print(f"Neptune ID: {entity.get('neptune_id', 'Unknown')}")
        print(f"Summary: {entity.get('summary', 'No summary')}")
    else:
        print("Entity not found")


if __name__ == "__main__":
    print("OpenSearch Entity Search Test")
    print("=" * 50)
    
    # 각 검색 방법 테스트
    test_text_search()
    test_vector_search()
    test_hybrid_search()
    # test_neptune_id_search()  # 실제 Neptune ID가 있을 때 테스트