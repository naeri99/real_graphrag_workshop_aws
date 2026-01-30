

def find_entity_opensearch(opensearch_client, entity_name, index_name):
    """
    OpenSearch에서 엔티티 이름으로 검색하여 기존 동의어를 찾습니다.
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        entity_name: 검색할 엔티티 이름 (공백 자동 제거)
        index_name: 검색할 인덱스 이름
        
    Returns:
        dict: 검색 결과 (entity 정보 포함) 또는 None
    """
    try:
        # 검색 전 공백 제거
        entity_name = entity_name.strip() if entity_name else ""
        
        if not entity_name:
            print("⚠️ 엔티티 이름이 비어있습니다.")
            return None
        
        # 엔티티 이름으로 정확히 매칭되는 문서 검색
        search_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "entity.name": entity_name
                            }
                        }
                    ]
                }
            },
            "size": 1  # 첫 번째 매칭 결과만 가져오기
        }
        
        response = opensearch_client.search(
            index=index_name,
            body=search_body
        )
        
        hits = response.get('hits', {}).get('hits', [])
        
        if hits:
            # 첫 번째 매칭 결과 반환
            hit = hits[0]
            return {
                'id': hit['_id'],
                'source': hit['_source'],
                'entity': hit['_source'].get('entity', {})
            }
        else:
            print(f"🔍 '{entity_name}' 엔티티를 찾을 수 없습니다.")
            return None
            
    except Exception as e:
        print(f"❌ OpenSearch 검색 오류: {e}")
        return None