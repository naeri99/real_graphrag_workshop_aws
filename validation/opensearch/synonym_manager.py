from opensearch.opensearch_con import get_opensearch_client
import json


def find_entity_opensearch(opensearch_client, entity_name, index_name="movie_graph"):
    """
    OpenSearch에서 엔티티 이름으로 검색하여 기존 동의어를 찾습니다.
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        entity_name: 검색할 엔티티 이름
        index_name: 검색할 인덱스 이름
        
    Returns:
        dict: 검색 결과 (entity 정보 포함) 또는 None
    """
    try:
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


def merge_synonyms_with_set(existing_synonyms, new_synonyms):
    """
    기존 동의어와 새 동의어를 set을 사용하여 중복 제거하고 병합합니다.
    
    Args:
        existing_synonyms: 기존 동의어 리스트 또는 문자열
        new_synonyms: 새 동의어 리스트
        
    Returns:
        list: 중복이 제거된 병합된 동의어 리스트
    """
    # 기존 동의어 처리
    if isinstance(existing_synonyms, str):
        # 문자열인 경우 쉼표로 분리
        existing_set = set(syn.strip() for syn in existing_synonyms.split(',') if syn.strip())
    elif isinstance(existing_synonyms, list):
        # 리스트인 경우 그대로 사용
        existing_set = set(syn.strip() for syn in existing_synonyms if syn.strip())
    else:
        # 기타 경우 빈 set
        existing_set = set()
    
    # 새 동의어 처리
    if isinstance(new_synonyms, list):
        new_set = set(syn.strip() for syn in new_synonyms if syn.strip())
    else:
        new_set = set()
    
    # 병합 및 정렬
    merged_synonyms = sorted(list(existing_set.union(new_set)))
    
    print(f"📝 동의어 병합 결과:")
    print(f"   기존: {len(existing_set)}개 - {list(existing_set)}")
    print(f"   새로운: {len(new_set)}개 - {list(new_set)}")
    print(f"   병합: {len(merged_synonyms)}개 - {merged_synonyms}")
    
    return merged_synonyms


def update_entity_synonyms(opensearch_client, entity_id, merged_synonyms, index_name="movie_graph"):
    """
    OpenSearch에서 엔티티의 동의어를 업데이트합니다.
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        entity_id: 업데이트할 엔티티의 문서 ID
        merged_synonyms: 병합된 동의어 리스트
        index_name: 인덱스 이름
        
    Returns:
        bool: 업데이트 성공 여부
    """
    try:
        # 동의어를 쉼표로 구분된 문자열로 변환
        synonym_string = ','.join(merged_synonyms)
        
        # 부분 업데이트 수행
        update_body = {
            "doc": {
                "entity": {
                    "synonym": synonym_string
                }
            }
        }
        
        response = opensearch_client.update(
            index=index_name,
            id=entity_id,
            body=update_body
        )
        
        if response.get('result') in ['updated', 'noop']:
            print(f"✅ 엔티티 ID '{entity_id}' 동의어 업데이트 성공")
            print(f"   업데이트된 동의어: {synonym_string}")
            return True
        else:
            print(f"⚠️ 엔티티 ID '{entity_id}' 업데이트 결과: {response.get('result')}")
            return False
            
    except Exception as e:
        print(f"❌ 동의어 업데이트 오류: {e}")
        return False


def find_entity_opensearch_synonym(opensearch_client, entities_synonym, index_name="movie_graph"):
    """
    동의어 엔티티 리스트를 받아서 OpenSearch에서 기존 동의어를 찾고,
    새로운 동의어와 병합하여 중복을 제거한 후 다시 저장합니다.
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        entities_synonym: 동의어 엔티티 리스트 (parse_synonym_output 결과)
                         각 항목은 {'entity_name': str, 'entity_type': str, 'synonyms': list} 형태
        index_name: OpenSearch 인덱스 이름
        
    Returns:
        dict: 처리 결과 통계
    """
    print(f"🔄 동의어 병합 및 업데이트 시작...")
    print(f"   대상 인덱스: {index_name}")
    print(f"   처리할 엔티티 수: {len(entities_synonym)}")
    print("=" * 60)
    
    results = {
        'total': len(entities_synonym),
        'found': 0,
        'not_found': 0,
        'updated': 0,
        'failed': 0,
        'details': []
    }
    
    for i, entity_data in enumerate(entities_synonym, 1):
        entity_name = entity_data['entity_name']
        entity_type = entity_data['entity_type']
        new_synonyms = entity_data['synonyms']
        
        print(f"\n[{i}/{len(entities_synonym)}] 처리 중: {entity_name} ({entity_type})")
        print(f"   새 동의어 {len(new_synonyms)}개: {new_synonyms}")
        
        # 1. OpenSearch에서 기존 엔티티 검색
        existing_entity = find_entity_opensearch(opensearch_client, entity_name, index_name)
        
        if existing_entity:
            results['found'] += 1
            
            # 2. 기존 동의어 추출
            existing_synonyms = existing_entity['entity'].get('synonym', [])
            
            # 3. 동의어 병합 (set 사용하여 중복 제거)
            merged_synonyms = merge_synonyms_with_set(existing_synonyms, new_synonyms)
            
            # 4. OpenSearch에 업데이트
            update_success = update_entity_synonyms(
                opensearch_client, 
                existing_entity['id'], 
                merged_synonyms, 
                index_name
            )
            
            if update_success:
                results['updated'] += 1
                status = "✅ 업데이트 성공"
            else:
                results['failed'] += 1
                status = "❌ 업데이트 실패"
                
        else:
            results['not_found'] += 1
            status = "🔍 엔티티 없음"
        
        # 결과 기록
        results['details'].append({
            'entity_name': entity_name,
            'entity_type': entity_type,
            'status': status,
            'new_synonyms_count': len(new_synonyms),
            'found': existing_entity is not None
        })
        
        print(f"   결과: {status}")
    
    # 최종 결과 출력
    print("\n" + "=" * 60)
    print("🎯 동의어 병합 및 업데이트 완료!")
    print(f"   전체: {results['total']}개")
    print(f"   찾음: {results['found']}개")
    print(f"   없음: {results['not_found']}개")
    print(f"   업데이트 성공: {results['updated']}개")
    print(f"   업데이트 실패: {results['failed']}개")
    
    return results


def test_synonym_manager():
    """동의어 관리자 테스트 함수"""
    print("🧪 동의어 관리자 테스트 시작...")
    
    # OpenSearch 클라이언트 생성
    client = get_opensearch_client()
    
    # 테스트 데이터
    test_entities = [
        {
            'entity_name': 'Leonardo DiCaprio',
            'entity_type': 'ACTOR',
            'synonyms': ['레오나르도 디카프리오', '레오나르도', '디카프리오', 'Leo DiCaprio']
        },
        {
            'entity_name': 'Dom Cobb',
            'entity_type': 'MOVIE_CHARACTER', 
            'synonyms': ['코브', '도미닉 코브', '돔 코브']
        }
    ]
    
    # 동의어 병합 및 업데이트 실행
    results = find_entity_opensearch_synonym(client, test_entities)
    
    print(f"\n📊 테스트 결과: {results}")
    
    return results


if __name__ == "__main__":
    test_synonym_manager()