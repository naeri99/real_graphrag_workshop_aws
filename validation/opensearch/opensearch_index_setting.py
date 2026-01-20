# from utils.bedrock_embedding import create_embeddings
import json


def check_index_exists(opensearch_client, index_name):
    """인덱스 존재 여부 확인"""
    try:
        return opensearch_client.indices.exists(index=index_name)
    except Exception as e:
        print(f"Error checking index existence: {e}")
        return False


def delete_index(opensearch_client, index_name):
    """OpenSearch 인덱스 삭제"""
    try:
        if not check_index_exists(opensearch_client, index_name):
            print(f"Index '{index_name}' does not exist, skipping deletion")
            return True
            
        response = opensearch_client.indices.delete(index=index_name)
        print(f"✅ Index '{index_name}' deleted successfully")
        return response
    except Exception as e:
        print(f"❌ Error deleting index '{index_name}': {e}")
        return None


def define_chunk_index(opensearch_client, index_name):
    """청크용 OpenSearch 인덱스 생성"""
    
    # 인덱스가 이미 존재하는지 확인
    if check_index_exists(opensearch_client, index_name):
        print(f"⚠️ Index '{index_name}' already exists")
        
        # 기존 매핑 확인
        mapping_valid = validate_chunk_mapping(opensearch_client, index_name)
        if mapping_valid:
            print(f"✅ Index '{index_name}' has valid mapping")
            return {"acknowledged": True, "index": index_name, "status": "already_exists"}
        else:
            print(f"❌ Index '{index_name}' has invalid mapping, consider recreating")
            return None
    
    index_settings = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100,
                "number_of_shards": 3,
                "number_of_replicas": 2,
                "analysis": {
                    "analyzer": {
                        "nori_analyzer": {
                            "tokenizer": "nori_tokenizer",
                            "filter": ["nori_stop", "lowercase"]
                        }
                    },
                    "filter": {
                        "nori_stop": {
                            "type": "nori_part_of_speech",
                            "stoptags": ["J", "JKS", "JKB", "JKO", "JKG", "JKC", "JKV", "JKQ", "JX", "JC"]
                        }
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "chunk": {
                    "properties": {
                        "chunk_id": {
                            "type": "text",
                            "analyzer": "nori_analyzer"
                        },
                        "chunk_text": {
                            "type": "text",
                            "analyzer": "nori_analyzer"
                        },
                        "source_id": {
                            "type": "keyword"
                        },
                        "summary": {
                            "type": "text",
                            "analyzer": "nori_analyzer"
                        },
                        "summary_vec": {
                            "type": "knn_vector",
                            "dimension": 1024,
                            "method": {
                                "name": "hnsw",
                                "space_type": "l2",
                                "engine": "faiss",
                                "parameters": {
                                    "ef_construction": 128,
                                    "m": 16
                                }
                            }
                        },
                        "neptune_id": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }
    }
    
    try:
        response = opensearch_client.indices.create(
            index=index_name,
            body=index_settings
        )
        print(f"✅ Chunk index '{index_name}' created successfully")
        
        # 생성된 매핑 검증
        if validate_chunk_mapping(opensearch_client, index_name):
            print(f"✅ Chunk index mapping validation passed")
        else:
            print(f"⚠️ Chunk index mapping validation failed")
            
        return response
    except Exception as e:
        print(f"❌ Error creating chunk index '{index_name}': {e}")
        return None


def define_entity_index(opensearch_client, index_name):
    """엔티티용 OpenSearch 인덱스 생성"""
    
    # 인덱스가 이미 존재하는지 확인
    if check_index_exists(opensearch_client, index_name):
        print(f"⚠️ Index '{index_name}' already exists")
        
        # 기존 매핑 확인
        mapping_valid = validate_entity_mapping(opensearch_client, index_name)
        if mapping_valid:
            print(f"✅ Index '{index_name}' has valid mapping")
            return {"acknowledged": True, "index": index_name, "status": "already_exists"}
        else:
            print(f"❌ Index '{index_name}' has invalid mapping, consider recreating")
            return None
    
    index_settings = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100,
                "number_of_shards": 3,
                "number_of_replicas": 2,
                "analysis": {
                    "analyzer": {
                        "nori_analyzer": {
                            "tokenizer": "nori_tokenizer",
                            "filter": ["nori_stop", "lowercase"]
                        }
                    },
                    "filter": {
                        "nori_stop": {
                            "type": "nori_part_of_speech",
                            "stoptags": ["J", "JKS", "JKB", "JKO", "JKG", "JKC", "JKV", "JKQ", "JX", "JC"]
                        }
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "entity": {
                    "properties": {
                        "name": {
                            "type": "text",
                            "analyzer": "nori_analyzer"
                        },
                        "synonym": {
                            "type": "keyword",
                            "fields": {
                                "text": {
                                    "type": "text",
                                    "analyzer": "nori_analyzer"
                                }
                            }
                        },
                        "entity_type": {
                            "type": "keyword"
                        },
                        "summary": {
                            "type": "text",
                            "analyzer": "nori_analyzer"
                        },
                        "summary_vec": {
                            "type": "knn_vector",
                            "dimension": 1024,
                            "method": {
                                "name": "hnsw",
                                "space_type": "l2",
                                "engine": "faiss",
                                "parameters": {
                                    "ef_construction": 128,
                                    "m": 16
                                }
                            }
                        },
                        "neptune_id": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }
    }
    
    try:
        response = opensearch_client.indices.create(
            index=index_name,
            body=index_settings
        )
        print(f"✅ Entity index '{index_name}' created successfully")
        
        # 생성된 매핑 검증
        if validate_entity_mapping(opensearch_client, index_name):
            print(f"✅ Entity index mapping validation passed")
        else:
            print(f"⚠️ Entity index mapping validation failed")
            
        return response
    except Exception as e:
        print(f"❌ Error creating entity index '{index_name}': {e}")
        return None


def check_index_settings(opensearch_client, index_name):
    """인덱스 설정 확인"""
    try:
        if not check_index_exists(opensearch_client, index_name):
            print(f"❌ Index '{index_name}' does not exist")
            return None
            
        settings = opensearch_client.indices.get_settings(index=index_name)
        mappings = opensearch_client.indices.get_mapping(index=index_name)
        
        print(f"=== {index_name} Index Settings ===")
        print("Settings:", json.dumps(settings, indent=2, ensure_ascii=False))
        print("\nMappings:", json.dumps(mappings, indent=2, ensure_ascii=False))
        
        return {"settings": settings, "mappings": mappings}
    except Exception as e:
        print(f"❌ Error checking index '{index_name}': {e}")
        return None


def validate_entity_mapping(opensearch_client, index_name):
    """엔티티 인덱스 매핑 검증"""
    try:
        mapping = opensearch_client.indices.get_mapping(index=index_name)
        properties = mapping.get(index_name, {}).get('mappings', {}).get('properties', {})
        entity_props = properties.get('entity', {}).get('properties', {})
        
        # 필수 필드 확인
        required_fields = {
            'name': 'text',
            'entity_type': 'keyword', 
            'summary': 'text',
            'summary_vec': 'knn_vector',
            'neptune_id': 'keyword'
        }
        
        for field, expected_type in required_fields.items():
            if field not in entity_props:
                print(f"❌ Missing field: entity.{field}")
                return False
                
            actual_type = entity_props[field].get('type')
            if actual_type != expected_type:
                print(f"❌ Wrong type for entity.{field}: expected {expected_type}, got {actual_type}")
                return False
        
        # 벡터 필드 상세 검증
        vec_field = entity_props.get('summary_vec', {})
        if vec_field.get('dimension') != 1024:
            print(f"❌ Wrong vector dimension: expected 1024, got {vec_field.get('dimension')}")
            return False
            
        print(f"✅ Entity mapping validation passed for '{index_name}'")
        return True
        
    except Exception as e:
        print(f"❌ Error validating entity mapping for '{index_name}': {e}")
        return False


def validate_chunk_mapping(opensearch_client, index_name):
    """청크 인덱스 매핑 검증"""
    try:
        mapping = opensearch_client.indices.get_mapping(index=index_name)
        properties = mapping.get(index_name, {}).get('mappings', {}).get('properties', {})
        chunk_props = properties.get('chunk', {}).get('properties', {})
        
        # 필수 필드 확인
        required_fields = {
            'chunk_id': 'text',
            'chunk_text': 'text',
            'source_id': 'keyword',
            'summary': 'text',
            'summary_vec': 'knn_vector',
            'neptune_id': 'keyword'
        }
        
        for field, expected_type in required_fields.items():
            if field not in chunk_props:
                print(f"❌ Missing field: chunk.{field}")
                return False
                
            actual_type = chunk_props[field].get('type')
            if actual_type != expected_type:
                print(f"❌ Wrong type for chunk.{field}: expected {expected_type}, got {actual_type}")
                return False
        
        # 벡터 필드 상세 검증
        vec_field = chunk_props.get('summary_vec', {})
        if vec_field.get('dimension') != 1024:
            print(f"❌ Wrong vector dimension: expected 1024, got {vec_field.get('dimension')}")
            return False
            
        print(f"✅ Chunk mapping validation passed for '{index_name}'")
        return True
        
    except Exception as e:
        print(f"❌ Error validating chunk mapping for '{index_name}': {e}")
        return False


def recreate_entity_index(opensearch_client, index_name="movie_graph", backup_data=False):
    """
    엔티티 인덱스 재생성 (기존 삭제 후 새로 생성)
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        index_name: 인덱스 이름
        backup_data: 기존 데이터 백업 여부 (현재는 로그만 출력)
    """
    print(f"🔄 '{index_name}' 인덱스 재생성 중...")
    
    # 기존 데이터 백업 (옵션)
    if backup_data and check_index_exists(opensearch_client, index_name):
        print(f"📦 '{index_name}' 데이터 백업 중... (구현 필요)")
        # TODO: 실제 백업 로직 구현
    
    # 1. 기존 인덱스 삭제
    delete_result = delete_index(opensearch_client, index_name)
    if delete_result is None:
        print(f"❌ '{index_name}' 인덱스 삭제 실패")
        return None
    
    # 2. 새 인덱스 생성
    result = define_entity_index(opensearch_client, index_name)
    
    if result and result.get('acknowledged'):
        print(f"✅ '{index_name}' 인덱스 재생성 완료!")
        
        # 매핑 검증 및 출력
        if validate_entity_mapping(opensearch_client, index_name):
            mapping = opensearch_client.indices.get_mapping(index=index_name)
            summary_vec_mapping = mapping[index_name]['mappings']['properties']['entity']['properties']['summary_vec']
            print(f"📋 summary_vec 필드 타입: {summary_vec_mapping['type']}")
            print(f"📏 벡터 차원: {summary_vec_mapping['dimension']}")
            print(f"🔧 벡터 엔진: {summary_vec_mapping['method']['engine']}")
        
        return result
    else:
        print(f"❌ '{index_name}' 인덱스 재생성 실패!")
        return None


def recreate_chunk_index(opensearch_client, index_name="chunks_book", backup_data=False):
    """
    청크 인덱스 재생성 (기존 삭제 후 새로 생성)
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        index_name: 인덱스 이름
        backup_data: 기존 데이터 백업 여부 (현재는 로그만 출력)
    """
    print(f"🔄 '{index_name}' 인덱스 재생성 중...")
    
    # 기존 데이터 백업 (옵션)
    if backup_data and check_index_exists(opensearch_client, index_name):
        print(f"📦 '{index_name}' 데이터 백업 중... (구현 필요)")
        # TODO: 실제 백업 로직 구현
    
    # 1. 기존 인덱스 삭제
    delete_result = delete_index(opensearch_client, index_name)
    if delete_result is None:
        print(f"❌ '{index_name}' 인덱스 삭제 실패")
        return None
    
    # 2. 새 인덱스 생성
    result = define_chunk_index(opensearch_client, index_name)
    
    if result and result.get('acknowledged'):
        print(f"✅ '{index_name}' 인덱스 재생성 완료!")
        
        # 매핑 검증 및 출력
        if validate_chunk_mapping(opensearch_client, index_name):
            mapping = opensearch_client.indices.get_mapping(index=index_name)
            summary_vec_mapping = mapping[index_name]['mappings']['properties']['chunk']['properties']['summary_vec']
            print(f"📋 summary_vec 필드 타입: {summary_vec_mapping['type']}")
            print(f"📏 벡터 차원: {summary_vec_mapping['dimension']}")
            print(f"🔧 벡터 엔진: {summary_vec_mapping['method']['engine']}")
        
        return result
    else:
        print(f"❌ '{index_name}' 인덱스 재생성 실패!")
        return None


def create_or_validate_index(opensearch_client, index_name, index_type="entity"):
    """
    인덱스 생성 또는 검증 (통합 함수)
    
    Args:
        opensearch_client: OpenSearch 클라이언트
        index_name: 인덱스 이름
        index_type: 인덱스 타입 ("entity" 또는 "chunk")
    
    Returns:
        dict: 생성/검증 결과
    """
    print(f"🔍 '{index_name}' 인덱스 확인 중...")
    
    if index_type == "entity":
        create_func = define_entity_index
        validate_func = validate_entity_mapping
    elif index_type == "chunk":
        create_func = define_chunk_index
        validate_func = validate_chunk_mapping
    else:
        print(f"❌ 지원하지 않는 인덱스 타입: {index_type}")
        return None
    
    # 인덱스 존재 확인
    if check_index_exists(opensearch_client, index_name):
        print(f"📋 '{index_name}' 인덱스가 이미 존재합니다")
        
        # 매핑 검증
        if validate_func(opensearch_client, index_name):
            print(f"✅ '{index_name}' 인덱스 매핑이 유효합니다")
            return {"status": "valid", "action": "none", "index": index_name}
        else:
            print(f"⚠️ '{index_name}' 인덱스 매핑이 유효하지 않습니다")
            return {"status": "invalid_mapping", "action": "recreate_needed", "index": index_name}
    else:
        print(f"🆕 '{index_name}' 인덱스를 생성합니다")
        result = create_func(opensearch_client, index_name)
        
        if result and result.get('acknowledged'):
            return {"status": "created", "action": "created", "index": index_name, "result": result}
        else:
            return {"status": "creation_failed", "action": "failed", "index": index_name}


if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from opensearch.opensearch_con import get_opensearch_client
    
    client = get_opensearch_client()
    
    print("� OpvenSearch 인덱스 관리 도구")
    print("=" * 50)
    
    # 사용자 입력 받기 (기본값: movie_graph 재생성)
    if len(sys.argv) > 1:
        action = sys.argv[1]
        index_name = sys.argv[2] if len(sys.argv) > 2 else "movie_graph"
    else:
        action = "recreate"
        index_name = "movie_graph"
    
    print(f"📋 Action: {action}")
    print(f"📋 Index: {index_name}")
    print("-" * 50)
    
    if action == "recreate":
        # 인덱스 재생성
        result = recreate_entity_index(client, index_name)
        
        if result:
            print(f"\n🎉 '{index_name}' 인덱스 재생성 성공!")
        else:
            print(f"\n💥 '{index_name}' 인덱스 재생성 실패!")
            
    elif action == "create":
        # 인덱스 생성 (존재하지 않을 때만)
        result = create_or_validate_index(client, index_name, "entity")
        print(f"\n📊 결과: {result}")
        
    elif action == "validate":
        # 인덱스 검증만
        if check_index_exists(client, index_name):
            if validate_entity_mapping(client, index_name):
                print(f"\n✅ '{index_name}' 인덱스 매핑이 유효합니다!")
            else:
                print(f"\n❌ '{index_name}' 인덱스 매핑이 유효하지 않습니다!")
        else:
            print(f"\n❌ '{index_name}' 인덱스가 존재하지 않습니다!")
            
    elif action == "check":
        # 인덱스 설정 확인
        result = check_index_settings(client, index_name)
        if result:
            print(f"\n📊 '{index_name}' 인덱스 정보 조회 완료!")
        else:
            print(f"\n❌ '{index_name}' 인덱스 정보 조회 실패!")
            
    else:
        print(f"❌ 지원하지 않는 액션: {action}")
        print("사용 가능한 액션: recreate, create, validate, check")
        print("사용법: python opensearch_index_setting.py [action] [index_name]")
        print("예시: python opensearch_index_setting.py recreate entities_book")