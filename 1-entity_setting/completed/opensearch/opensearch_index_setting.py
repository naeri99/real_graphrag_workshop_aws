# from utils.bedrock_embedding import create_embeddings
import json


def validate_entity_mapping(opensearch_client, index_name: str) -> bool:
    """OpenSearch 인덱스 존재 및 매핑 검증"""
    print(f"🔍 '{index_name}' 인덱스 검증 중...")
    
    if not opensearch_client.indices.exists(index=index_name):
        print(f"❌ 인덱스 '{index_name}'가 존재하지 않습니다!")
        print("💡 먼저 올바른 매핑으로 인덱스를 생성해주세요")
        return False
    
    try:
        mapping = opensearch_client.indices.get_mapping(index=index_name)
        properties = mapping.get(index_name, {}).get('mappings', {}).get('properties', {})
        entity_props = properties.get('entity', {}).get('properties', {})
        
        summary_vec_field = entity_props.get('summary_vec', {})
        vec_type = summary_vec_field.get('type')
        vec_dimension = summary_vec_field.get('dimension')
        
        if vec_type != 'knn_vector':
            print(f"❌ summary_vec 필드 타입이 올바르지 않습니다: {vec_type} (예상: knn_vector)")
            return False
        
        if vec_dimension != 1024:
            print(f"❌ summary_vec 차원이 올바르지 않습니다: {vec_dimension} (예상: 1024)")
            return False
        
        print(f"✅ 인덱스 매핑 검증 완료 (타입: {vec_type}, 차원: {vec_dimension})")
        return True
        
    except Exception as e:
        print(f"❌ 인덱스 매핑 검증 실패: {e}")
        return False

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



def validate_opensearch_index(opensearch_client, index_name: str) -> bool:
    """OpenSearch 인덱스 존재 및 매핑 검증"""
    print(f"🔍 '{index_name}' 인덱스 검증 중...")
    
    if not opensearch_client.indices.exists(index=index_name):
        print(f"❌ 인덱스 '{index_name}'가 존재하지 않습니다!")
        print("💡 먼저 올바른 매핑으로 인덱스를 생성해주세요")
        return False
    
    try:
        mapping = opensearch_client.indices.get_mapping(index=index_name)
        properties = mapping.get(index_name, {}).get('mappings', {}).get('properties', {})
        entity_props = properties.get('entity', {}).get('properties', {})
        
        summary_vec_field = entity_props.get('summary_vec', {})
        vec_type = summary_vec_field.get('type')
        vec_dimension = summary_vec_field.get('dimension')
        
        if vec_type != 'knn_vector':
            print(f"❌ summary_vec 필드 타입이 올바르지 않습니다: {vec_type} (예상: knn_vector)")
            return False
        
        if vec_dimension != 1024:
            print(f"❌ summary_vec 차원이 올바르지 않습니다: {vec_dimension} (예상: 1024)")
            return False
        
        print(f"✅ 인덱스 매핑 검증 완료 (타입: {vec_type}, 차원: {vec_dimension})")
        return True
        
    except Exception as e:
        print(f"❌ 인덱스 매핑 검증 실패: {e}")
        return False



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


def define_chunk_index(opensearch_client, index_name):
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
                "chunk": {
                    "properties": {
                        "context": {
                            "type": "keyword",
                            "fields": {
                                "text": {
                                    "type": "text",
                                    "analyzer": "nori_analyzer"
                                }
                            }
                        },
                        "context_vec": {
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
          
        return response
    except Exception as e:
        print(f"❌ Error creating entity index '{index_name}': {e}")
        return None

