"""
OpenSearch 엔티티 검색 및 저장 유틸리티
"""
from typing import Dict, List, Tuple
from opensearch.opensearch_con import get_opensearch_client


# ============ OpenSearch 저장 관련 함수 ============

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


def save_entity_to_opensearch(
    opensearch_client,
    index_name: str,
    embedder,
    name: str,
    entity_type: str,
    summary: str,
    neptune_id: str
) -> str:
    """
    단일 엔티티를 OpenSearch에 저장
    
    Returns:
        str: 'saved', 'skipped', 'failed'
    """
    try:
        # 기존 문서 존재 확인
        try:
            existing_doc = opensearch_client.get(index=index_name, id=neptune_id)
            if existing_doc.get('found'):
                print(f"   ⏭️ 이미 존재함: {name} ({entity_type})")
                return "skipped"
        except:
            pass  # 문서가 존재하지 않음 (정상)
        
        # 임베딩 생성
        summary_vec = embedder.embed_text(summary)
        
        # 벡터 검증
        if not isinstance(summary_vec, list) or len(summary_vec) != 1024:
            print(f"   ❌ 벡터 오류: {name}")
            return "failed"
        
        # OpenSearch에 저장
        doc = {
            "entity": {
                "name": name,
                "entity_type": entity_type,
                "summary": summary,
                "summary_vec": summary_vec,
                "neptune_id": neptune_id
            }
        }
        
        response = opensearch_client.index(
            index=index_name,
            id=neptune_id,
            body=doc,
            refresh=False
        )
        
        if response and response.get('result') in ['created', 'updated']:
            return "saved"
        else:
            print(f"   ❌ 저장 실패: {name}")
            return "failed"
            
    except Exception as e:
        print(f"   ❌ 오류 ({name}): {e}")
        return "failed"


def refresh_opensearch_index(opensearch_client, index_name: str):
    """OpenSearch 인덱스 refresh"""
    try:
        opensearch_client.indices.refresh(index=index_name)
        print("🔄 인덱스 refresh 완료")
    except Exception as e:
        print(f"⚠️ 인덱스 refresh 실패: {e}")


def search_entity_by_neptune_id(neptune_id: str, index_name: str = "entities"):
    """Neptune ID로 OpenSearch에서 엔티티 검색"""
    try:
        opensearch_client = get_opensearch_client()
        response = opensearch_client.get(index=index_name, id=neptune_id)
        return response['_source']
    except Exception as e:
        print(f"❌ Error searching entity by neptune_id {neptune_id}: {e}")
        return None


# ============ OpenSearch 검색 관련 함수 ============


def search_entity_in_opensearch(
    entity_name: str, 
    entity_type: str, 
    opensearch_client=None, 
    index_name: str = "entities"
) -> Tuple[str, bool, str]:
    """
    OpenSearch에서 동의어를 우선으로 엔티티를 검색하여 정확한 이름을 찾습니다.
    
    Returns:
        tuple: (정확한 엔티티 이름, 매칭 여부, 매칭 타입)
        매칭 타입: 'synonym_exact', 'synonym_partial', 'name_exact', 'not_found'
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    entity_name = entity_name.strip() if entity_name else ""
    entity_type = entity_type.strip() if entity_type else ""
    
    if not entity_name or not entity_type:
        return entity_name, False, 'not_found'
    
    try:
        # 1. 해당 타입의 모든 엔티티에서 동의어 검색
        search_body = {
            "query": {
                "term": {
                    "entity.entity_type": entity_type
                }
            },
            "size": 100,
            "_source": ["entity.name", "entity.synonym", "entity.entity_type"]
        }
        
        response = opensearch_client.search(index=index_name, body=search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        for hit in hits:
            entity = hit['_source'].get('entity', {})
            entity_real_name = entity.get('name', '').strip()
            synonyms = entity.get('synonym', '')
            
            if not synonyms:
                continue
            
            if isinstance(synonyms, str):
                synonym_list = [s.strip() for s in synonyms.split(',') if s.strip()]
            else:
                synonym_list = synonyms if isinstance(synonyms, list) else []
            
            # 정확한 매칭
            if entity_name in synonym_list:
                return entity_real_name, True, 'synonym_exact'
            
            # 부분 매칭
            if any(entity_name in syn for syn in synonym_list):
                return entity_real_name, True, 'synonym_partial'
        
        # 2. 정확한 이름 매칭 시도
        exact_search_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {"term": {"entity.name.keyword": {"value": entity_name, "boost": 3.0}}},
                                    {"match": {"entity.name": {"query": entity_name, "operator": "and", "boost": 2.0}}}
                                ]
                            }
                        },
                        {"term": {"entity.entity_type": entity_type}}
                    ]
                }
            },
            "size": 1,
            "_source": ["entity.name"]
        }
        
        response = opensearch_client.search(index=index_name, body=exact_search_body)
        hits = response.get('hits', {}).get('hits', [])
        
        if hits:
            exact_name = hits[0]['_source'].get('entity', {}).get('name', entity_name).strip()
            return exact_name, True, 'name_exact'
        
        return entity_name, False, 'not_found'
        
    except Exception as e:
        print(f"   ❌ OpenSearch 검색 오류: {e}")
        return entity_name, False, 'not_found'


def resolve_entities(entities: List[Dict], opensearch_client=None, index_name: str = "entities") -> Tuple[List[Dict], Dict]:
    """
    엔티티 리스트를 OpenSearch를 통해 해결하고 메트릭을 반환합니다.
    
    Returns:
        tuple: (해결된 엔티티 리스트, 메트릭)
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not entities:
        return [], {'matched': 0, 'new': 0, 'synonym_exact': 0, 'synonym_partial': 0, 'name_exact': 0}
    
    resolved = []
    metrics = {'matched': 0, 'new': 0, 'synonym_exact': 0, 'synonym_partial': 0, 'name_exact': 0}
    
    for entity in entities:
        original_name = entity.get('entity_name', '').strip()
        entity_type = entity.get('entity_type', '').strip()
        
        if not original_name or not entity_type:
            resolved.append(entity)
            continue
        
        resolved_name, found, match_type = search_entity_in_opensearch(
            original_name, entity_type, opensearch_client, index_name
        )
        
        if found:
            metrics['matched'] += 1
            metrics[match_type] += 1
        else:
            metrics['new'] += 1
        
        updated = entity.copy()
        updated['entity_name'] = resolved_name
        updated['_original_name'] = original_name
        updated['_matched'] = found
        updated['_match_type'] = match_type
        resolved.append(updated)
    
    return resolved, metrics


def resolve_relationships(relationships: List[Dict], opensearch_client=None, index_name: str = "entities") -> Tuple[List[Dict], Dict]:
    """
    관계 리스트의 엔티티 이름들을 OpenSearch를 통해 정확한 이름으로 변환합니다.
    
    Args:
        relationships: 관계 리스트
        opensearch_client: OpenSearch 클라이언트
        index_name: 검색할 인덱스 이름
        
    Returns:
        tuple: (해결된 관계 리스트, 메트릭)
    """
    if opensearch_client is None:
        opensearch_client = get_opensearch_client()
    
    if not relationships:
        return [], {'source_matched': 0, 'target_matched': 0, 'source_new': 0, 'target_new': 0}
    
    resolved = []
    metrics = {'source_matched': 0, 'target_matched': 0, 'source_new': 0, 'target_new': 0}
    
    for rel in relationships:
        updated = rel.copy()
        
        # source_entity 처리
        source_name = rel.get('source_entity', '').strip()
        source_type = rel.get('source_type', '').strip()
        
        if source_name and source_type:
            resolved_source, found, _ = search_entity_in_opensearch(
                source_name, source_type, opensearch_client, index_name
            )
            updated['source_entity'] = resolved_source
            updated['_source_original'] = source_name
            updated['_source_matched'] = found
            
            if found:
                metrics['source_matched'] += 1
            else:
                metrics['source_new'] += 1
        
        # target_entity 처리
        target_name = rel.get('target_entity', '').strip()
        target_type = rel.get('target_type', '').strip()
        
        if target_name and target_type:
            resolved_target, found, _ = search_entity_in_opensearch(
                target_name, target_type, opensearch_client, index_name
            )
            updated['target_entity'] = resolved_target
            updated['_target_original'] = target_name
            updated['_target_matched'] = found
            
            if found:
                metrics['target_matched'] += 1
            else:
                metrics['target_new'] += 1
        
        resolved.append(updated)
    
    return resolved, metrics
