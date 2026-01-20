"""
OpenSearch 저장 유틸리티
- 3-entity_relationship_summary에서 사용하는 함수만 포함
"""
from opensearch.opensearch_con import get_opensearch_client


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
