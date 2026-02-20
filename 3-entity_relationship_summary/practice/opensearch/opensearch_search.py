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



def refresh_opensearch_index(opensearch_client, index_name: str):
    """OpenSearch 인덱스 refresh"""
    try:
        opensearch_client.indices.refresh(index=index_name)
        print("🔄 인덱스 refresh 완료")
    except Exception as e:
        print(f"⚠️ 인덱스 refresh 실패: {e}")
