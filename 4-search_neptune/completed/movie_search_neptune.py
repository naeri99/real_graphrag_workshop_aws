"""
Movie Search Neptune
- 사용자 쿼리에서 엔티티 추출
- OpenSearch로 엔티티 이름 해결
- Cypher 쿼리 생성 및 실행
"""
from utils.smart_search_llm import SmartGraphSearchLLM
from utils.generate_entity import extract_entity_from_search
from utils.parse_utils import parse_search_context
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import (
    search_entity_in_opensearch,
    resolve_entities_with_opensearch
)


def search_specific_queries(queries):
    """특정 쿼리들에 대해 엔티티 추출 → OpenSearch 해결 → Cypher 쿼리 실행"""
    search = SmartGraphSearchLLM()
    opensearch_client = get_opensearch_client()
    
    
    for i, query in enumerate(queries, 1):
        print(f"\n{'='*80}")
        print(f"쿼리 {i}: {query}")
        print('='*80)
        
        # 1단계: 쿼리에서 엔티티 추출
        print("🔍 1단계: 엔티티 추출 중...")
        try:
            result = extract_entity_from_search({"user_query": query})
            entities = parse_search_context(result)
            
            print(f"✅ 추출된 엔티티: {entities}")
            
            if not entities:
                print("⚠️ 추출된 엔티티가 없습니다.")
                continue
                
        except Exception as e:
            print(f"❌ 엔티티 추출 오류: {e}")
            continue
        
        # 2단계: OpenSearch를 통한 엔티티 이름 해결
        print(f"\n🔄 2단계: OpenSearch를 통한 엔티티 이름 해결...")
        try:
            resolved_mapping = resolve_entities_with_opensearch(entities, opensearch_client)
            
            print(f"✅ 이름 해결 완료:")
            for original, resolved in resolved_mapping.items():
                print(f"   '{original}' → '{resolved}'")
                
        except Exception as e:
            print(f"❌ 엔티티 이름 해결 오류: {e}")
            resolved_mapping = {entity: entity for entity in entities}
        
        # 3단계: 해결된 엔티티 이름으로 쿼리 업데이트
        print(f"\n🔄 3단계: 쿼리 업데이트...")
        updated_query = query
        for original, resolved in resolved_mapping.items():
            if original != resolved:
                updated_query = updated_query.replace(original, resolved)
        
        print(f"원본 쿼리: {query}")
        print(f"업데이트된 쿼리: {updated_query}")
        
        # 4단계: SmartGraphSearchLLM으로 Cypher 쿼리 실행
        print(f"\n🚀 4단계: Cypher 쿼리 실행...")
        try:
            search_result = search.smart_search(updated_query)
            
            print(f"성공: {search_result['success']}")
            if search_result['success']:
                print(f"Cypher 쿼리: {search_result['cypher_query']}")
                print(f"결과 수: {search_result['results_count']}")
                print(f"\n📋 요약:")
                print(search_result['summary'])
                
                if search_result.get('results'):
                    print(f"\n📊 상세 결과 (처음 3개):")
                    for j, row in enumerate(search_result['results'][:3], 1):
                        print(f"  {j}. {row}")
            else:
                print(f"❌ 오류: {search_result['error']}")
                
        except Exception as e:
            print(f"❌ Cypher 쿼리 실행 오류: {e}")
        
        print(f"\n{'='*80}")
        print(f"쿼리 {i} 처리 완료")
        print('='*80)



if __name__ == "__main__":
    queries = [
        "썬더와 가든 그리고 이안은 어떤 관계야?",
        "대길과 고광렬은 어떤 관계야?",
        "김윤석이는 연기한 캐릭터는?",
        "전우치와 무륵의 관계는 어떤관계가 있는지 가변길이를 활용해서 찾아줘"

    ]
    search_specific_queries(queries)
