"""
Entity Extraction from Chunk - LLM 추출 후 OpenSearch 확인
"""
from langchain_text_splitters import RecursiveCharacterTextSplitter
from utils.parse_utils import parse_extraction_output
from utils.generate_entity import extract_entities
from utils.helper import get_context_from_review_file, get_all_review_files
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import search_entity_in_opensearch, resolve_entities


def run_entity_check_pipeline(reviews_dir: str = None):
    """
    LLM으로 엔티티 추출 후 OpenSearch에서 매칭 확인만 수행
    (Neptune 저장 없음)
    """
    print("=" * 60)
    print("🔍 Entity Extraction & OpenSearch Check Pipeline")
    print("=" * 60)
    
    # OpenSearch 초기화
    opensearch_client = get_opensearch_client()
    print("✅ OpenSearch connected")
    
    # 리뷰 파일 가져오기
    if reviews_dir:
        from pathlib import Path
        review_files = list(Path(reviews_dir).rglob("*.json"))
    else:
        review_files = get_all_review_files()
    
    print(f"📂 Found {len(review_files)} review files")
    
    # 텍스트 스플리터
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1500, chunk_overlap=100)
    
    # 전체 통계
    total = {'entities': 0, 'matched': 0, 'new': 0, 'synonym_exact': 0, 'synonym_partial': 0, 'name_exact': 0}
    
    for i, review_file in enumerate(review_files, 1):
        print(f"\n{'='*60}")
        print(f"📄 [{i}/{len(review_files)}] {review_file.name}")
        print('='*60)
        
        try:
            # 파일에서 정보 추출
            _, transcript, movie_id, reviewer = get_context_from_review_file(str(review_file))
            print(f"   Movie: {movie_id}, Reviewer: {reviewer}")
            
            # 청킹
            chunks = text_splitter.split_text(transcript)
            print(f"   Chunks: {len(chunks)}")
            
            for j, chunk in enumerate(chunks, 1):
                print(f"\n   --- Chunk {j}/{len(chunks)} ---")
                
                # Step 1: LLM으로 엔티티 추출
                result = extract_entities({"user_query": chunk})
                entities, relationships = parse_extraction_output(result)
                
                if not entities:
                    print("   ⚠️ No entities found")
                    continue
                
                print(f"   📝 Extracted {len(entities)} entities from LLM")
                
                # Step 2: OpenSearch에서 매칭 확인
                resolved, metrics = resolve_entities(entities, opensearch_client)
                
                total['entities'] += len(entities)
                total['matched'] += metrics['matched']
                total['new'] += metrics['new']
                total['synonym_exact'] += metrics['synonym_exact']
                total['synonym_partial'] += metrics['synonym_partial']
                total['name_exact'] += metrics['name_exact']
                
                # 결과 출력
                print(f"\n   📊 OpenSearch Matching Results:")
                for ent in resolved:
                    original = ent.get('_original_name', ent['entity_name'])
                    resolved_name = ent['entity_name']
                    etype = ent['entity_type']
                    matched = ent.get('_matched', False)
                    match_type = ent.get('_match_type', 'not_found')
                    
                    if matched:
                        if original != resolved_name:
                            print(f"      ✅ '{original}' → '{resolved_name}' ({etype}) [{match_type}]")
                        else:
                            print(f"      ✅ '{original}' ({etype}) [{match_type}]")
                    else:
                        print(f"      🆕 '{original}' ({etype}) [NEW - not in OpenSearch]")
                
                print(f"\n   Summary: {metrics['matched']} matched, {metrics['new']} new")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # 최종 결과
    print("\n" + "=" * 60)
    print("🎉 Pipeline Complete!")
    print("=" * 60)
    print(f"Total entities extracted: {total['entities']}")
    print(f"Matched in OpenSearch: {total['matched']}")
    print(f"New (not found): {total['new']}")
    print(f"\n=== Match Type Breakdown ===")
    print(f"  Synonym exact: {total['synonym_exact']}")
    print(f"  Synonym partial: {total['synonym_partial']}")
    print(f"  Name exact: {total['name_exact']}")
    
    return total


if __name__ == "__main__":
    run_entity_check_pipeline(reviews_dir=None)
