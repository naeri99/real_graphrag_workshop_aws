"""
Entity Extraction from Chunk Pipeline
Flow:
1. Extract entities from chunk (LLM)
2. Resolve entity names via OpenSearch (synonym matching)
3. Save entities to Neptune
4. Resolve relationship names using cache
5. Save relationships to Neptune
"""
from langchain_text_splitters import RecursiveCharacterTextSplitter
from utils.parse_utils import parse_extraction_output
from utils.generate_entity import extract_entities
from utils.helper import (
    get_context_from_review_file, 
    get_all_review_files,
    generate_chunk_hash,
    generate_chunk_id
)
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import resolve_entities, resolve_relationships, delete_chunk_index_opensearch
from neptune.cyper_queries import (
    import_nodes_with_dynamic_label,
    import_relationships_with_dynamic_label,
    delete_all_nodes_and_relationships,
    get_database_stats
)


def run_entity_extraction_pipeline(
    reviews_dir: str = None,
    chunk_size: int = 1500,
    chunk_overlap: int = 100,
    clean_database: bool = True,
    save_to_neptune: bool = True
):
    """
    전체 엔티티 추출 파이프라인
    
    Args:
        reviews_dir: 리뷰 파일 디렉토리 (None이면 기본 경로)
        chunk_size: 청크 크기
        chunk_overlap: 청크 오버랩
        clean_database: Neptune DB 초기화 여부
        save_to_neptune: Neptune에 저장 여부
    """
    print("=" * 60)
    print("🚀 Entity Extraction Pipeline Start")
    print("=" * 60)
    
    # OpenSearch 초기화
    opensearch_client = get_opensearch_client()
    print("✅ OpenSearch connected")
    
    # Neptune 상태 확인
    if save_to_neptune:
        stats = get_database_stats()
        print(f"📊 Neptune: {stats['total_nodes']} nodes, {stats['total_relationships']} relationships")
        
        if clean_database:
            delete_all_nodes_and_relationships()
            print("🗑️ Database cleaned")
            delete_chunk_index_opensearch()
    
    # 리뷰 파일 가져오기
    if reviews_dir:
        from pathlib import Path
        review_files = list(Path(reviews_dir).rglob("*.json"))
    else:
        review_files = get_all_review_files()
    
    print(f"📂 Found {len(review_files)} review files")
    
    # 텍스트 스플리터
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    
    # 전체 통계
    total = {
        'entities_extracted': 0,
        'entities_matched': 0,
        'entities_new': 0,
        'entities_saved': 0,
        'entities_existing_in_neptune': 0,
        'entities_new_in_neptune': 0,
        'relationships_extracted': 0,
        'relationships_saved': 0,
        'relationships_existing_in_neptune': 0,
        'relationships_new_in_neptune': 0,
        'chunks_processed': 0
    }
    
    for i, review_file in enumerate(review_files, 1):
        print(f"\n{'='*60}")
        print(f"📄 [{i}/{len(review_files)}] {review_file.name}")
        print('='*60)
        
        try:
            # 파일에서 정보 추출
            _, transcript, movie_id, reviewer = get_context_from_review_file(str(review_file))
            print(f"   🎬 Movie: {movie_id}, Reviewer: {reviewer}")
            
            # 청킹
            chunks = text_splitter.split_text(transcript)
            print(f"   📝 Chunks: {len(chunks)}")
            
            for j, chunk in enumerate(chunks, 1):
                print(f"\n   --- Chunk {j}/{len(chunks)} ---")
                print(f"f{chunk[:800]}... 생략 ...")
                print(f"\n   --- Chunk Transformation ---")

                # 청크 ID 생성
                chunk_hash = generate_chunk_hash(chunk)
                chunk_id = generate_chunk_id(reviewer, chunk_hash)
                
                # Step 1: LLM으로 엔티티/관계 추출
                result = extract_entities({"user_query": chunk})
                entities, relationships = parse_extraction_output(result)
                
                total['entities_extracted'] += len(entities)
                total['relationships_extracted'] += len(relationships)
                
                if not entities:
                    print("   ⚠️ No entities found")
                    total['chunks_processed'] += 1
                    continue
                
                print(f"   📝 Extracted: {len(entities)} entities, {len(relationships)} relationships")
                
                # Step 2: OpenSearch에서 엔티티 이름 해결
                resolved_entities, entity_metrics = resolve_entities(entities, opensearch_client)
                
                total['entities_matched'] += entity_metrics['matched']
                total['entities_new'] += entity_metrics['new']
                
                # 엔티티 매칭 결과 출력
                print(f"\n   📊 Entity Resolution: Search Entity in Neptune via OpenSearch Synonym")
                for ent in resolved_entities:
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
                        print(f"      🆕 '{original}' ({etype}) [NEW]")
                
                # Step 3: Neptune에 엔티티 저장
                if save_to_neptune and resolved_entities:
                    # 메타데이터 제거 후 저장
                    clean_entities = []
                    for ent in resolved_entities:
                        clean_ent = {k: v for k, v in ent.items() if not k.startswith('_')}
                        clean_entities.append(clean_ent)
                    
                    save_result = import_nodes_with_dynamic_label(clean_entities, movie_id, reviewer, chunk_id, chunk, chunk_hash)
                    entity_stats = save_result.get('stats', {})
                    total['entities_saved'] += entity_stats.get('total', len(clean_entities))
                    total['entities_existing_in_neptune'] += entity_stats.get('existing', 0)
                    total['entities_new_in_neptune'] += entity_stats.get('new', 0)
                    
                    # 기존/신규 entity 통계 출력
                    existing_count = entity_stats.get('existing', 0)
                    new_count = entity_stats.get('new', 0)
                    total_count = entity_stats.get('total', 0)
                    print(f"   💾 Saved {total_count} entities to Neptune (DB에 기존 존재: {existing_count}, 신규 생성: {new_count})")
                
                # Step 4 & 5: 관계 처리 및 저장
                if relationships:
                    # 관계의 엔티티 이름도 해결
                    resolved_relationships, rel_metrics = resolve_relationships(
                        relationships, opensearch_client
                    )
                    
                    print(f"\n   🔗 Relationship Resolution:")
                    for rel in resolved_relationships[:5]:  # 처음 5개만 출력
                        src = rel.get('source_entity', '')
                        tgt = rel.get('target_entity', '')
                        print(f"      {src} → {tgt}")
                    if len(resolved_relationships) > 5:
                        print(f"      ... and {len(resolved_relationships) - 5} more")
                    
                    # Neptune에 관계 저장
                    if save_to_neptune and resolved_relationships:
                        # 메타데이터 제거 후 저장
                        clean_rels = []
                        for rel in resolved_relationships:
                            clean_rel = {k: v for k, v in rel.items() if not k.startswith('_')}
                            clean_rels.append(clean_rel)
                        
                        rel_save_result = import_relationships_with_dynamic_label(clean_rels)
                        rel_stats = rel_save_result.get('stats', {})
                        total['relationships_saved'] += rel_stats.get('total', len(clean_rels))
                        total['relationships_existing_in_neptune'] += rel_stats.get('existing', 0)
                        total['relationships_new_in_neptune'] += rel_stats.get('new', 0)
                        
                        # 기존/신규 relationship 통계 출력
                        rel_existing = rel_stats.get('existing', 0)
                        rel_new = rel_stats.get('new', 0)
                        rel_total = rel_stats.get('total', 0)
                        print(f"   💾 Saved {rel_total} relationships to Neptune (DB에 기존 존재: {rel_existing}, 신규 생성: {rel_new})")
                
                total['chunks_processed'] += 1
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    # 최종 결과
    print("\n" + "=" * 60)
    print("🎉 Pipeline Complete!")
    print("=" * 60)
    print(f"Chunks processed: {total['chunks_processed']}")
    print(f"Entities extracted: {total['entities_extracted']}")
    print(f"  - Matched in OpenSearch: {total['entities_matched']}")
    print(f"  - New (not found): {total['entities_new']}")
    print(f"  - Saved to Neptune: {total['entities_saved']}")
    print(f"    └─ DB에 기존 존재 (업데이트): {total['entities_existing_in_neptune']}")
    print(f"    └─ 신규 생성: {total['entities_new_in_neptune']}")
    print(f"Relationships extracted: {total['relationships_extracted']}")
    print(f"  - Saved to Neptune: {total['relationships_saved']}")
    print(f"    └─ DB에 기존 존재 (업데이트): {total['relationships_existing_in_neptune']}")
    print(f"    └─ 신규 생성: {total['relationships_new_in_neptune']}")
    
    if save_to_neptune:
        final_stats = get_database_stats()
        print(f"\n📊 Final Neptune Stats:")
        print(f"  - Total nodes: {final_stats['total_nodes']}")
        print(f"  - Total relationships: {final_stats['total_relationships']}")
    
    return total


def run_entity_check_pipeline(reviews_dir: str = None):
    """
    LLM으로 엔티티 추출 후 OpenSearch에서 매칭 확인만 수행
    (Neptune 저장 없음 - 테스트용)
    """
    return run_entity_extraction_pipeline(
        reviews_dir=reviews_dir,
        save_to_neptune=False,
        clean_database=False
    )


if __name__ == "__main__":
    # 전체 파이프라인 실행 (Neptune 저장 포함)
    run_entity_extraction_pipeline(
        reviews_dir="../../data/reviews/DonghoonChoi",  # 기본 경로 사용
        chunk_size=1500,
        chunk_overlap=100,
        clean_database=True,
        save_to_neptune=True
    )
