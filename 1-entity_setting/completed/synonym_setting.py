"""
Entity Setting - 엔티티 동의어 추출 및 OpenSearch 저장 파이프라인

흐름:
1. 청크에서 엔티티 기준으로 동의어 추출
2. OpenSearch에서 엔티티 이름으로 검색
3. 기존 동의어와 새 동의어 병합 (strip() + set)
4. OpenSearch에 저장
"""
from opensearch.opensearch_index_setting import delete_index, define_entity_index
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearh_search import find_entity_opensearch
from utils.parse_utils import parse_mixed_synonym_output
from utils.generate_entity import extract_synonym
from utils.synonym import (
    clean_entities_whitespace,
    merge_synonyms_with_set,
    update_entity_synonyms
)
from utils.movie_context import get_context_from_review_file, get_all_review_files
from langchain_text_splitters import RecursiveCharacterTextSplitter
import os




# ============================================================
# 청킹 함수
# ============================================================
def chunk_text(text: str, chunk_size: int = 1500, chunk_overlap: int = 100) -> list:
    """텍스트를 청크로 분할"""
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    return text_splitter.split_text(text)


# ============================================================
# 파이프라인 함수들
# ============================================================
def find_synonyms_from_chunk(chunk: str, context: str) -> list:
    """Step 2: 청크에서 동의어 찾기 (추출 + 공백 제거)"""
    result = extract_synonym({"movie_context": context, "movie_chunk": chunk})
    entities = parse_mixed_synonym_output(result)
    if not entities:
        return []
    return clean_entities_whitespace(entities)


def find_entity_from_opensearch(opensearch_client, entity_name: str, index_name: str = "entities"):
    """Step 3: OpenSearch에서 엔티티 검색"""
    return find_entity_opensearch(opensearch_client, entity_name, index_name)


def add_synonyms_to_entity(opensearch_client, existing_entity: dict, new_synonyms: list, index_name: str = "entities") -> dict:
    """Step 4: 엔티티에 동의어 추가 (병합 + 저장)"""
    existing_synonyms = existing_entity['entity'].get('synonym', [])
    merged_synonyms = merge_synonyms_with_set(existing_synonyms, new_synonyms)
    
    # set을 list로 변환하여 저장
    merged_synonyms_list = list(merged_synonyms) if isinstance(merged_synonyms, set) else merged_synonyms
    
    success = update_entity_synonyms(opensearch_client, existing_entity['id'], merged_synonyms_list, index_name)
    return {'success': success, 'merged_synonyms': merged_synonyms_list}


def print_final_stats(stats: dict):
    """최종 결과 출력"""
    print("\n" + "=" * 60)
    print("🎯 파이프라인 완료!")
    print(f"   전체 엔티티: {stats['total_entities']}개")
    print(f"   업데이트 성공: {stats['updated']}개")
    print(f"   엔티티 없음: {stats['not_found']}개")
    print(f"   업데이트 실패: {stats['failed']}개")


# ============================================================
# 리뷰 디렉토리 기반 파이프라인
# ============================================================
def process_single_review_file(opensearch_client, review_filepath: str, stats: dict):
    """
    단일 리뷰 파일 처리
    
    흐름:
    1. 리뷰 파일에서 context, transcript 추출
    2. transcript를 청크로 분할
    3. 각 청크에서 동의어 추출
    4. OpenSearch에서 엔티티 검색 후 동의어 추가
    """
    filename = os.path.basename(review_filepath)
    print(f"\n📄 파일 처리: {filename}")
    
    # Step 1: context, transcript 추출
    context, transcript = get_context_from_review_file(review_filepath)
    print(f"   📝 Context 생성 완료")
    
    # Step 2: transcript를 청크로 분할
    chunks = chunk_text(transcript)
    print(f"   📦 {len(chunks)}개 청크 생성")
    
    # Step 3-4: 각 청크 처리
    for i, chunk in enumerate(chunks, 1):
        # 청크에서 동의어 찾기
        entities = find_synonyms_from_chunk(chunk, context)
        if not entities:
            continue
        
        for entity_data in entities:
            entity_name = entity_data['entity_name']
            new_synonyms = entity_data['synonyms']
            stats['total_entities'] += 1
            
            # OpenSearch에서 엔티티 검색
            existing_entity = find_entity_from_opensearch(opensearch_client, entity_name)
            if not existing_entity:
                stats['not_found'] += 1
                continue
            
            # 엔티티에 동의어 추가
            result = add_synonyms_to_entity(opensearch_client, existing_entity, new_synonyms)
            if result['success']:
                stats['updated'] += 1
                print(f"   ✅ {entity_name}: 동의어 업데이트")
            else:
                stats['failed'] += 1


def run_directory_pipeline(reviews_dir: str = None):
    """
    리뷰 디렉토리 기반 동의어 추출 파이프라인
    
    Args:
        reviews_dir: 리뷰 파일들이 있는 디렉토리 경로
                    기본값: /home/ec2-user/real_graphrag_workshop_aws/data/reviews/DonghoonChoi
    
    흐름:
    1. 디렉토리에서 모든 리뷰 파일 목록 가져오기
    2. 각 파일에서 context, transcript 추출
    3. transcript를 청크로 분할
    4. 청크에서 동의어 추출
    5. OpenSearch에서 엔티티 검색
    6. 엔티티에 동의어 추가
    """
    print("🚀 리뷰 디렉토리 기반 동의어 추출 파이프라인 시작")
    print("=" * 60)
    
    opensearch_client = get_opensearch_client()
    
    # Step 1: 리뷰 파일 목록 가져오기
    if reviews_dir:
        from pathlib import Path
        review_files = list(Path(reviews_dir).glob("*.json"))
    else:
        review_files = get_all_review_files()
    
    print(f"📂 총 {len(review_files)}개 리뷰 파일 발견")
    
    stats = {'total_entities': 0, 'updated': 0, 'not_found': 0, 'failed': 0, 'files_processed': 0}
    
    # Step 2-6: 각 파일 처리
    for file_idx, review_file in enumerate(review_files, 1):
        print(f"\n{'='*60}")
        print(f"📁 [{file_idx}/{len(review_files)}] 파일 처리 중")
        
        try:
            process_single_review_file(opensearch_client, str(review_file), stats)
            stats['files_processed'] += 1
        except Exception as e:
            print(f"   ❌ 파일 처리 실패: {e}")
    
    # 결과 출력
    print("\n" + "=" * 60)
    print("🎯 디렉토리 파이프라인 완료!")
    print(f"   처리된 파일: {stats['files_processed']}개")
    print(f"   전체 엔티티: {stats['total_entities']}개")
    print(f"   업데이트 성공: {stats['updated']}개")
    print(f"   엔티티 없음: {stats['not_found']}개")
    print(f"   업데이트 실패: {stats['failed']}개")
    
    return stats


# ============================================================
# 실행
# ============================================================
if __name__ == "__main__":
    # 디렉토리 기반 파이프라인 실행
    run_directory_pipeline()
