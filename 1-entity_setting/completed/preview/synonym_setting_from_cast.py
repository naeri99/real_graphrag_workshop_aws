"""
Entity Setting - movie_cast 기반 동의어 추출 및 OpenSearch 저장 파이프라인

데이터 소스:
  - movie_cast/*.json → 영화 정보(context): movie_title, director, cast
  - real/*/*.json (review 경로) → refined_transcript (chunk 대상)

흐름:
1. movie_cast JSON에서 영화별 context 생성
2. review 경로의 refined_transcript를 청크로 분할
3. 청크에서 동의어 추출
4. OpenSearch에서 엔티티 검색 후 동의어 추가
"""
import json, glob, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearh_search import find_entity_opensearch
from utils.parse_utils import parse_mixed_synonym_output
from utils.generate_entity import extract_synonym
from utils.synonym import (
    clean_entities_whitespace,
    merge_synonyms_with_set,
    update_entity_synonyms
)
from langchain_text_splitters import RecursiveCharacterTextSplitter


CAST_DIR = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir, os.pardir, "data", "raw_csv", "movie_cast"
))


# ============================================================
# movie_cast 기반 context 생성
# ============================================================
def build_context_from_cast(cast_data: dict) -> str:
    """movie_cast JSON에서 context 문자열 생성"""
    title = cast_data["movie_title"]
    directors = cast_data.get("director", [])
    cast = cast_data.get("cast", [])

    parts = [f"영화 {title}의 주요 등장인물과 배우 정보:", ""]
    for c in cast:
        parts.append(f"- {c['character']}: {c['actor']}이 연기한 캐릭터")

    parts.append("")
    parts.append(f"영화: {title}")
    if directors:
        parts.append(f"감독: {directors[0]['name']}")
    parts.append(f"총 {len(cast)}명의 배우가 {len(cast)}개의 캐릭터를 연기했습니다.")

    return "\n".join(parts)


def load_refined_transcripts(review_paths: list) -> list:
    """review 경로들에서 refined_transcript 로드, 없으면 스킵. (path, transcript) 튜플 반환"""
    results = []
    for rpath in review_paths:
        try:
            with open(rpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            rt = data.get("refined_transcript", "")
            if rt:
                results.append((rpath, rt))
            else:
                print(f"      ⏭️  refined_transcript 없음, 스킵: {os.path.basename(rpath)}")
        except Exception as e:
            print(f"      ⏭️  파일 로드 실패, 스킵: {os.path.basename(rpath)} ({e})")
    return results


# ============================================================
# 청킹 함수 (기존과 동일)
# ============================================================
def chunk_text(text: str, chunk_size: int = 1500, chunk_overlap: int = 100) -> list:
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )
    return text_splitter.split_text(text)


# ============================================================
# 파이프라인 함수들 (기존 핵심 로직 그대로)
# ============================================================
def find_synonyms_from_chunk(chunk: str, context: str) -> list:
    """청크에서 동의어 찾기 (추출 + 공백 제거)"""
    result = extract_synonym({"movie_context": context, "movie_chunk": chunk})
    entities = parse_mixed_synonym_output(result)
    if not entities:
        return []
    return clean_entities_whitespace(entities)


def find_entity_from_opensearch(opensearch_client, entity_name: str, index_name: str = "entities"):
    """OpenSearch에서 엔티티 검색"""
    return find_entity_opensearch(opensearch_client, entity_name, index_name)


def add_synonyms_to_entity(opensearch_client, existing_entity: dict, new_synonyms: list, index_name: str = "entities") -> dict:
    """엔티티에 동의어 추가 (병합 + 저장)"""
    existing_synonyms = existing_entity['entity'].get('synonym', [])
    merged_synonyms = merge_synonyms_with_set(existing_synonyms, new_synonyms)
    merged_synonyms_list = list(merged_synonyms) if isinstance(merged_synonyms, set) else merged_synonyms
    success = update_entity_synonyms(opensearch_client, existing_entity['id'], merged_synonyms_list, index_name)
    return {'success': success, 'merged_synonyms': merged_synonyms_list}


# ============================================================
# 단일 영화 처리
# ============================================================
def process_single_movie(opensearch_client, cast_data: dict, stats: dict):
    """
    단일 movie_cast JSON 처리

    흐름:
    1. cast_data에서 context 생성
    2. review 경로의 refined_transcript 로드
    3. transcript를 청크로 분할
    4. 각 청크에서 동의어 추출
    5. OpenSearch에서 엔티티 검색 후 동의어 추가
    """
    title = cast_data["movie_title"]
    print(f"\n🎬 영화 처리: {title}")

    # Step 1: context 생성
    context = build_context_from_cast(cast_data)
    print(f"   📝 Context 생성 완료")

    # Step 2: review 경로에서 refined_transcript 로드
    review_paths = cast_data.get("review", [])
    transcript_pairs = load_refined_transcripts(review_paths)
    if not transcript_pairs:
        print(f"   ⚠️ refined_transcript 없음, 건너뜀")
        return

    print(f"   📄 {len(transcript_pairs)}개 리뷰 transcript 로드")

    # Step 3: 각 transcript를 청크로 분할 후 처리
    for t_idx, (rpath, transcript) in enumerate(transcript_pairs, 1):
        review_filename = os.path.basename(rpath)
        chunks = chunk_text(transcript)
        print(f"   📄 리뷰 [{t_idx}] {review_filename} → {len(chunks)}개 청크")

        # Step 4-5: 각 청크 처리 (기존 핵심 로직 그대로)
        for c_idx, chunk in enumerate(chunks, 1):
            print(f"      📦 chunk [{c_idx}/{len(chunks)}] ({len(chunk)}자) | {review_filename}")
            try:
                entities = find_synonyms_from_chunk(chunk, context)
                if not entities:
                    print(f"      ⏭️  동의어 없음")
                    continue

                for entity_data in entities:
                    entity_name = entity_data['entity_name']
                    new_synonyms = entity_data['synonyms']
                    stats['total_entities'] += 1

                    existing_entity = find_entity_from_opensearch(opensearch_client, entity_name)
                    if not existing_entity:
                        stats['not_found'] += 1
                        continue

                    result = add_synonyms_to_entity(opensearch_client, existing_entity, new_synonyms)
                    if result['success']:
                        stats['updated'] += 1
                        print(f"      ✅ {entity_name}: 동의어 업데이트")
                    else:
                        stats['failed'] += 1
            except Exception as e:
                print(f"      ❌ chunk 처리 에러: {e}")
                continue


# ============================================================
# 메인 파이프라인
# ============================================================
def run_cast_synonym_pipeline():
    """
    movie_cast 기반 동의어 추출 파이프라인

    흐름:
    1. movie_cast 디렉토리에서 모든 JSON 로드
    2. 각 영화별로 context 생성 + review의 refined_transcript 청킹
    3. 청크에서 동의어 추출
    4. OpenSearch에서 엔티티 검색 후 동의어 추가
    """
    print("🚀 movie_cast 기반 동의어 추출 파이프라인 시작")
    print("=" * 60)

    opensearch_client = get_opensearch_client()

    # Step 1: movie_cast JSON 파일 목록
    cast_files = sorted(glob.glob(os.path.join(CAST_DIR, "*.json")))
    print(f"📂 총 {len(cast_files)}개 영화 발견")

    stats = {
        'total_entities': 0, 'updated': 0,
        'not_found': 0, 'failed': 0, 'movies_processed': 0
    }

    # Step 2-4: 각 영화 처리
    for file_idx, cast_file in enumerate(cast_files, 1):
        movie_name = os.path.splitext(os.path.basename(cast_file))[0]
        print(f"\n{'='*60}")
        print(f"📁 [{file_idx}/{len(cast_files)}] {movie_name}")

        try:
            with open(cast_file, "r", encoding="utf-8") as f:
                cast_data = json.load(f)
            process_single_movie(opensearch_client, cast_data, stats)
            stats['movies_processed'] += 1
        except Exception as e:
            print(f"   ❌ 처리 실패: {e}")

    # 결과 출력
    print("\n" + "=" * 60)
    print("🎯 파이프라인 완료!")
    print(f"   처리된 영화: {stats['movies_processed']}개")
    print(f"   전체 엔티티: {stats['total_entities']}개")
    print(f"   업데이트 성공: {stats['updated']}개")
    print(f"   엔티티 없음: {stats['not_found']}개")
    print(f"   업데이트 실패: {stats['failed']}개")

    return stats


# ============================================================
# 실행
# ============================================================
if __name__ == "__main__":
    run_cast_synonym_pipeline()
