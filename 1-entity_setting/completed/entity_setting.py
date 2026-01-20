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
from utils.read_files import load_json_from_list
from utils.parse_utils import parse_mixed_synonym_output
from utils.generate_entity import extract_synonym
from utils.synonym import (
    clean_entities_whitespace,
    merge_synonyms_with_set,
    update_entity_synonyms
)
from langchain_text_splitters import RecursiveCharacterTextSplitter


# ============================================================
# OpenSearch 인덱스 초기화
# ============================================================
opensearch_conn = get_opensearch_client()

try:
    delete_index(opensearch_conn, "entities")
except:
    print("no entities")

define_entity_index(opensearch_conn, "entities")


# ============================================================
# 컨텍스트 생성
# ============================================================
def make_inception_cast_context() -> str:
    """인셉션 캐스트 컨텍스트 생성"""
    cast = [
        ("Leonardo DiCaprio", "Dom Cobb"),
        ("Joseph Gordon-Levitt", "Arthur"),
        ("Ellen Page", "Ariadne"),
        ("Tom Hardy", "Eames"),
        ("Ken Watanabe", "Saito"),
        ("Dileep Rao", "Yusuf"),
        ("Cillian Murphy", "Robert Michael Fischer"),
        ("Tom Berenger", "Peter Browning"),
        ("Marion Cotillard", "Mal Cobb"),
        ("Pete Postlethwaite", "Maurice Fischer"),
        ("Michael Caine", "Professor Miles"),
        ("Lukas Haas", "Nash")
    ]
    
    context_parts = ["영화 인셉션의 주요 등장인물과 배우 정보:", ""]
    context_parts.extend([f"- {char}: {actor}이 연기한 캐릭터" for actor, char in cast])
    context_parts.extend(["", "영화: 인셉션", "리뷰어: reviwerman", "감독: Christopher Nolan"])
    context_parts.append(f"총 {len(cast)}명의 배우가 {len(cast)}개의 캐릭터를 연기했습니다.")
    
    return "\n".join(context_parts)


# ============================================================
# 파이프라인 함수들
# ============================================================
def load_and_chunk_data(file_path: str, chunk_size: int = 1500, chunk_overlap: int = 100) -> list:
    """데이터 로드 및 청킹"""
    result = load_json_from_list(file_path)
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    
    chunks = []
    for item in result:
        chunks.extend(text_splitter.split_text(item["data"]))
    return chunks


def init_pipeline(file_path: str) -> tuple:
    """Step 1: 파이프라인 초기화 - 데이터 로드 및 청킹"""
    print("\n📂 Step 1: 데이터 로드 및 청킹")
    chunks = load_and_chunk_data(file_path)
    print(f"   총 {len(chunks)}개 청크 생성")
    
    stats = {'total_entities': 0, 'updated': 0, 'not_found': 0, 'failed': 0}
    return chunks, stats


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
    
    success = update_entity_synonyms(opensearch_client, existing_entity['id'], merged_synonyms, index_name)
    return {'success': success, 'merged_synonyms': merged_synonyms}


def print_final_stats(stats: dict):
    """최종 결과 출력"""
    print("\n" + "=" * 60)
    print("🎯 파이프라인 완료!")
    print(f"   전체 엔티티: {stats['total_entities']}개")
    print(f"   업데이트 성공: {stats['updated']}개")
    print(f"   엔티티 없음: {stats['not_found']}개")
    print(f"   업데이트 실패: {stats['failed']}개")


# ============================================================
# 메인 파이프라인
# ============================================================
def run_synonym_pipeline():
    """
    동의어 추출 파이프라인 실행
    
    흐름:
    1. 데이터 로드 및 청킹
    2. 청크에서 동의어 찾기
    3. OpenSearch에서 엔티티 검색
    4. 엔티티에 동의어 추가
    """
    print("🚀 동의어 추출 파이프라인 시작")
    print("=" * 60)
    
    opensearch_client = get_opensearch_client()
    context = make_inception_cast_context()
    
    # Step 1: 초기화
    chunks, stats = init_pipeline("./data/inception/list.txt")
    
    for i, chunk in enumerate(chunks, 1):
        print(f"\n{'='*60}")
        print(f"📝 Chunk {i}/{len(chunks)} 처리 중")
        
        # Step 2: 청크에서 동의어 찾기
        entities = find_synonyms_from_chunk(chunk, context)
        if not entities:
            print("   ⚠️ 추출된 엔티티 없음")
            continue
        print(f"   🔍 {len(entities)}개 엔티티 동의어 추출 완료")
        
        for entity_data in entities:
            entity_name = entity_data['entity_name']
            new_synonyms = entity_data['synonyms']
            stats['total_entities'] += 1
            
            # Step 3: OpenSearch에서 엔티티 검색
            existing_entity = find_entity_from_opensearch(opensearch_client, entity_name)
            if not existing_entity:
                stats['not_found'] += 1
                print(f"   🔍 {entity_name}: 엔티티 없음")
                continue
            
            # Step 4: 엔티티에 동의어 추가
            result = add_synonyms_to_entity(opensearch_client, existing_entity, new_synonyms)
            if result['success']:
                stats['updated'] += 1
                print(f"   ✅ {entity_name}: 동의어 업데이트 완료")
            else:
                stats['failed'] += 1
                print(f"   ❌ {entity_name}: 업데이트 실패")
    
    # 결과 출력
    print_final_stats(stats)
    return stats


# ============================================================
# 실행
# ============================================================
if __name__ == "__main__":
    run_synonym_pipeline()
