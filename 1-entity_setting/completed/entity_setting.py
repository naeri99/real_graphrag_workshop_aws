"""
Entity Setting - 모든 엔티티를 OpenSearch에 인덱싱

data 폴더의 CSV 파일들에서 엔티티 정보를 읽어 OpenSearch에 저장:
- 영화 (MOVIE) - movie_list.csv
- 리뷰어 (REVIEWER) - reviewers.csv
- 배우 (ACTOR) - choi_donghoon_movies_cast.csv
- 캐릭터 (MOVIE_CHARACTER) - choi_donghoon_movies_cast.csv
- 영화 스태프 (MOVIE_STAFF) - movie_staff.csv
"""
import csv
from pathlib import Path
from opensearch.opensearch_index_setting import delete_index, define_entity_index, define_chunk_index
from opensearch.opensearch_con import get_opensearch_client
from utils.bedrock_embedding import BedrockEmbedding


# ============================================================
# 데이터 경로 설정
# ============================================================
DATA_DIR = Path(__file__).parent.parent.parent / "data"
MOVIES_CSV = DATA_DIR / "movies" / "movie_list.csv"
REVIEWERS_CSV = DATA_DIR / "reviwers" / "reviewers.csv"
CAST_CSV = DATA_DIR / "actors_chractor" / "choi_donghoon_movies_cast.csv"
STAFF_CSV = DATA_DIR / "movie_staff" / "movie_staff.csv"


# ============================================================
# CSV 로드 함수들
# ============================================================
def load_movies() -> list:
    """movie_list.csv에서 영화 정보 로드"""
    movies = []
    with open(MOVIES_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            movies.append({
                'title': row['Title'],
                'synonym': row['Synonym'],
                'year': row['Year'],
                'synopsis': row['Synopsis']
            })
    return movies


def load_reviewers() -> list:
    """reviewers.csv에서 리뷰어 정보 로드"""
    reviewers = []
    with open(REVIEWERS_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            reviewers.append({
                'name': row['Reviewers'],
                'synonym': row['Synonym']
            })
    return reviewers


def load_cast() -> list:
    """choi_donghoon_movies_cast.csv에서 캐스트 정보 로드"""
    cast = []
    with open(CAST_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cast.append({
                'actor': row['배우'],
                'character': row['역할'],
                'movie': row['영화']
            })
    return cast


def load_staff() -> list:
    """movie_staff.csv에서 스태프 정보 로드"""
    staff = []
    with open(STAFF_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            staff.append({
                'name': row['Name'].strip(),
                'synonym': row['Synonym'].strip()
            })
    return staff


# ============================================================
# 인덱싱 함수들
# ============================================================
def index_movies(opensearch_client, embedder: BedrockEmbedding, movies: list, index_name: str = "entities") -> int:
    """영화 엔티티 인덱싱"""
    print("\n🎬 영화 인덱싱 시작...")
    indexed = 0
    
    for movie in movies:
        try:
            summary = movie['synopsis']
            summary_vec = embedder.embed_text(summary)
            
            doc = {
                "entity": {
                    "name": movie['title'],
                    "synonym": [movie['title'], movie['synonym']],
                    "entity_type": "MOVIE",
                    "summary": summary,
                    "summary_vec": summary_vec,
                    "neptune_id": None
                }
            }
            
            doc_id = f"{movie['title']}_MOVIE"
            opensearch_client.index(index=index_name, id=doc_id, body=doc)
            indexed += 1
            print(f"   ✅ {movie['title']} 인덱싱 완료")
            
        except Exception as e:
            print(f"   ❌ {movie['title']} 인덱싱 실패: {e}")
    
    return indexed


def index_reviewers(opensearch_client, embedder: BedrockEmbedding, reviewers: list, index_name: str = "entities") -> int:
    """리뷰어 엔티티 인덱싱"""
    print("\n👤 리뷰어 인덱싱 시작...")
    indexed = 0
    
    for reviewer in reviewers:
        try:
            summary = f"{reviewer['name']}은 영화 리뷰어입니다."
            summary_vec = embedder.embed_text(summary)
            
            doc = {
                "entity": {
                    "name": reviewer['name'],
                    "synonym": [reviewer['name'], reviewer['synonym']],
                    "entity_type": "REVIEWER",
                    "summary": summary,
                    "summary_vec": summary_vec,
                    "neptune_id": None
                }
            }
            
            doc_id = f"{reviewer['name']}_REVIEWER"
            opensearch_client.index(index=index_name, id=doc_id, body=doc)
            indexed += 1
            print(f"   ✅ {reviewer['name']} 인덱싱 완료")
            
        except Exception as e:
            print(f"   ❌ {reviewer['name']} 인덱싱 실패: {e}")
    
    return indexed


def index_staff(opensearch_client, embedder: BedrockEmbedding, staff: list, index_name: str = "entities") -> int:
    """영화 스태프 엔티티 인덱싱"""
    print("\n🎥 영화 스태프 인덱싱 시작...")
    indexed = 0
    
    for person in staff:
        try:
            summary = f"{person['name']}은 영화 감독입니다."
            summary_vec = embedder.embed_text(summary)
            
            doc = {
                "entity": {
                    "name": person['name'],
                    "synonym": [person['name'], person['synonym']],
                    "entity_type": "MOVIE_STAFF",
                    "summary": summary,
                    "summary_vec": summary_vec,
                    "neptune_id": None
                }
            }
            
            doc_id = f"{person['name']}_MOVIE_STAFF"
            opensearch_client.index(index=index_name, id=doc_id, body=doc)
            indexed += 1
            print(f"   ✅ {person['name']} 인덱싱 완료")
            
        except Exception as e:
            print(f"   ❌ {person['name']} 인덱싱 실패: {e}")
    
    return indexed


def index_cast(opensearch_client, embedder: BedrockEmbedding, cast: list, index_name: str = "entities") -> int:
    """배우 및 캐릭터 엔티티 인덱싱"""
    print("\n🎭 배우 및 캐릭터 인덱싱 시작...")
    indexed = 0
    
    # 중복 제거를 위한 set
    indexed_actors = set()
    indexed_characters = set()
    
    for item in cast:
        actor = item['actor']
        character = item['character']
        movie = item['movie']
        
        # 배우 인덱싱 (중복 방지)
        if actor not in indexed_actors:
            try:
                actor_summary = f"{actor}은 한국 영화배우입니다."
                actor_summary_vec = embedder.embed_text(actor_summary)
                
                actor_doc = {
                    "entity": {
                        "name": actor,
                        "synonym": [actor],
                        "entity_type": "ACTOR",
                        "summary": actor_summary,
                        "summary_vec": actor_summary_vec,
                        "neptune_id": None
                    }
                }
                
                doc_id = f"{actor}_ACTOR"
                opensearch_client.index(index=index_name, id=doc_id, body=actor_doc)
                indexed_actors.add(actor)
                indexed += 1
                print(f"   ✅ 배우 '{actor}' 인덱싱 완료")
                
            except Exception as e:
                print(f"   ❌ 배우 '{actor}' 인덱싱 실패: {e}")
        
        # 캐릭터 인덱싱 (중복 방지 - 영화+캐릭터 조합)
        char_key = f"{movie}_{character}"
        if char_key not in indexed_characters:
            try:
                char_summary = f"{character}은 영화 '{movie}'의 등장인물로, {actor}이 연기한 캐릭터입니다."
                char_summary_vec = embedder.embed_text(char_summary)
                
                char_doc = {
                    "entity": {
                        "name": character,
                        "synonym": [character],
                        "entity_type": "MOVIE_CHARACTER",
                        "summary": char_summary,
                        "summary_vec": char_summary_vec,
                        "neptune_id": None
                    }
                }
                
                doc_id = f"{character}_{movie}_MOVIE_CHARACTER"
                opensearch_client.index(index=index_name, id=doc_id, body=char_doc)
                indexed_characters.add(char_key)
                indexed += 1
                print(f"   ✅ 캐릭터 '{character}' ({movie}) 인덱싱 완료")
                
            except Exception as e:
                print(f"   ❌ 캐릭터 '{character}' 인덱싱 실패: {e}")
    
    return indexed


# ============================================================
# 메인 파이프라인
# ============================================================
def run_entity_indexing(index_name: str = "entities"):
    """
    모든 엔티티를 OpenSearch에 인덱싱
    
    흐름:
    1. 인덱스 초기화 (삭제 후 재생성)
    2. 영화 인덱싱
    3. 리뷰어 인덱싱
    4. 영화 스태프 인덱싱
    5. 배우 및 캐릭터 인덱싱
    """
    print("🚀 엔티티 인덱싱 파이프라인 시작")
    print("=" * 60)
    
    # OpenSearch 클라이언트 및 임베딩 클라이언트 초기화
    opensearch_client = get_opensearch_client()
    embedder = BedrockEmbedding()
    
    # Step 1: 인덱스 초기화
    print("\n📦 Step 1: 인덱스 초기화")
    try:
        delete_index(opensearch_client, index_name)
        delete_index(opensearch_client,  "chunks")

        print(f"   기존 인덱스 '{index_name}' 삭제 완료")
    except:
        print(f"   인덱스 '{index_name}' 없음 (새로 생성)")
    
    define_entity_index(opensearch_client, index_name)
    define_chunk_index(opensearch_client, "chunks")
    print(f"   인덱스 '{index_name}' 생성 완료")
    
    # Step 2: 데이터 로드
    print("\n📂 Step 2: 데이터 로드")
    movies = load_movies()
    reviewers = load_reviewers()
    staff = load_staff()
    cast = load_cast()
    
    print(f"   영화: {len(movies)}개")
    print(f"   리뷰어: {len(reviewers)}명")
    print(f"   스태프: {len(staff)}명")
    print(f"   캐스트: {len(cast)}개 레코드")
    
    # Step 3-6: 인덱싱 (embedder 전달)
    stats = {
        'movies': index_movies(opensearch_client, embedder, movies, index_name),
        'reviewers': index_reviewers(opensearch_client, embedder, reviewers, index_name),
        'staff': index_staff(opensearch_client, embedder, staff, index_name),
        'cast': index_cast(opensearch_client, embedder, cast, index_name)
    }
    
    # 인덱스 새로고침
    try:
        opensearch_client.indices.refresh(index=index_name)
    except Exception as e:
        print(f"⚠️ 인덱스 새로고침 실패: {e}")
    
    # 결과 출력
    total = sum(stats.values())
    print("\n" + "=" * 60)
    print("🎯 엔티티 인덱싱 완료!")
    print(f"   영화: {stats['movies']}개")
    print(f"   리뷰어: {stats['reviewers']}명")
    print(f"   스태프: {stats['staff']}명")
    print(f"   배우/캐릭터: {stats['cast']}개")
    print(f"   총 인덱싱: {total}개")
    
    return stats


# ============================================================
# 실행
# ============================================================
if __name__ == "__main__":
    run_entity_indexing()
