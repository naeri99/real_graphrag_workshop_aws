"""
Helper utilities for entity extraction pipeline
"""
import os
import json
import csv
import uuid
import hashlib
from pathlib import Path
from typing import List, Dict, Tuple

from langchain_text_splitters import RecursiveCharacterTextSplitter


# ============================================================
# 데이터 경로 설정
# ============================================================
# 2-entity_extraction_from_chunk/utils/helper.py -> real_graphrag_workshop_aws/data
DATA_DIR = Path(__file__).parent.parent.parent / "data"

# 절대 경로 폴백
if not DATA_DIR.exists():
    DATA_DIR = Path("/home/ec2-user/real_graphrag_workshop_aws/data")
if not DATA_DIR.exists():
    DATA_DIR = Path("/home/ec2-user/workshop/data")

MOVIES_CSV = DATA_DIR / "movies" / "movie_list.csv"
REVIEWERS_CSV = DATA_DIR / "reviwers" / "reviewers.csv"
CAST_CSV = DATA_DIR / "actors_chractor" / "choi_donghoon_movies_cast.csv"
STAFF_CSV = DATA_DIR / "movie_staff" / "movie_staff.csv"
REVIEWS_DIR = DATA_DIR / "reviews" / "DonghoonChoi"


# ============================================================
# 해시/ID 생성
# ============================================================
def generate_chunk_hash(chunk_text: str) -> str:
    """Generate unique hash for chunk content"""
    return hashlib.md5(chunk_text.encode('utf-8')).hexdigest()[:14]


def generate_chunk_id(reviewer: str, chunk_hash: str) -> str:
    """Generate unique chunk ID"""
    return f"{reviewer}_{chunk_hash}_{uuid.uuid4().hex[:8]}"


# ============================================================
# CSV 로드 함수
# ============================================================
def load_movies_csv() -> dict:
    """movie_list.csv 로드"""
    movies = {}
    with open(MOVIES_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            synonym = row['Synonym']
            movies[synonym] = {
                'title': row['Title'],
                'synonym': synonym,
                'year': row['Year'],
                'synopsis': row['Synopsis']
            }
    return movies


def load_reviewers_csv() -> dict:
    """reviewers.csv 로드"""
    reviewers = {}
    with open(REVIEWERS_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            synonym = row['Synonym']
            reviewers[synonym] = {
                'name': row['Reviewers'],
                'synonym': synonym
            }
    return reviewers


def load_cast_csv() -> dict:
    """choi_donghoon_movies_cast.csv 로드"""
    cast_by_movie = {}
    with open(CAST_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            movie = row['영화']
            if movie not in cast_by_movie:
                cast_by_movie[movie] = []
            cast_by_movie[movie].append({
                'actor': row['배우'],
                'character': row['역할']
            })
    return cast_by_movie


def load_staff_csv() -> dict:
    """movie_staff.csv 로드"""
    staff = {}
    with open(STAFF_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            synonym = row['Synonym'].strip()
            staff[synonym] = {
                'name': row['Name'].strip(),
                'synonym': synonym
            }
    return staff


# ============================================================
# 파일명 파싱
# ============================================================
def parse_review_filename(filename: str) -> Tuple[str, str]:
    """
    리뷰 파일명에서 영화 Synonym과 리뷰어 Synonym 추출
    예: Alienoid1_Agony.json -> ('Alienoid1', 'Agony')
    예: Alienoid1_B+Man.json -> ('Alienoid1', 'B Man')
    """
    name = filename.replace('.json', '').replace('+', ' ')
    parts = name.split('_')
    
    if len(parts) >= 2:
        movie_synonym = parts[0]
        reviewer_synonym = '_'.join(parts[1:])
        return movie_synonym, reviewer_synonym
    
    return None, None


def find_movie_by_synonym(movies: dict, synonym: str) -> dict:
    """Synonym으로 영화 찾기"""
    for key, movie in movies.items():
        if key == synonym or synonym in key:
            return movie
    return None


def find_reviewer_by_synonym(reviewers: dict, synonym: str) -> dict:
    """Synonym으로 리뷰어 찾기"""
    for key, reviewer in reviewers.items():
        if key == synonym or synonym.replace(' ', '') == key.replace(' ', ''):
            return reviewer
    return None


def get_director_from_path(review_filepath: str, staff: dict) -> dict:
    """리뷰 파일 경로에서 감독 정보 추출"""
    path_parts = review_filepath.replace('\\', '/').split('/')
    for i, part in enumerate(path_parts):
        if part == 'reviews' and i + 1 < len(path_parts):
            director_folder = path_parts[i + 1]
            return staff.get(director_folder, {'name': director_folder, 'synonym': director_folder})
    return {'name': 'Unknown', 'synonym': 'Unknown'}


# ============================================================
# 컨텍스트 생성
# ============================================================
def build_movie_context(movie: dict, reviewer: dict, cast: list, director: dict) -> str:
    """영화 컨텍스트 문자열 생성"""
    context_parts = [f"영화 {movie['title']}의 주요 등장인물과 배우 정보:", ""]
    
    for c in cast:
        context_parts.append(f"- {c['character']}: {c['actor']}이 연기한 캐릭터")
    
    context_parts.extend([
        "",
        f"영화: {movie['title']}",
        f"감독: {director['name']}",
        f"리뷰어: {reviewer['name']}",
        f"개봉년도: {movie['year']}",
        f"총 {len(cast)}명의 배우가 {len(cast)}개의 캐릭터를 연기했습니다."
    ])
    
    return "\n".join(context_parts)


def get_context_from_review_file(review_filepath: str) -> Tuple[str, str, str, str]:
    """
    리뷰 파일에서 컨텍스트, transcript, movie_id, reviewer_id 추출
    
    Returns:
        tuple: (context_str, transcript_str, movie_title, reviewer_name)
    """
    movies = load_movies_csv()
    reviewers = load_reviewers_csv()
    cast_by_movie = load_cast_csv()
    staff = load_staff_csv()
    
    filename = os.path.basename(review_filepath)
    movie_synonym, reviewer_synonym = parse_review_filename(filename)
    
    if not movie_synonym or not reviewer_synonym:
        raise ValueError(f"파일명 파싱 실패: {filename}")
    
    movie = find_movie_by_synonym(movies, movie_synonym)
    if not movie:
        raise ValueError(f"영화를 찾을 수 없음: {movie_synonym}")
    
    reviewer = find_reviewer_by_synonym(reviewers, reviewer_synonym)
    if not reviewer:
        raise ValueError(f"리뷰어를 찾을 수 없음: {reviewer_synonym}")
    
    director = get_director_from_path(review_filepath, staff)
    cast = cast_by_movie.get(movie['title'], [])
    
    context = build_movie_context(movie, reviewer, cast, director)
    
    with open(review_filepath, 'r', encoding='utf-8') as f:
        review_data = json.load(f)
    transcript = review_data.get('transcript', '')
    
    return context, transcript, movie['title'], reviewer['name']


def get_all_review_files() -> List[Path]:
    """모든 리뷰 파일 경로 반환"""
    return list(REVIEWS_DIR.glob("*.json"))


# ============================================================
# 청킹
# ============================================================
def chunk_text(text: str, chunk_size: int = 1500, chunk_overlap: int = 100) -> List[str]:
    """텍스트를 청크로 분할"""
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size, 
        chunk_overlap=chunk_overlap
    )
    return text_splitter.split_text(text)


# ============================================================
# 출력 함수
# ============================================================
def print_pipeline_header(title: str) -> None:
    """Print pipeline section header"""
    print(f"\n{'='*60}")
    print(title)
    print('='*60)


def print_chunk_stats(chunk_stats: Dict) -> None:
    """Print chunk processing stats"""
    print(f"   Chunk done: {chunk_stats['entities_saved']} entities, {chunk_stats['relationships_saved']} relationships")


def print_final_stats(total_stats: Dict) -> None:
    """Print final pipeline stats"""
    print(f"\n{'='*60}")
    print("Pipeline Complete!")
    print('='*60)
    print(f"   Processed files: {total_stats.get('files_processed', 0)}")
    print(f"   Processed chunks: {total_stats['processed_chunks']}/{total_stats['total_chunks']}")
    print(f"   Saved entities: {total_stats['total_entities']}")
    print(f"   Saved relationships: {total_stats['total_relationships']}")
