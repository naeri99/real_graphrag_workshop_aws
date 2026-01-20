"""
영화 컨텍스트 생성 모듈
- 리뷰 파일에서 영화/리뷰어 정보 추출
- CSV 파일에서 영화, 리뷰어, 캐스트 정보 로드
- 컨텍스트 문자열 생성
"""
import os
import json
import csv
from pathlib import Path


# 데이터 경로 설정
DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"
MOVIES_CSV = DATA_DIR / "movies" / "movie_list.csv"
REVIEWERS_CSV = DATA_DIR / "reviwers" / "reviewers.csv"
CAST_CSV = DATA_DIR / "actors_chractor" / "choi_donghoon_movies_cast.csv"
STAFF_CSV = DATA_DIR / "movie_staff" / "movie_staff.csv"
REVIEWS_DIR = DATA_DIR / "reviews" / "DonghoonChoi"


def load_movies_csv() -> dict:
    """movie_list.csv 로드 - Synonym으로 영화 찾기용"""
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
    """reviewers.csv 로드 - Synonym으로 리뷰어 찾기용"""
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
    """choi_donghoon_movies_cast.csv 로드 - 영화별 캐스트 정보"""
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
    """movie_staff.csv 로드 - 감독 정보"""
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


def get_director_from_path(review_filepath: str, staff: dict) -> dict:
    """리뷰 파일 경로에서 감독 정보 추출 (폴더명 기반)"""
    # 경로에서 감독 폴더명 추출: .../reviews/DonghoonChoi/...
    path_parts = review_filepath.replace('\\', '/').split('/')
    for i, part in enumerate(path_parts):
        if part == 'reviews' and i + 1 < len(path_parts):
            director_folder = path_parts[i + 1]
            return staff.get(director_folder, {'name': director_folder, 'synonym': director_folder})
    return {'name': 'Unknown', 'synonym': 'Unknown'}


def parse_review_filename(filename: str) -> tuple:
    """
    리뷰 파일명에서 영화 Synonym과 리뷰어 Synonym 추출
    예: Alienoid1_Agony.json -> ('Alienoid1', 'Agony')
    예: Alienoid1_B+Man.json -> ('Alienoid1', 'B Man')
    """
    # +를 space로 변환
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


def get_cast_for_movie(cast_by_movie: dict, movie_title: str) -> list:
    """영화 제목으로 캐스트 정보 가져오기"""
    return cast_by_movie.get(movie_title, [])


def load_review_json(filepath: str) -> dict:
    """리뷰 JSON 파일 로드"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def build_movie_context(movie: dict, reviewer: dict, cast: list, director: dict) -> str:
    """
    영화 컨텍스트 문자열 생성
    
    Args:
        movie: 영화 정보 dict
        reviewer: 리뷰어 정보 dict
        cast: 캐스트 리스트 [{'actor': str, 'character': str}, ...]
        director: 감독 정보 dict
    
    Returns:
        str: 컨텍스트 문자열
    """
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


def get_context_from_review_file(review_filepath: str) -> tuple:
    """
    리뷰 파일 경로에서 컨텍스트와 transcript 추출
    
    Args:
        review_filepath: 리뷰 JSON 파일 경로
            예: /home/ec2-user/real_graphrag_workshop_aws/data/reviews/DonghoonChoi/Alienoid1_Agony.json
    
    Returns:
        tuple: (context_str, transcript_str)
    
    흐름:
    1. 파일명에서 영화 Synonym, 리뷰어 Synonym 추출
    2. movie_list.csv에서 영화 정보 찾기
    3. reviewers.csv에서 리뷰어 정보 찾기
    4. movie_staff.csv에서 감독 정보 찾기
    5. choi_donghoon_movies_cast.csv에서 캐스트 정보 가져오기
    6. 컨텍스트 문자열 생성
    """
    # CSV 데이터 로드
    movies = load_movies_csv()
    reviewers = load_reviewers_csv()
    cast_by_movie = load_cast_csv()
    staff = load_staff_csv()
    
    # 파일명에서 Synonym 추출
    filename = os.path.basename(review_filepath)
    movie_synonym, reviewer_synonym = parse_review_filename(filename)
    
    if not movie_synonym or not reviewer_synonym:
        raise ValueError(f"파일명 파싱 실패: {filename}")
    
    # 영화 찾기
    movie = find_movie_by_synonym(movies, movie_synonym)
    if not movie:
        raise ValueError(f"영화를 찾을 수 없음: {movie_synonym}")
    
    # 리뷰어 찾기
    reviewer = find_reviewer_by_synonym(reviewers, reviewer_synonym)
    if not reviewer:
        raise ValueError(f"리뷰어를 찾을 수 없음: {reviewer_synonym}")
    
    # 감독 찾기
    director = get_director_from_path(review_filepath, staff)
    
    # 캐스트 가져오기
    cast = get_cast_for_movie(cast_by_movie, movie['title'])
    
    # 컨텍스트 생성
    context = build_movie_context(movie, reviewer, cast, director)
    
    # transcript 로드
    review_data = load_review_json(review_filepath)
    transcript = review_data.get('transcript', '')
    
    return context, transcript


def get_all_review_files() -> list:
    """모든 리뷰 파일 경로 반환"""
    return list(REVIEWS_DIR.glob("*.json"))


# 테스트용
if __name__ == "__main__":
    # 테스트: Alienoid1_Agony.json
    test_file = REVIEWS_DIR / "Alienoid1_Agony.json"
    context, transcript = get_context_from_review_file(str(test_file))
    
    print("=" * 60)
    print("📝 Context:")
    print(context)
    print("\n" + "=" * 60)
    print(f"📄 Transcript 길이: {len(transcript)} 글자")
    print(transcript[:500] + "...")
