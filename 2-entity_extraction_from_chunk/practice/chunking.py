"""
Entity Extraction from Chunk Pipeline
Flow:
1. movie_cast JSON에서 review 경로 로드
2. review/ 디렉토리의 refined_transcript 기준으로 chunking
3. chunk 데이터를 JSON으로 저장
"""
from langchain_text_splitters import RecursiveCharacterTextSplitter
from utils.helper import (
    generate_chunk_hash,
    generate_chunk_id
)
import glob
import json
import os
from pathlib import Path

# 스크립트 파일 기준 디렉토리
SCRIPT_DIR = Path(__file__).parent.resolve()
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "step" / "chunkings"

# movie_cast 디렉토리 (기본값)
DEFAULT_CAST_DIR = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir, os.pardir, "data", "raw_csv", "movie_cast"
))


def save_chunk_to_json(chunk_data: dict, output_dir: str = None) -> str:
    """
    Chunk 데이터를 JSON 파일로 저장합니다.
    
    Args:
        chunk_data: 저장할 chunk 데이터 (chunk_hash, chunk_id, user_query 포함)
        output_dir: 저장할 디렉토리 경로
    
    Returns:
        저장된 파일 경로
    """
    # 디렉토리 생성 (기본값: 스크립트 기준)
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 파일명: chunk_id 사용
    filename = f"{chunk_data['chunk_id']}.json"
    filepath = os.path.join(output_dir, filename)
    
    # JSON 저장
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(chunk_data, f, ensure_ascii=False, indent=2)
    
    print(f"   💾 Saved: {filepath}")
    return filepath


def save_all_chunks_to_json(chunks_list: list, output_dir: str = None) -> str:
    """
    모든 chunk 데이터를 하나의 JSON 파일로 저장합니다.
    
    Args:
        chunks_list: chunk 데이터 리스트
        output_dir: 저장할 디렉토리 경로
    
    Returns:
        저장된 파일 경로
    """
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filepath = output_dir / "all_chunks.json"
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(chunks_list, f, ensure_ascii=False, indent=2)
    
    print(f"   💾 Saved all chunks: {filepath}")
    return filepath

def clear_output_directory(output_dir: str = None):
    """
    출력 디렉토리의 모든 파일을 삭제합니다.
    """
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    dir_path = Path(output_dir)
    if dir_path.exists():
        for file in dir_path.glob("*.json"):
            file.unlink()
        print(f"🗑️ Cleared: {output_dir}")


def get_chunk(cast_dir):
    """movie_cast 디렉토리에서 모든 JSON 파일 로드, review 경로의 refined_transcript 반환
    
    Returns:
        list of (review_path, refined_transcript, movie_title, channel_name)
    """
    cast_files = sorted(glob.glob(os.path.join(cast_dir, "*.json")))
    print(f"📂 총 {len(cast_files)}개 영화 발견")
    
    review_items = []
    for cast_file in cast_files:
        with open(cast_file, "r", encoding="utf-8") as f:
            cast_data = json.load(f)
        
        movie_title = cast_data["movie_title"]
        review_paths = cast_data.get("review", [])
        # 프로젝트 루트 (movie_cast 기준 ../../..)
        project_root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir))
        
        for rpath in review_paths:
            # 상대경로(./data/review/...) → 절대경로로 변환
            if rpath.startswith("./"):
                rpath = os.path.join(project_root, rpath[2:])
            try:
                with open(rpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                rt = data.get("refined_transcript", "")
                if not rt:
                    print(f"   ⏭️  refined_transcript 없음, 스킵: {os.path.basename(rpath)}")
                    continue
                channel_name = data.get("channel_name", "unknown")
                # 파일명에 사용 불가한 문자 제거
                channel_name = channel_name.replace("/", "_").replace("\\", "_")
                review_items.append((rpath, rt, movie_title, channel_name))
            except Exception as e:
                print(f"   ⏭️  파일 로드 실패, 스킵: {os.path.basename(rpath)} ({e})")
    
    print(f"📄 총 {len(review_items)}개 리뷰 로드 완료")
    return review_items


def run_chunking(
    cast_dir: str = None,
    chunk_size: int = 1500,
    chunk_overlap: int = 100,
    output_dir: str = None
):
    # 출력 디렉토리 설정 (기본값: 스크립트 기준)
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    output_dir = Path(output_dir)
    
    # cast 디렉토리 설정
    if cast_dir is None:
        cast_dir = DEFAULT_CAST_DIR
    
    # 출력 디렉토리 초기화
    clear_output_directory(output_dir)
    
    # movie_cast에서 review 정보 로드
    review_items = get_chunk(cast_dir)
    
    # 텍스트 스플리터
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    
    for i, (review_path, transcript, movie_id, reviewer) in enumerate(review_items, 1):
        review_filename = os.path.basename(review_path)
        print(f"\n{'='*60}")
        print(f"📄 [{i}/{len(review_items)}] {review_filename}")
        print('='*60)
        print(f"   🎬 Movie: {movie_id}, Reviewer: {reviewer}")
        
        # 청킹
        chunks = text_splitter.split_text(transcript)
        print(f"   📝 Chunks: {len(chunks)}")
        
        for j, chunk in enumerate(chunks, 1):
            print(f"\n   --- Chunk {j}/{len(chunks)} ---")
            print(f"   {chunk[:800]}... 생략 ...")
            chunk_hash = generate_chunk_hash(chunk)
            chunk_id = generate_chunk_id(reviewer, chunk_hash)

            # Step 1: chunk 데이터 구성 및 저장
            save_chunk = {
                "chunk_hash": chunk_hash,
                "chunk_id": chunk_id,
                "user_query": chunk,
                "movie_id": movie_id,
                "reviewer": reviewer,
                "chunk_index": j,
            }
            
            # JSON 파일로 저장
            save_chunk_to_json(save_chunk, output_dir)


if __name__ == "__main__":
    # 전체 파이프라인 실행
    run_chunking(
        cast_dir=DEFAULT_CAST_DIR,
        chunk_size=1500,
        chunk_overlap=100
    )
    print(f"📁 Output directory: {DEFAULT_OUTPUT_DIR}")
