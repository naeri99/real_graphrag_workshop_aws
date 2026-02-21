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
import time
import json
import os
from pathlib import Path

# 스크립트 파일 기준 디렉토리
SCRIPT_DIR = Path(__file__).parent.resolve()
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "step" / "chunkings"


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


def get_chunk(reviews_dir):
    if reviews_dir:
        from pathlib import Path
        review_files = list(Path(reviews_dir).rglob("*.json"))
    else:
        review_files = get_all_review_files()
    return review_files


def run_chunking(
    reviews_dir: str = None,
    chunk_size: int = 1500,
    chunk_overlap: int = 100,
    output_dir: str = None
):
    # 출력 디렉토리 설정 (기본값: 스크립트 기준)
    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    output_dir = Path(output_dir)
    
    # 출력 디렉토리 초기화
    clear_output_directory(output_dir)
    
    review_files=get_chunk(reviews_dir)
    
    # 텍스트 스플리터
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    
    for i, review_file in enumerate(review_files, 1):
        print(f"\n{'='*60}")
        print(f"📄 [{i}/{len(review_files)}] {review_file.name}")
        print('='*60)
        
        # 파일에서 정보 추출
        _, transcript, movie_id, reviewer = get_context_from_review_file(str(review_file))
        print(f"   🎬 Movie: {movie_id}, Reviewer: {reviewer}")
        
        # 청킹
        chunks = text_splitter.split_text(transcript)
        print(f"   📝 Chunks: {len(chunks)}")
        
        for j, chunk in enumerate(chunks, 1):
            print(f"\n   --- Chunk {j}/{len(chunks)} ---")
            print(f"f{chunk[:800]}... 생략 ...")
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
    # 전체 파이프라인 실행 (Neptune 저장 포함)
    run_chunking(
        reviews_dir="../../data/reviews/DonghoonChoi",  # 기본 경로 사용
        chunk_size=1500,
        chunk_overlap=100
    )
    print(f"📁 Output directory: {DEFAULT_OUTPUT_DIR}")
