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


def save_chunk_to_json(chunk_data: dict, output_dir: str = "./step/chunkings") -> str:
    """
    Chunk 데이터를 JSON 파일로 저장합니다.
    
    Args:
        chunk_data: 저장할 chunk 데이터 (chunk_hash, chunk_id, user_query 포함)
        output_dir: 저장할 디렉토리 경로
    
    Returns:
        저장된 파일 경로
    """
    # 디렉토리 생성
    os.makedirs(output_dir, exist_ok=True)
    
    # 파일명: chunk_id 사용
    filename = f"{chunk_data['chunk_id']}.json"
    filepath = os.path.join(output_dir, filename)
    
    # JSON 저장
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(chunk_data, f, ensure_ascii=False, indent=2)
    
    print(f"   💾 Saved: {filepath}")
    return filepath


def save_all_chunks_to_json(chunks_list: list, output_dir: str = "./step/chunkings") -> str:
    """
    모든 chunk 데이터를 하나의 JSON 파일로 저장합니다.
    
    Args:
        chunks_list: chunk 데이터 리스트
        output_dir: 저장할 디렉토리 경로
    
    Returns:
        저장된 파일 경로
    """
    os.makedirs(output_dir, exist_ok=True)
    
    filepath = os.path.join(output_dir, "all_chunks.json")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(chunks_list, f, ensure_ascii=False, indent=2)
    
    print(f"   💾 Saved all chunks: {filepath}")
    return filepath

def clear_output_directory(output_dir: str = "./step/chunkings"):
    """
    출력 디렉토리의 모든 파일을 삭제합니다.
    """
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


