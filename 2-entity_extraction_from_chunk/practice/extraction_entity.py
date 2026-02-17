"""
Entity Extraction from Chunk Pipeline
Flow:
1. Read chunks from ./step/chunkings
2. Extract entities from chunk (LLM)
3. Save entities and relationships back to JSON
"""
import json
import os
from pathlib import Path
from utils.parse_utils import parse_extraction_output
from utils.generate_entity import extract_entities


def read_chunks_from_dir(chunk_dir: str = "./step/chunkings") -> list:
    """
    ./step/chunkings 디렉토리에서 모든 JSON 파일을 읽어옵니다.
    
    Args:
        chunk_dir: chunk JSON 파일들이 있는 디렉토리
    
    Returns:
        chunk 데이터 리스트
    """
    chunks = []
    chunk_path = Path(chunk_dir)
    
    if not chunk_path.exists():
        print(f"   ⚠️ Directory not found: {chunk_dir}")
        return chunks
    
    json_files = list(chunk_path.glob("*.json"))
    
    for json_file in json_files:
        # all_chunks.json은 제외 (개별 파일만 처리)
        if json_file.name == "all_chunks.json":
            continue
        
        with open(json_file, 'r', encoding='utf-8') as f:
            chunk_data = json.load(f)
            chunk_data['_filepath'] = str(json_file)  # 원본 파일 경로 저장
            chunks.append(chunk_data)
    
    return chunks


def save_chunk_with_entities(chunk_data: dict) -> str:
    """
    엔티티/관계가 추가된 chunk 데이터를 원본 파일에 덮어씁니다.
    
    Args:
        chunk_data: entities, relationships가 포함된 chunk 데이터
    
    Returns:
        저장된 파일 경로
    """
    filepath = chunk_data.get('_filepath')
    
    # _filepath 필드 제거 (내부용)
    save_data = {k: v for k, v in chunk_data.items() if k != '_filepath'}
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)
    
    print(f"   💾 Saved: {filepath}")
    return filepath


