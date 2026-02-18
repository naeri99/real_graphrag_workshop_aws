"""
Entity Resolution Pipeline
Flow:
1. Read chunks from ./step/chunkings
2. Resolve entity names via OpenSearch (synonym matching)
3. Save entity_resolution hashmap back to JSON
"""
import json
import os
from pathlib import Path
from opensearch.opensearch_search import resolve_entities
import time 

def read_chunks_from_dir(chunk_dir: str = "./step/chunkings") -> list:
    """
    ./step/chunkings 디렉토리에서 모든 JSON 파일을 읽어옵니다.
    """
    chunks = []
    chunk_path = Path(chunk_dir)
    
    if not chunk_path.exists():
        print(f"   ⚠️ Directory not found: {chunk_dir}")
        return chunks
    
    json_files = list(chunk_path.glob("*.json"))
    
    for json_file in json_files:
        if json_file.name == "all_chunks.json":
            continue
        
        with open(json_file, 'r', encoding='utf-8') as f:
            chunk_data = json.load(f)
            chunk_data['_filepath'] = str(json_file)
            chunks.append(chunk_data)
    
    return chunks

def save_chunk_with_entities(chunk_data: dict) -> str:
    """
    엔티티/관계가 추가된 chunk 데이터를 원본 파일에 덮어씁니다.
    """
    filepath = chunk_data.get('_filepath')
    save_data = {k: v for k, v in chunk_data.items() if k != '_filepath'}
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)
    
    return filepath
