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


def run_entity_extraction_pipeline(
    chunk_dir: str = "./step/chunkings"
):
    """
    ./step/chunkings에서 chunk 파일들을 읽어 엔티티/관계를 추출하고 원본 파일에 덮어씁니다.
    
    Args:
        chunk_dir: chunk JSON 파일들이 있는 디렉토리
    """
    # chunk 파일들 읽기
    chunks = read_chunks_from_dir(chunk_dir)
    print(f"   📝 Loaded Chunks: {len(chunks)}")
    
    if not chunks:
        print("   ⚠️ No chunks found to process")
        return
    
    for j, chunk in enumerate(chunks, 1):
        print(f"\n   --- Chunk {j}/{len(chunks)} ---")
        print(f"   📄 ID: {chunk.get('chunk_id', 'unknown')}")
        print(f"   📝 Chunk: {chunk.get('user_query', '')[:400]}...")
        
        # Step 1: LLM으로 엔티티/관계 추출
        print(f"\n   --- Chunk Reformation ---")

        result = extract_entities({"user_query": chunk.get('user_query', '')})
        entities, relationships = parse_extraction_output(result)
        
        # chunk에 엔티티/관계 추가
        chunk["entities"] = entities
        chunk["relationships"] = relationships
        
        print(f"   ✅ Entities: {len(entities)}, Relationships: {len(relationships)}")
        
        # Step 2: 원본 파일에 덮어쓰기
        save_chunk_with_entities(chunk)
        print(f"   💾 Saved: {chunk.get('_filepath')}")
    
    print(f"\n{'='*60}")
    print(f"✅ Entity extraction completed for {len(chunks)} chunks")


if __name__ == "__main__":
    run_entity_extraction_pipeline(
        chunk_dir="./step/chunkings"
    )
