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
            chunk_data['_filepath'] = str(json_file.resolve())  # 절대 경로로 저장
            print(f"   📖 Read from: {chunk_data['_filepath']}")
            chunks.append(chunk_data)
    
    return chunks


def save_chunk_with_entities(chunk_data: dict) -> str:
    """
    엔티티/관계가 추가된 chunk 데이터를 원본 파일에 덮어씁니다.
    """
    filepath = chunk_data.get('_filepath')
    save_data = {k: v for k, v in chunk_data.items() if k != '_filepath'}
    
    print(f"   💾 Saving to: {filepath}")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)
    
    return filepath


def resolve_and_build_hashmap(entities: list) -> dict:
    """
    엔티티들을 OpenSearch로 resolve하고 hashmap 형태로 반환합니다.
    """
    resolved_entities, metrics = resolve_entities(entities)
    
    hashmap = {}
    
    print(f"\n   📊 Entity Resolution: Search Entity in Neptune via OpenSearch Synonym")
    for ent in resolved_entities:
        original = ent.get('_original_name', ent['entity_name'])
        resolved_name = ent['entity_name']
        etype = ent['entity_type']
        matched = ent.get('_matched', False)
        match_type = ent.get('_match_type', 'not_found')
        
        # hashmap에 저장
        hashmap[original] = {
            "description": f"{original} -> {resolved_name}",
            "resolved_name": resolved_name,
            "entity_type": etype,
            "matched": matched,
            "match_type": match_type
        }
        
        if matched:
            if original != resolved_name:
                print(f"      ✅ '{original}' → '{resolved_name}' ({etype}) [{match_type}]")
            else:
                print(f"      ✅ '{original}' ({etype}) [{match_type}]")
        else:
            print(f"      🆕 '{original}' ({etype}) [NEW]")
    
    return hashmap


def run_entity_resolution_pipeline(chunk_dir: str = "./step/chunkings"):
    """
    ./step/chunkings에서 chunk 파일들을 읽어 엔티티를 resolve하고 원본 파일에 덮어씁니다.
    """
    chunks = read_chunks_from_dir(chunk_dir)
    print(f"   📝 Loaded Chunks: {len(chunks)}")
    
    if not chunks:
        print("   ⚠️ No chunks found to process")
        return
    
    for j, chunk in enumerate(chunks, 1):
        print(f"\n   --- Chunk {j}/{len(chunks)} ---")
        print(f"   📄 ID: {chunk.get('chunk_id', 'unknown')}")
        
        entities = chunk.get('entities', [])
        if not entities:
            print(f"   ⚠️ No entities found in chunk")
            continue
        
        print(f"   📄 Entities count: {len(entities)}")
        
        # Entity resolution 수행 및 hashmap 생성
        hashmap = resolve_and_build_hashmap(entities)
        
        # chunk에 entity_resolution 추가
        chunk["entity_resolution"] = hashmap
        
        # 저장
        save_chunk_with_entities(chunk)
        time.sleep(1)
        print(f"   💾 Saved: {chunk.get('_filepath')}")
    
    print(f"\n{'='*60}")
    print(f"✅ Entity resolution completed for {len(chunks)} chunks")


if __name__ == "__main__":
    run_entity_resolution_pipeline(
        chunk_dir="./step/chunkings"
    )
