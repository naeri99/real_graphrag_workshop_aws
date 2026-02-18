"""
Save to Neptune Pipeline
Flow:
1. Read chunks from ./step/chunkings (with entities, relationships, entity_resolution)
2. Save entities to Neptune
3. Save relationships to Neptune
"""
import json
from pathlib import Path
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import delete_chunk_index_opensearch
from neptune.cyper_queries import (
    import_nodes_with_dynamic_label,
    import_relationships_with_dynamic_label,
    delete_all_nodes_and_relationships,
    get_database_stats
)


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


