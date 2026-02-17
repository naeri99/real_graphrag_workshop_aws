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


def save_entities_to_neptune(resolved_entities: list, movie_id: str, reviewer: str, 
                             chunk_id: str, user_query: str, chunk_hash: str) -> dict:
    """
    엔티티들을 Neptune에 저장합니다.
    
    Returns:
        저장 통계 dict (total, existing, new)
    """
    if not resolved_entities:
        return {'total': 0, 'existing': 0, 'new': 0}
    
    save_result = import_nodes_with_dynamic_label(
        resolved_entities, movie_id, reviewer, chunk_id, user_query, chunk_hash
    )
    entity_stats = save_result.get('stats', {})
    
    total_count = entity_stats.get('total', len(resolved_entities))
    existing_count = entity_stats.get('existing', 0)
    new_count = entity_stats.get('new', 0)
    
    print(f"   💾 Entities: {total_count}개 저장 (기존: {existing_count}, 신규: {new_count})")
    
    return {'total': total_count, 'existing': existing_count, 'new': new_count}


def save_relationships_to_neptune(relationships: list, entity_resolution: dict) -> dict:
    """
    관계들을 Neptune에 저장합니다. entity_resolution을 적용하여 resolved_name으로 변환합니다.
    
    Returns:
        저장 통계 dict (total, existing, new)
    """
    if not relationships:
        return {'total': 0, 'existing': 0, 'new': 0}
    
    # 관계의 엔티티 이름도 resolution 적용
    resolved_relationships = []
    for rel in relationships:
        resolved_rel = rel.copy()
        
        # source_entity resolution
        src_name = rel.get('source_entity', '')
        src_resolution = entity_resolution.get(src_name, {})
        resolved_rel['source_entity'] = src_resolution.get('resolved_name', src_name)
        
        # target_entity resolution
        tgt_name = rel.get('target_entity', '')
        tgt_resolution = entity_resolution.get(tgt_name, {})
        resolved_rel['target_entity'] = tgt_resolution.get('resolved_name', tgt_name)
        
        resolved_relationships.append(resolved_rel)
    
    # Neptune에 관계 저장
    rel_save_result = import_relationships_with_dynamic_label(resolved_relationships)
    rel_stats = rel_save_result.get('stats', {})
    
    total_count = rel_stats.get('total', len(resolved_relationships))
    existing_count = rel_stats.get('existing', 0)
    new_count = rel_stats.get('new', 0)
    
    print(f"   🔗 Relationships: {total_count}개 저장 (기존: {existing_count}, 신규: {new_count})")
    
    return {'total': total_count, 'existing': existing_count, 'new': new_count}


def run_save_to_neptune_pipeline(
    chunk_dir: str = "./step/chunkings",
    clean_database: bool = True
):
    """
    ./step/chunkings에서 chunk 파일들을 읽어 Neptune에 저장합니다.
    """
    print("=" * 60)
    print("🚀 Save to Neptune Pipeline Start")
    print("=" * 60)
    
    # Neptune 상태 확인
    stats = get_database_stats()
    print(f"📊 Neptune: {stats['total_nodes']} nodes, {stats['total_relationships']} relationships")
    
    if clean_database:
        delete_all_nodes_and_relationships()
        print("🗑️ Database cleaned")
        delete_chunk_index_opensearch()
    
    # chunk 파일들 읽기
    chunks = read_chunks_from_dir(chunk_dir)
    print(f"   📝 Loaded Chunks: {len(chunks)}")
    
    if not chunks:
        print("   ⚠️ No chunks found to process")
        return
    
    # 통계
    total = {
        'chunks_processed': 0,
        'entities_saved': 0,
        'entities_existing': 0,
        'entities_new': 0,
        'relationships_saved': 0,
        'relationships_existing': 0,
        'relationships_new': 0
    }
    
    for j, chunk in enumerate(chunks, 1):
        print(f"\n   --- Chunk {j}/{len(chunks)} ---")
        print(f"   📄 ID: {chunk.get('chunk_id', 'unknown')}")
        
        try:
            chunk_id = chunk.get('chunk_id', '')
            chunk_hash = chunk.get('chunk_hash', '')
            movie_id = chunk.get('movie_id', '')
            reviewer = chunk.get('reviewer', '')
            user_query = chunk.get('user_query', '')
            entities = chunk.get('entities', [])
            relationships = chunk.get('relationships', [])
            entity_resolution = chunk.get('entity_resolution', {})
            
            # entity_resolution을 적용하여 resolved_name으로 변환
            resolved_entities = []
            for ent in entities:
                original_name = ent.get('entity_name', '')
                resolution = entity_resolution.get(original_name, {})
                resolved_name = resolution.get('resolved_name', original_name)
                
                resolved_ent = ent.copy()
                resolved_ent['entity_name'] = resolved_name
                resolved_entities.append(resolved_ent)
            
            # Neptune에 엔티티 저장
            entity_result = save_entities_to_neptune(
                resolved_entities, movie_id, reviewer, chunk_id, user_query, chunk_hash
            )
            total['entities_saved'] += entity_result['total']
            total['entities_existing'] += entity_result['existing']
            total['entities_new'] += entity_result['new']
            
            # Neptune에 관계 저장
            rel_result = save_relationships_to_neptune(
                relationships, entity_resolution
            )
            total['relationships_saved'] += rel_result['total']
            total['relationships_existing'] += rel_result['existing']
            total['relationships_new'] += rel_result['new']
            
            total['chunks_processed'] += 1
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    # 최종 결과
    print("\n" + "=" * 60)
    print("🎉 Pipeline Complete!")
    print("=" * 60)
    print(f"Chunks processed: {total['chunks_processed']}")
    print(f"Entities saved: {total['entities_saved']}")
    print(f"  └─ 기존 (덮어쓰기): {total['entities_existing']}")
    print(f"  └─ 신규 생성: {total['entities_new']}")
    print(f"Relationships saved: {total['relationships_saved']}")
    print(f"  └─ 기존 (덮어쓰기): {total['relationships_existing']}")
    print(f"  └─ 신규 생성: {total['relationships_new']}")
    
    final_stats = get_database_stats()
    print(f"\n📊 Final Neptune Stats:")
    print(f"  - Total nodes: {final_stats['total_nodes']}")
    print(f"  - Total relationships: {final_stats['total_relationships']}")
    
    return total


if __name__ == "__main__":
    run_save_to_neptune_pipeline(
        chunk_dir="./step/chunkings",
        clean_database=True
    )
