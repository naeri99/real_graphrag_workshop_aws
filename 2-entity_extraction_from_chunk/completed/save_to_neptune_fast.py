"""
Save to Neptune Pipeline (병렬 버전 - 10 workers + 재시도)
Flow:
1. Read chunks from step/chunkings
2. Save entities to Neptune (병렬)
3. Save relationships to Neptune (병렬)
"""
import json
import os
import sys
import time
import random
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import delete_chunk_index_opensearch
from neptune.cyper_queries import (
    import_nodes_with_dynamic_label,
    import_relationships_with_dynamic_label,
    delete_all_nodes_and_relationships,
    get_database_stats
)

SCRIPT_DIR = Path(__file__).parent.resolve()
CHUNK_DIR = SCRIPT_DIR / "step" / "chunkings"
MAX_WORKERS = 40
MAX_WORKERS_ENTITY = 20
MAX_WORKERS_REL = 1
MAX_RETRIES = 5

# 실패 큐
failed_queue = []
failed_queue_lock = threading.Lock()
stats_lock = threading.Lock()
total_stats = {
    'chunks_processed': 0, 'chunks_failed': 0,
    'entities_saved': 0, 'entities_existing': 0, 'entities_new': 0,
    'relationships_saved': 0, 'relationships_existing': 0, 'relationships_new': 0,
}


def read_chunks_from_dir(chunk_dir: Path) -> list:
    chunks = []
    if not chunk_dir.exists():
        print(f"⚠️ Directory not found: {chunk_dir}")
        return chunks
    for json_file in sorted(chunk_dir.glob("*.json")):
        if json_file.name == "all_chunks.json":
            continue
        with open(json_file, 'r', encoding='utf-8') as f:
            chunk_data = json.load(f)
            chunk_data['_filepath'] = str(json_file)
            chunks.append(chunk_data)
    return chunks


def process_entities(idx: int, total: int, chunk: dict) -> bool:
    """엔티티만 저장 (1단계)"""
    chunk_id = chunk.get('chunk_id', 'unknown')
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            chunk_hash = chunk.get('chunk_hash', '')
            movie_id = chunk.get('movie_id', '')
            reviewer = chunk.get('reviewer', '')
            user_query = chunk.get('user_query', '')
            entities = chunk.get('entities', [])
            entity_resolution = chunk.get('entity_resolution', {})

            resolved_entities = []
            for ent in entities:
                original_name = ent.get('entity_name', '')
                resolution = entity_resolution.get(original_name, {})
                resolved_name = resolution.get('resolved_name', original_name)
                resolved_ent = ent.copy()
                resolved_ent['entity_name'] = resolved_name
                resolved_entities.append(resolved_ent)

            e_total, e_existing, e_new = 0, 0, 0
            if resolved_entities:
                save_result = import_nodes_with_dynamic_label(
                    resolved_entities, movie_id, reviewer, chunk_id, user_query, chunk_hash
                )
                es = save_result.get('stats', {})
                e_total = es.get('total', len(resolved_entities))
                e_existing = es.get('existing', 0)
                e_new = es.get('new', 0)

            with stats_lock:
                total_stats['chunks_processed'] += 1
                total_stats['entities_saved'] += e_total
                total_stats['entities_existing'] += e_existing
                total_stats['entities_new'] += e_new

            print(f"✅ [{idx}/{total}] {chunk_id} | entities: {e_total}")
            return True
        except Exception as e:
            err_msg = str(e)
            if "ConcurrentModification" in err_msg and attempt < MAX_RETRIES:
                wait = attempt * 0.5
                print(f"⚠️ [{idx}/{total}] {chunk_id} | 충돌, {wait}s 후 재시도 ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            with stats_lock:
                total_stats['chunks_failed'] += 1
            print(f"❌ [{idx}/{total}] {chunk_id} | 에러: {e}")
            with failed_queue_lock:
                failed_queue.append(chunk)
            return False


def process_relationships(idx: int, total: int, chunk: dict) -> bool:
    """관계만 저장 (2단계)"""
    chunk_id = chunk.get('chunk_id', 'unknown')
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            relationships = chunk.get('relationships', [])
            entity_resolution = chunk.get('entity_resolution', {})

            resolved_relationships = []
            for rel in relationships:
                resolved_rel = rel.copy()
                src_name = rel.get('source_entity', '')
                src_res = entity_resolution.get(src_name, {})
                resolved_rel['source_entity'] = src_res.get('resolved_name', src_name)
                tgt_name = rel.get('target_entity', '')
                tgt_res = entity_resolution.get(tgt_name, {})
                resolved_rel['target_entity'] = tgt_res.get('resolved_name', tgt_name)
                resolved_relationships.append(resolved_rel)

            r_total, r_existing, r_new = 0, 0, 0
            if resolved_relationships:
                rel_result = import_relationships_with_dynamic_label(resolved_relationships)
                rs = rel_result.get('stats', {})
                r_total = rs.get('total', len(resolved_relationships))
                r_existing = rs.get('existing', 0)
                r_new = rs.get('new', 0)

            with stats_lock:
                total_stats['relationships_saved'] += r_total
                total_stats['relationships_existing'] += r_existing
                total_stats['relationships_new'] += r_new

            print(f"🔗 [{idx}/{total}] {chunk_id} | rels: {r_total}")
            return True
        except Exception as e:
            err_msg = str(e)
            if "ConcurrentModification" in err_msg and attempt < MAX_RETRIES:
                wait = attempt * 0.5
                print(f"⚠️ [{idx}/{total}] {chunk_id} | 충돌, {wait}s 후 재시도 ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            with failed_queue_lock:
                failed_queue.append(chunk)
            print(f"❌ [{idx}/{total}] {chunk_id} | 에러: {e}")
            return False


def run_parallel_with_retry(chunks, process_fn, label, workers):
    """병렬 실행 + 실패 큐 재처리"""
    random.shuffle(chunks)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(process_fn, i, len(chunks), chunk): chunk
            for i, chunk in enumerate(chunks, 1)
        }
        for future in as_completed(futures):
            future.result()

    retry_round = 1
    while failed_queue:
        retry_chunks = list(failed_queue)
        failed_queue.clear()
        random.shuffle(retry_chunks)
        print(f"\n🔄 {label} 실패 큐 재처리 (round {retry_round}, {len(retry_chunks)}개)")
        time.sleep(2)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_fn, i, len(retry_chunks), chunk): chunk
                for i, chunk in enumerate(retry_chunks, 1)
            }
            for future in as_completed(futures):
                future.result()
        retry_round += 1
        if retry_round > 5:
            print(f"⚠️ {label} 재처리 5회 초과, 남은 실패: {len(failed_queue)}개")
            break


def run(clean_database: bool = True):
    print("=" * 60)
    print("🚀 Save to Neptune Pipeline (Entity → Relationship 순차)")
    print("=" * 60)

    stats = get_database_stats()
    print(f"� Neptune: {stats['total_nodes']} nodes, {stats['total_relationships']} relationships")

    if clean_database:
        delete_all_nodes_and_relationships()
        print("🗑️ Database cleaned")
        delete_chunk_index_opensearch()

    chunks = read_chunks_from_dir(CHUNK_DIR)
    print(f"📝 Loaded Chunks: {len(chunks)}")
    if not chunks:
        print("⚠️ No chunks found to process")
        return

    # === 1단계: Entity 저장 ===
    print(f"\n{'='*60}")
    print(f"📦 1단계: Entity 저장 ({MAX_WORKERS_ENTITY} workers)")
    print('='*60)
    run_parallel_with_retry(list(chunks), process_entities, "Entity", MAX_WORKERS_ENTITY)

    print(f"\n  Entity 결과: 저장 {total_stats['entities_saved']}, 처리 {total_stats['chunks_processed']}, 실패 {total_stats['chunks_failed']}")

    # 실패 큐 초기화
    failed_queue.clear()

    # === 2단계: Relationship 저장 ===
    print(f"\n{'='*60}")
    print(f"🔗 2단계: Relationship 저장 ({MAX_WORKERS_REL} workers)")
    print('='*60)
    run_parallel_with_retry(list(chunks), process_relationships, "Relationship", MAX_WORKERS_REL)

    print(f"\n{'='*60}")
    print("🎯 완료!")
    print(f"  Chunks 처리: {total_stats['chunks_processed']} (실패: {total_stats['chunks_failed']})")
    print(f"  Entities: {total_stats['entities_saved']} (기존: {total_stats['entities_existing']}, 신규: {total_stats['entities_new']})")
    print(f"  Relationships: {total_stats['relationships_saved']} (기존: {total_stats['relationships_existing']}, 신규: {total_stats['relationships_new']})")

    final = get_database_stats()
    print(f"\n📊 Final Neptune: {final['total_nodes']} nodes, {final['total_relationships']} relationships")


if __name__ == "__main__":
    run(clean_database=True)
