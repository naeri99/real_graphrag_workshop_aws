"""
Neptune __Chunk__ 노드를 읽어 Bedrock 임베딩 생성 후 OpenSearch chunks 인덱스에 저장
- Neptune에서 __Chunk__ 노드 전체 조회
- 각 chunk의 text를 Bedrock Titan으로 임베딩
- OpenSearch chunks 인덱스에 저장 (context, context_vec, neptune_id)
- 병렬 처리 (ThreadPoolExecutor)
"""
import os
import sys
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from neptune.neptune_con import execute_cypher
from opensearch.opensearch_con import get_opensearch_client
from utils.bedrock_embedding import BedrockEmbedding

MAX_WORKERS = 10
BATCH_SIZE = 200  # Neptune 쿼리 페이징

stats_lock = threading.Lock()
print_lock = threading.Lock()
stats = {'success': 0, 'error': 0}


def fetch_all_chunks() -> list:
    """Neptune에서 모든 __Chunk__ 노드를 가져옵니다."""
    print("📦 Neptune에서 __Chunk__ 노드 조회 중...")

    # 전체 개수 확인
    count_result = execute_cypher("MATCH (c:__Chunk__) RETURN count(c) as cnt")
    total = 0
    if count_result and 'results' in count_result:
        total = count_result['results'][0].get('cnt', 0)
    print(f"   총 __Chunk__ 노드: {total}개")

    # 전체 조회 (id, text, neptune_id)
    query = """
    MATCH (c:__Chunk__)
    RETURN c.id AS id, c.text AS text, id(c) AS neptune_id
    """
    result = execute_cypher(query)
    if not result or 'results' not in result:
        print("❌ __Chunk__ 조회 실패")
        return []

    chunks = result['results']
    print(f"   조회 완료: {len(chunks)}개")
    return chunks


def process_chunk(idx, total, chunk, opensearch_client, embedder):
    """단일 chunk 처리: 임베딩 생성 + OpenSearch 인덱싱"""
    try:
        chunk_id = chunk.get('id', '')
        text = chunk.get('text', '')

        if not text:
            with print_lock:
                print(f"⚠️ [{idx}/{total}] {chunk_id} | text 없음, 스킵")
            return

        # Bedrock 임베딩 생성
        context_vec = embedder.embed_text(text)

        # OpenSearch에 저장
        # neptune_id = c.id (chunk의 id 속성) → movie_search_chunk.py에서 이 값으로 Neptune 매칭
        doc = {
            "chunk": {
                "context": text,
                "context_vec": context_vec,
                "neptune_id": chunk_id
            }
        }

        doc_id = chunk_id
        opensearch_client.index(index="chunks", id=doc_id, body=doc)

        with stats_lock:
            stats['success'] += 1
            cnt = stats['success']

        if cnt % 50 == 0 or cnt == total:
            with print_lock:
                print(f"✅ [{cnt}/{total}] 저장 완료")

    except Exception as e:
        with stats_lock:
            stats['error'] += 1
        with print_lock:
            print(f"❌ [{idx}/{total}] {chunk.get('id', '?')} | 에러: {e}")


def run():
    print("=" * 60)
    print("🚀 Neptune __Chunk__ → OpenSearch chunks 임포트")
    print(f"   Workers: {MAX_WORKERS}")
    print("=" * 60)

    # 1. Neptune에서 __Chunk__ 조회
    chunks = fetch_all_chunks()
    if not chunks:
        print("⚠️ 처리할 chunk가 없습니다.")
        return

    total = len(chunks)

    # 2. OpenSearch 클라이언트 + 임베딩 클라이언트
    opensearch_client = get_opensearch_client()
    embedder = BedrockEmbedding()

    # 3. 기존 chunks 인덱스 문서 수 확인
    try:
        count_resp = opensearch_client.count(index="chunks")
        existing = count_resp.get('count', 0)
        print(f"📊 OpenSearch chunks 기존 문서: {existing}개")
    except:
        print("📊 OpenSearch chunks 인덱스 없음 또는 비어있음")

    # 4. 병렬 임베딩 + 인덱싱
    print(f"\n{'='*60}")
    print(f"📦 임베딩 생성 + OpenSearch 인덱싱 ({MAX_WORKERS} workers)")
    print("=" * 60)

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_chunk, i, total, chunk, opensearch_client, embedder): chunk
            for i, chunk in enumerate(chunks, 1)
        }
        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start_time

    # 5. 인덱스 새로고침
    try:
        opensearch_client.indices.refresh(index="chunks")
    except Exception as e:
        print(f"⚠️ 인덱스 새로고침 실패: {e}")

    # 6. 최종 결과
    try:
        final_count = opensearch_client.count(index="chunks").get('count', 0)
    except:
        final_count = '?'

    print(f"\n{'='*60}")
    print("🎯 완료!")
    print(f"   성공: {stats['success']}개")
    print(f"   에러: {stats['error']}개")
    print(f"   소요 시간: {elapsed:.1f}초")
    print(f"   OpenSearch chunks 최종 문서 수: {final_count}")


if __name__ == "__main__":
    run()
