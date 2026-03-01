"""
entities_opensearch/ 디렉토리의 JSON 파일들을 읽어
Bedrock 임베딩 생성 후 OpenSearch에 병렬 인덱싱
"""
import json, glob, os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from opensearch.opensearch_index_setting import delete_index, define_entity_index, define_chunk_index
from opensearch.opensearch_con import get_opensearch_client
from utils.bedrock_embedding import BedrockEmbedding

ENTITIES_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), os.pardir, os.pardir,
    "data", "entities_opensearch_import"
)
ENTITIES_DIR = os.path.normpath(ENTITIES_DIR)

# 병렬 워커 수 (Bedrock throttling 고려)
MAX_WORKERS = 10

print_lock = Lock()
stats_lock = Lock()


def process_one(fpath, index_name, opensearch_client, embedder, stats, counter, total):
    """단일 entity JSON 처리: 임베딩 생성 + OpenSearch 인덱싱"""
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        entity = data["entity"]
        etype = entity["entity_type"]
        name = entity["name"]
        summary = entity["summary"]

        # summary_vec이 이미 있으면 그대로 사용, 없으면 임베딩 생성
        summary_vec = entity.get("summary_vec")
        if not summary_vec:
            summary_vec = embedder.embed_text(summary)
        entity["summary_vec"] = summary_vec

        # neptune_id: 없으면 null로 설정, 있으면 그대로
        if "neptune_id" not in entity:
            entity["neptune_id"] = None

        doc_id = os.path.splitext(os.path.basename(fpath))[0]
        opensearch_client.index(index=index_name, id=doc_id, body={"entity": entity})

        with stats_lock:
            stats[etype] = stats.get(etype, 0) + 1
            counter[0] += 1
            c = counter[0]

        if c % 100 == 0 or c == total:
            with print_lock:
                print(f"   [{c}/{total}] {etype}: {name}")

        return None
    except Exception as e:
        return os.path.basename(fpath), str(e)


def run_entity_indexing(index_name: str = "entities"):
    print("🚀 entities_opensearch → OpenSearch 병렬 인덱싱 시작")
    print(f"   워커 수: {MAX_WORKERS}")
    print("=" * 60)

    opensearch_client = get_opensearch_client()
    embedder = BedrockEmbedding()

    # Step 1: 인덱스 초기화
    print("\n📦 Step 1: 인덱스 초기화")
    try:
        delete_index(opensearch_client, index_name)
        delete_index(opensearch_client, "chunks")
    except:
        pass
    define_entity_index(opensearch_client, index_name)
    define_chunk_index(opensearch_client, "chunks")

    # Step 2: JSON 파일 로드
    files = sorted(glob.glob(os.path.join(ENTITIES_DIR, "*.json")))
    total = len(files)
    print(f"\n📂 Step 2: {total}개 entity JSON 로드")

    stats = {"MOVIE": 0, "REVIEWER": 0, "ACTOR": 0, "MOVIE_CHARACTER": 0, "MOVIE_STAFF": 0}
    errors = 0
    counter = [0]  # mutable for thread access

    start_time = time.time()

    # Step 3: 병렬 인덱싱
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_one, fp, index_name, opensearch_client, embedder, stats, counter, total): fp
            for fp in files
        }
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                errors += 1
                print(f"   ❌ {result[0]}: {result[1]}")

    elapsed = time.time() - start_time

    # 인덱스 새로고침
    try:
        opensearch_client.indices.refresh(index=index_name)
    except Exception as e:
        print(f"⚠️ 인덱스 새로고침 실패: {e}")

    # 결과
    indexed = sum(stats.values())
    print("\n" + "=" * 60)
    print("🎯 인덱싱 완료!")
    for k, v in stats.items():
        print(f"   {k}: {v}")
    print(f"   총 인덱싱: {indexed}개 (에러: {errors}개)")
    print(f"   소요 시간: {elapsed:.1f}초")
    return stats


if __name__ == "__main__":
    run_entity_indexing()
