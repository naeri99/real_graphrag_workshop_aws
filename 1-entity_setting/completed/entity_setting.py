from opensearch.opensearch_index_setting import define_chunk_index, delete_index, define_entity_index
from opensearch.opensearch_con import get_opensearch_client


opensearch_conn = get_opensearch_client()

try:
    delete_index(opensearch_conn, "entities")
except:
    print("no entities")

# 올바른 매핑으로 인덱스 생성
define_entity_index(opensearch_conn, "entities")     # 엔티티용 인덱스 (수정됨)


