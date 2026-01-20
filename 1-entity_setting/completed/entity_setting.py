from opensearch.opensearch_index_setting import delete_index, define_entity_index
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearh_search import find_entity_opensearch
from utils.parse_utils import parse_mixed_synonym_output
from utils.generate_entity import extract_synonym
from utils.synonym import (
    clean_entities_whitespace,
    merge_synonyms_with_set,
    update_entity_synonyms
)
from utils.movie_context import get_context_from_review_file, get_all_review_files
from langchain_text_splitters import RecursiveCharacterTextSplitter
import os


# ============================================================
# OpenSearch 인덱스 초기화
# ============================================================
opensearch_conn = get_opensearch_client()

try:
    delete_index(opensearch_conn, "entities")
except:
    print("no entities")

define_entity_index(opensearch_conn, "entities")