"""
Entity Extraction from Chunk Pipeline

Flow:
1. Extract entities from chunk (LLM)
2. Resolve entity names via OpenSearch (synonym matching)
3. Save entities to Neptune
4. Resolve relationship names using cache
5. Save relationships to Neptune
"""
from typing import List, Dict, Tuple

from utils.helper import (
    generate_chunk_hash, 
    generate_chunk_id, 
    chunk_text,
    get_context_from_review_file,
    get_all_review_files,
    print_pipeline_header,
    print_chunk_stats,
    print_final_stats
)
from utils.parse_utils import parse_extraction_output
from utils.generate_entity import extract_entities
from opensearch.opensearch_con import get_opensearch_client
from opensearch.opensearch_search import resolve_entities_with_cache, resolve_relationships_with_cache
from neptune.cyper_queries import (
    import_nodes_with_dynamic_label, 
    import_relationships_with_dynamic_label,
    delete_all_nodes_and_relationships, 
    get_database_stats
)


# ============================================================
# Step 1: Extract entities
# ============================================================

def extract_entities_from_chunk(chunk: str, movie_context: str = "") -> Tuple[List[Dict], List[Dict]]:
    """Step 1: Extract entities and relationships from chunk"""
    result = extract_entities({"user_query": chunk}, movie_context)
    entities, relationships = parse_extraction_output(result)
    return entities, relationships


# ============================================================
# Step 2: Resolve entity names via OpenSearch
# ============================================================

def resolve_entity_names(
    entities: List[Dict], 
    opensearch_client,
    index_name: str = "entities"
) -> Tuple[List[Dict], Dict[str, str]]:
    """Step 2: Resolve entity names via OpenSearch and return cache"""
    resolved_entities, name_cache = resolve_entities_with_cache(
        entities, opensearch_client, index_name
    )
    return resolved_entities, name_cache


# ============================================================
# Step 3: Save entities to Neptune
# ============================================================

def save_entities_to_neptune(
    entities: List[Dict], 
    movie_id: str, 
    reviewer: str, 
    chunk_id: str, 
    chunk_text: str
) -> None:
    """Step 3: Save entities to Neptune"""
    if not entities:
        return
    import_nodes_with_dynamic_label(entities, movie_id, reviewer, chunk_id, chunk_text)


# ============================================================
# Step 4: Resolve relationship names using cache
# ============================================================

def resolve_relationship_names(
    relationships: List[Dict], 
    name_cache: Dict[str, str],
    opensearch_client,
    index_name: str = "entities"
) -> List[Dict]:
    """Step 4: Resolve relationship entity names using cache"""
    resolved_relationships = resolve_relationships_with_cache(
        relationships, name_cache, opensearch_client, index_name
    )
    return resolved_relationships


# ============================================================
# Step 5: Save relationships to Neptune
# ============================================================

def save_relationships_to_neptune(relationships: List[Dict]) -> None:
    """Step 5: Save relationships to Neptune"""
    if not relationships:
        return
    import_relationships_with_dynamic_label(relationships)


# ============================================================
# Chunk Processing Pipeline
# ============================================================

def process_chunk(
    chunk: str,
    movie_id: str,
    reviewer: str,
    opensearch_client,
    movie_context: str = "",
    index_name: str = "entities"
) -> Dict:
    """
    Process single chunk:
    1. Extract entities
    2. Resolve entity names via OpenSearch
    3. Save entities to Neptune
    4. Resolve relationship names using cache
    5. Save relationships to Neptune
    """
    stats = {'entities_found': 0, 'entities_saved': 0, 'relationships_found': 0, 'relationships_saved': 0}
    
    chunk_hash = generate_chunk_hash(chunk)
    chunk_id = generate_chunk_id(reviewer, chunk_hash)
    
    # Step 1: Extract entities
    entities, relationships = extract_entities_from_chunk(chunk, movie_context)
    stats['entities_found'] = len(entities)
    stats['relationships_found'] = len(relationships)
    
    if not entities:
        return stats
    
    # Step 2: Resolve entity names
    resolved_entities, name_cache = resolve_entity_names(entities, opensearch_client, index_name)
    
    # Step 3: Save entities
    save_entities_to_neptune(resolved_entities, movie_id, reviewer, chunk_id, chunk)
    stats['entities_saved'] = len(resolved_entities)
    
    if not relationships:
        return stats
    
    # Step 4: Resolve relationship names
    resolved_relationships = resolve_relationship_names(relationships, name_cache, opensearch_client, index_name)
    
    # Step 5: Save relationships
    save_relationships_to_neptune(resolved_relationships)
    stats['relationships_saved'] = len(resolved_relationships)
    
    return stats


# ============================================================
# Single Review File Processing
# ============================================================

def process_single_review_file(
    review_filepath: str,
    opensearch_client,
    chunk_size: int = 1500,
    chunk_overlap: int = 100
) -> Dict:
    """
    단일 리뷰 파일 처리
    
    흐름:
    1. 파일명에서 영화/리뷰어 파싱 (Alienoid1_Agony.json -> movie, reviewer)
    2. OpenSearch에서 영화 검색하여 정확한 이름 확인
    3. context, transcript 추출
    4. transcript를 청크로 분할
    5. 각 청크에서 엔티티/관계 추출 및 저장
    """
    import os
    filename = os.path.basename(review_filepath)
    print(f"\n   Processing: {filename}")
    
    # Step 1-2: context, transcript, movie_id, reviewer 추출
    context, transcript, movie_id, reviewer = get_context_from_review_file(review_filepath)
    print(f"   Movie: {movie_id}, Reviewer: {reviewer}")
    
    # Step 3: transcript를 청크로 분할
    chunks = chunk_text(transcript, chunk_size, chunk_overlap)
    print(f"   Chunks: {len(chunks)}")
    
    file_stats = {
        'total_chunks': len(chunks),
        'processed_chunks': 0,
        'total_entities': 0,
        'total_relationships': 0
    }
    
    # Step 4-5: 각 청크 처리
    for i, chunk in enumerate(chunks, 1):
        print(f"   Chunk {i}/{len(chunks)}...")
        
        chunk_stats = process_chunk(
            chunk=chunk,
            movie_id=movie_id,
            reviewer=reviewer,
            opensearch_client=opensearch_client,
            movie_context=context
        )
        
        file_stats['processed_chunks'] += 1
        file_stats['total_entities'] += chunk_stats['entities_saved']
        file_stats['total_relationships'] += chunk_stats['relationships_saved']
    
    print(f"   Done: {file_stats['total_entities']} entities, {file_stats['total_relationships']} relationships")
    return file_stats


# ============================================================
# Main Pipeline - Directory Based
# ============================================================

def run_extraction_pipeline(
    reviews_dir: str = None,
    chunk_size: int = 1500,
    chunk_overlap: int = 100,
    clean_database: bool = True
) -> Dict:
    """
    리뷰 디렉토리 기반 엔티티 추출 파이프라인
    
    Args:
        reviews_dir: 리뷰 파일 디렉토리 (None이면 기본 경로 사용)
        chunk_size: 청크 크기
        chunk_overlap: 청크 오버랩
        clean_database: 데이터베이스 초기화 여부
    """
    print_pipeline_header("Entity Extraction Pipeline Start")
    
    # Initialize OpenSearch
    opensearch_client = get_opensearch_client()
    print("OpenSearch connected")
    
    # Check database stats
    stats = get_database_stats()
    print(f"Neptune: {stats['total_nodes']} nodes, {stats['total_relationships']} relationships")
    
    # Clean database
    if clean_database:
        delete_all_nodes_and_relationships()
        print("Database cleaned")
    
    # Get review files
    if reviews_dir:
        from pathlib import Path
        review_files = list(Path(reviews_dir).glob("*.json"))
    else:
        review_files = get_all_review_files()
    
    print(f"Found {len(review_files)} review files")
    
    # Total stats
    total_stats = {
        'files_processed': 0,
        'total_chunks': 0,
        'processed_chunks': 0,
        'total_entities': 0,
        'total_relationships': 0
    }
    
    # Process each review file
    for i, review_file in enumerate(review_files, 1):
        print_pipeline_header(f"File {i}/{len(review_files)}")
        
        try:
            file_stats = process_single_review_file(
                review_filepath=str(review_file),
                opensearch_client=opensearch_client,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap
            )
            
            total_stats['files_processed'] += 1
            total_stats['total_chunks'] += file_stats['total_chunks']
            total_stats['processed_chunks'] += file_stats['processed_chunks']
            total_stats['total_entities'] += file_stats['total_entities']
            total_stats['total_relationships'] += file_stats['total_relationships']
            
        except Exception as e:
            print(f"   Error: {e}")
    
    print_final_stats(total_stats)
    return total_stats


if __name__ == "__main__":
    run_extraction_pipeline(
        reviews_dir=None,  # Use default path
        chunk_size=1500,
        chunk_overlap=100,
        clean_database=True
    )
