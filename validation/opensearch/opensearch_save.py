from opensearch.opensearch_con import get_opensearch_client
import uuid

EMPTY_VECTOR = [0.0] * 1024


def generate_chunk_id(reviewer_name):
    """reviewer_chunk_id 형식의 unique ID 생성"""
    unique_suffix = str(uuid.uuid4())[:8]
    return f"{reviewer_name}_{unique_suffix}"


def save_chunk(opensearch_client, index_name, chunk_data, reviewer_name=None):
    """청크 저장 - chunk_id 필수 (자동 생성 가능), 나머지 null 허용"""
    chunk_id = chunk_data.get("chunk_id")
    
    if not chunk_id:
        if reviewer_name:
            chunk_id = generate_chunk_id(reviewer_name)
        else:
            raise ValueError("chunk_id is required or provide reviewer_name")
    
    doc = {
        "chunk": {
            "chunk_id": chunk_id,
            "origin": chunk_data.get("origin")
        }
    }
    
    return _save_document(opensearch_client, index_name, doc)


def save_entity(opensearch_client, index_name, entity_data):
    """엔티티 저장 - name 필수, 나머지 null 허용"""
    if not entity_data.get("name"):
        raise ValueError("entity.name is required")
    
    doc = {
        "entity": {
            "name": entity_data["name"],
            "synonym": entity_data.get("synonym"),
            "entity_type": entity_data["entity_type"],
            "summary": entity_data.get("summary"),
            "summary_vec": entity_data.get("summary_vec") or EMPTY_VECTOR,
            "neptune_id": entity_data.get("neptune_id")
        }
    }
    
    return _save_document(opensearch_client, index_name, doc)


def update_neptune_id(opensearch_client, index_name, doc_id, entity_type, neptune_id):
    """neptune_id 업데이트 - 모든 엔티티 공용"""
    doc = {
        "doc": {
            entity_type: {
                "neptune_id": neptune_id
            }
        }
    }
    
    try:
        response = opensearch_client.update(
            index=index_name,
            id=doc_id,
            body=doc,
            refresh=True
        )
        print(f"neptune_id updated successfully: {doc_id}")
        return response
    except Exception as e:
        print(f"Error updating neptune_id: {e}")
        return None


def _save_document(opensearch_client, index_name, doc):
    """문서 저장 공통 함수"""
    try:
        response = opensearch_client.index(
            index=index_name,
            body=doc,
            refresh=True
        )
        print(f"Document saved successfully: {response['_id']}")
        return response
    except Exception as e:
        print(f"Error saving document: {e}")
        return None
