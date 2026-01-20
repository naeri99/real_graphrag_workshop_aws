"""
Neptune Cypher 쿼리 유틸리티
- 3-entity_relationship_summary에서 사용하는 함수만 포함
"""
from neptune.neptune_con import execute_cypher
import json


def save_entity_summary(entity_name, summary, entity_type=None):
    """Save entity summary to Neptune graph."""
    if entity_type:
        query = f"""
        MATCH (n:{entity_type})
        WHERE n.name = $entity_name
        SET n.summary = $summary
        RETURN n.name AS name, labels(n) AS labels
        """
    else:
        query = """
        MATCH (n)
        WHERE n.name = $entity_name
        SET n.summary = $summary
        RETURN n.name AS name, labels(n) AS labels
        """
    return execute_cypher(query, entity_name=entity_name, summary=summary)


def get_all_entities_for_summary():
    """Get all entities that need summarization (have description but no summary)."""
    query = """
    MATCH (n)
    WHERE n.name IS NOT NULL 
      AND n.description IS NOT NULL 
      AND (n.summary IS NULL OR n.summary = '')
      AND NOT n:__Chunk__ 
      AND NOT n:MOVIE 
      AND NOT n:REVIEWER
    RETURN n.name AS name, labels(n) AS entity_type, n.description AS description, n.neptune_id AS neptune_id
    """
    return execute_cypher(query)


def get_all_relationships_for_summary():
    """Get all relationships that need summarization."""
    query = """
    MATCH (s)-[r:RELATIONSHIP]-(t)
    WHERE id(s) < id(t)
      AND r.description IS NOT NULL
      AND (r.summary IS NULL OR r.summary = '')
    RETURN labels(s) AS source_type, s.name AS source,
           labels(t) AS target_type, t.name AS target,
           r.description AS description_list, r.strength AS strength
    """
    result = execute_cypher(query)
    
    # Parse description_list from JSON string to list
    if result and 'results' in result:
        for item in result['results']:
            desc = item.get('description_list', '[]')
            if isinstance(desc, str):
                try:
                    item['description_list'] = json.loads(desc)
                except:
                    item['description_list'] = [desc]
    
    return result


def save_relationship_summary(source_entity, target_entity, summary, source_type=None, target_type=None):
    """Save relationship summary to Neptune graph."""
    query = """
    MATCH (s)-[r:RELATIONSHIP]-(t)
    WHERE (s.name = $source_entity AND t.name = $target_entity) 
       OR (s.name = $target_entity AND t.name = $source_entity)
    SET r.summary = $summary
    RETURN s.name AS source, t.name AS target, r.summary AS summary
    """
    return execute_cypher(query, source_entity=source_entity, target_entity=target_entity, summary=summary)
