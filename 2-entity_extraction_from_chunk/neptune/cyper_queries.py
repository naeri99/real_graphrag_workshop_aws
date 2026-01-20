"""
Neptune Cypher 쿼리 유틸리티
"""
from neptune.neptune_con import execute_cypher
import uuid
import re
import json


def generate_neptune_id(name, entity_type):
    """
    Neptune ID 생성: 이름_엔티티타입_UUID
    공백과 특수문자는 언더스코어로 변환
    """
    # 이름에서 공백과 특수문자를 언더스코어로 변환
    clean_name = re.sub(r'[^\w가-힣]', '_', name)
    # 연속된 언더스코어를 하나로 변환
    clean_name = re.sub(r'_+', '_', clean_name)
    # 앞뒤 언더스코어 제거
    clean_name = clean_name.strip('_')
    
    # UUID 생성 (8자리)
    unique_id = str(uuid.uuid4())[:8]
    
    # 최종 ID: 이름_엔티티타입_UUID
    neptune_id = f"{clean_name}_{entity_type}_{unique_id}"
    
    return neptune_id


def check_entity_exists(entity_name, entity_type):
    """
    Check if an entity already exists in Neptune.
    Returns True if exists, False otherwise.
    """
    find_query = f"""
    MATCH (n:{entity_type} {{name: $entity_name}})
    RETURN count(n) AS count
    """
    result = execute_cypher(find_query, entity_name=entity_name)
    if result and 'results' in result and result['results']:
        return result['results'][0].get('count', 0) > 0
    return False


def check_relationship_exists(entity1, entity2):
    """
    Check if a relationship already exists between two entities in Neptune.
    Returns True if exists, False otherwise.
    """
    find_query = """
    MATCH (a)-[r:RELATIONSHIP]-(b)
    WHERE (a.name = $entity1 AND b.name = $entity2) OR (a.name = $entity2 AND b.name = $entity1)
    RETURN count(r) AS count
    """
    result = execute_cypher(find_query, entity1=entity1, entity2=entity2)
    if result and 'results' in result and result['results']:
        return result['results'][0].get('count', 0) > 0
    return False


def import_nodes_with_dynamic_label(entities, movie_id, reviewer_id, chunk_id, text):
    """
    Import nodes with dynamic labels by grouping entities by type.
    If entity exists, append new descriptions to existing ones.
    
    Returns:
        dict: {'results': [...], 'stats': {'existing': int, 'new': int, 'total': int}}
    """
    # Generate neptune_ids for base entities
    reviewer_neptune_id = generate_neptune_id(reviewer_id, "REVIEWER")
    movie_neptune_id = generate_neptune_id(movie_id, "MOVIE")
    chunk_neptune_id = generate_neptune_id(chunk_id, "__Chunk__")
    
    base_query = """
    MERGE (r:REVIEWER {id: $reviewer_id})
    ON CREATE SET r.neptune_id = $reviewer_neptune_id
    MERGE (m:MOVIE {id: $movie_id})
    ON CREATE SET m.neptune_id = $movie_neptune_id
    MERGE (m)-[:HAS_CHUNK]->(c:__Chunk__ {id: $chunk_id})
    ON CREATE SET c.neptune_id = $chunk_neptune_id
    SET c.text = $text
    MERGE (c)-[:WRITTEN_BY]->(r)
    """
    execute_cypher(base_query, movie_id=movie_id, reviewer_id=reviewer_id, 
                   chunk_id=chunk_id, text=text,
                   reviewer_neptune_id=reviewer_neptune_id,
                   movie_neptune_id=movie_neptune_id,
                   chunk_neptune_id=chunk_neptune_id)
    
    # Group entities by (type, name) and accumulate descriptions
    entity_map = {}
    for entity in entities:
        entity_type = entity.get('entity_type', 'UNKNOWN')
        entity_name = entity.get('entity_name', '')
        entity_desc = entity.get('entity_description', '')
        
        key = (entity_type, entity_name)
        if key not in entity_map:
            entity_map[key] = []
        entity_map[key].append(entity_desc)
    
    results = []
    stats = {'existing': 0, 'new': 0, 'total': 0}
    
    # Create or update entities with accumulated descriptions and neptune_id
    for (entity_type, entity_name), new_descriptions in entity_map.items():
        # First, try to get existing entity (including neptune_id)
        find_query = f"""
        MATCH (n:{entity_type} {{name: $entity_name}})
        RETURN n.description AS description, n.neptune_id AS neptune_id
        """
        existing = execute_cypher(find_query, entity_name=entity_name)
        
        # Merge existing descriptions with new ones
        existing_desc = []
        existing_neptune_id = None
        is_existing = False
        
        if existing and 'results' in existing and existing['results']:
            is_existing = True
            stats['existing'] += 1
            for row in existing['results']:
                existing_neptune_id = row.get('neptune_id')
                if row.get('description'):
                    desc = row['description']
                    # Parse JSON string back to list
                    if isinstance(desc, str):
                        try:
                            existing_desc = json.loads(desc)
                        except:
                            existing_desc = [desc]
                    elif isinstance(desc, list):
                        existing_desc = desc
        else:
            stats['new'] += 1
        
        stats['total'] += 1
        
        # Combine and deduplicate
        all_descriptions = existing_desc + new_descriptions
        
        # Convert to JSON string for Neptune (doesn't support array properties)
        descriptions_str = json.dumps(all_descriptions, ensure_ascii=False)
        
        # Generate neptune_id if it doesn't exist
        if not existing_neptune_id:
            neptune_id = generate_neptune_id(entity_name, entity_type)
        else:
            neptune_id = existing_neptune_id
        
        # Create/update entity and link to chunk with neptune_id
        query = f"""
        MATCH (c:__Chunk__ {{id: $chunk_id}})
        MERGE (n:{entity_type} {{name: $entity_name}})
        SET n.description = $descriptions, n.neptune_id = $neptune_id
        MERGE (n)<-[:MENTIONS]-(c)
        """
        result = execute_cypher(query, chunk_id=chunk_id, 
                               entity_name=entity_name, descriptions=descriptions_str, 
                               neptune_id=neptune_id)
        results.append({'result': result, 'entity_name': entity_name, 'is_existing': is_existing})
    
    print("finish upload")
    return {'results': results, 'stats': stats}


def import_relationships_with_dynamic_label(relationships):
    """
    Import relationships ensuring only one relationship exists between any two entities.
    
    Returns:
        dict: {'results': [...], 'stats': {'existing': int, 'new': int, 'total': int}}
    """
    # Group relationships first to avoid processing same pair multiple times
    relationship_pairs = {}
    for rel in relationships:
        source_type = rel.get('source_type', '')
        target_type = rel.get('target_type', '')
        source_entity = rel.get('source_entity', '')
        target_entity = rel.get('target_entity', '')
        description = rel.get('relationship_description', '')
        strength = rel.get('relationship_strength', 0)
        
        try:
            strength = float(strength)
        except (ValueError, TypeError):
            strength = 0.0
        
        # Normalize pair (smaller name first)
        if source_entity < target_entity:
            key = (source_entity, target_entity, source_type, target_type)
        else:
            key = (target_entity, source_entity, target_type, source_type)
        
        if key not in relationship_pairs:
            relationship_pairs[key] = {'descriptions': [], 'strength': strength}
        relationship_pairs[key]['descriptions'].append(description)
        relationship_pairs[key]['strength'] = max(relationship_pairs[key]['strength'], strength)
    
    stats = {'existing': 0, 'new': 0, 'total': 0}
    results = []
    
    # Process each unique pair once
    for (entity1, entity2, type1, type2), data in relationship_pairs.items():
        new_descriptions = data['descriptions']
        strength = data['strength']
        
        # Get existing descriptions
        find_query = f"""
        MATCH (a)-[r:RELATIONSHIP]-(b)
        WHERE (a.name = $entity1 AND b.name = $entity2) OR (a.name = $entity2 AND b.name = $entity1)
        RETURN r.description AS description
        """
        existing = execute_cypher(find_query, entity1=entity1, entity2=entity2)
        
        all_descriptions = new_descriptions
        is_existing = False
        
        if existing and 'results' in existing and existing['results']:
            is_existing = True
            stats['existing'] += 1
            for row in existing['results']:
                if row.get('description'):
                    desc = row['description']
                    if isinstance(desc, str):
                        try:
                            existing_desc = json.loads(desc)
                            if isinstance(existing_desc, list):
                                all_descriptions.extend(existing_desc)
                            else:
                                all_descriptions.append(existing_desc)
                        except:
                            all_descriptions.append(desc)
        else:
            stats['new'] += 1
        
        stats['total'] += 1
        
        unique_descriptions = list(dict.fromkeys(all_descriptions))
        
        # Delete all existing relationships
        delete_query = f"""
        MATCH (a)-[r:RELATIONSHIP]-(b)
        WHERE (a.name = $entity1 AND b.name = $entity2) OR (a.name = $entity2 AND b.name = $entity1)
        DELETE r
        """
        execute_cypher(delete_query, entity1=entity1, entity2=entity2)
        
        # Create one relationship
        descriptions_str = json.dumps(unique_descriptions, ensure_ascii=False)
        
        create_query = f"""
        MATCH (s:{type1} {{name: $entity1}})
        MATCH (t:{type2} {{name: $entity2}})
        CREATE (s)-[r:RELATIONSHIP {{description: $descriptions, strength: $strength}}]->(t)
        """
        result = execute_cypher(create_query, entity1=entity1, entity2=entity2, 
                      descriptions=descriptions_str, strength=strength)
        results.append({'result': result, 'source': entity1, 'target': entity2, 'is_existing': is_existing})
    
    return {'results': results, 'stats': stats}


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


def get_all_nodes():
    """Get all nodes with their labels and properties."""
    query = """
    MATCH (n)
    RETURN labels(n) AS labels, properties(n) AS properties
    """
    return execute_cypher(query)


def get_all_nodes_by_label(label):
    """Get all nodes of a specific label."""
    query = f"""
    MATCH (n:{label})
    RETURN n
    """
    return execute_cypher(query)


def get_all_movies():
    """Get all MOVIE nodes."""
    query = """
    MATCH (m:MOVIE)
    RETURN m.id AS id, properties(m) AS properties
    """
    return execute_cypher(query)


def get_all_characters():
    """Get all MOVIE_CHARACTER nodes."""
    query = """
    MATCH (c:MOVIE_CHARACTER)
    RETURN c.name AS name, c.description AS description, c.summary AS summary, c.neptune_id AS neptune_id, labels(c) AS entity_type
    """
    return execute_cypher(query)


def get_all_relationships():
    """Get all relationships between entities (bidirectional)."""
    query = """
    MATCH (s)-[r:RELATIONSHIP]-(t)
    WHERE id(s) < id(t)
    RETURN labels(s) AS source_label, s.name AS source_name,
           labels(t) AS target_label, t.name AS target_name,
           r.description AS description, r.strength AS strength
    """
    return execute_cypher(query)


def get_entity_by_name(name):
    """Search entity by name across all labels."""
    query = """
    MATCH (n)
    WHERE n.name = $name
    RETURN labels(n) AS labels, properties(n) AS properties
    """
    return execute_cypher(query, name=name)


def get_movie_graph(movie_id):
    """Get all entities and relationships related to a movie."""
    query = """
    MATCH (m:MOVIE {id: $movie_id})-[:HAS_CHUNK]->(c:__Chunk__)-[:MENTIONS]->(e)
    RETURN DISTINCT labels(e) AS entity_type, e.name AS name, e.description AS description
    """
    return execute_cypher(query, movie_id=movie_id)


def count_nodes_by_label():
    """Count nodes grouped by label."""
    query = """
    MATCH (n)
    RETURN labels(n) AS label, count(n) AS count
    """
    return execute_cypher(query)


def count_relationships_by_type():
    """Count relationships by type."""
    query = """
    MATCH ()-[r]->()
    RETURN type(r) AS relationship_type, count(r) AS count
    ORDER BY count DESC
    """
    return execute_cypher(query)


# ============ Database Cleanup Queries ============

def delete_all_nodes_and_relationships():
    """Delete all nodes and relationships in the database. USE WITH CAUTION!"""
    query = """
    MATCH (n)
    DETACH DELETE n
    """
    return execute_cypher(query)


def delete_all_relationships():
    """Delete all relationships but keep nodes."""
    query = """
    MATCH ()-[r]->()
    DELETE r
    """
    return execute_cypher(query)


def delete_nodes_by_label(label):
    """Delete all nodes with a specific label and their relationships."""
    query = f"""
    MATCH (n:{label})
    DETACH DELETE n
    """
    return execute_cypher(query)


def clear_database():
    """Complete database cleanup - removes everything. USE WITH EXTREME CAUTION!"""
    print("WARNING: This will delete ALL data in the database!")
    return delete_all_nodes_and_relationships()


def get_database_stats():
    """Get basic statistics about the database before cleanup."""
    stats = {}
    
    # Count all nodes
    node_query = "MATCH (n) RETURN count(n) AS total_nodes"
    node_result = execute_cypher(node_query)
    stats['total_nodes'] = node_result.get('results', [{}])[0].get('total_nodes', 0) if node_result else 0
    
    # Count all relationships
    rel_query = "MATCH ()-[r]->() RETURN count(r) AS total_relationships"
    rel_result = execute_cypher(rel_query)
    stats['total_relationships'] = rel_result.get('results', [{}])[0].get('total_relationships', 0) if rel_result else 0
    
    # Count by labels
    label_query = "MATCH (n) RETURN labels(n) AS label, count(n) AS count"
    label_result = execute_cypher(label_query)
    stats['nodes_by_label'] = label_result.get('results', []) if label_result else []
    
    # Count by relationship types
    rel_type_query = "MATCH ()-[r]->() RETURN type(r) AS relationship_type, count(r) AS count"
    rel_type_result = execute_cypher(rel_type_query)
    stats['relationships_by_type'] = rel_type_result.get('results', []) if rel_type_result else []
    
    return stats
