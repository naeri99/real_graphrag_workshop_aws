"""
Neptune 그래프 스키마 유틸리티
"""
from neptune.cyper_queries import execute_cypher
from typing import Dict, Any


def get_graph_schema() -> str:
    """
    Neptune 그래프 스키마를 생성합니다.
    프롬프트에서 사용할 수 있는 형식으로 반환합니다.
    """
    try:
        # 모든 노드 라벨 조회
        labels_query = "MATCH (n) RETURN DISTINCT labels(n) AS labels"
        labels_result = execute_cypher(labels_query)
        
        # 모든 관계 타입 조회
        relationships_query = "MATCH ()-[r]-() RETURN DISTINCT type(r) AS relationship_type"
        relationships_result = execute_cypher(relationships_query)
        
        # 라벨 처리
        unique_labels = set()
        if labels_result and 'results' in labels_result:
            for record in labels_result['results']:
                if record.get('labels'):
                    for label in record['labels']:
                        if label != 'None':
                            unique_labels.add(label)
        
        # 각 라벨의 속성 조회
        label_properties = {}
        for label in unique_labels:
            props_query = f"MATCH (n:{label}) RETURN DISTINCT keys(n) AS properties LIMIT 1"
            props_result = execute_cypher(props_query)
            if props_result and 'results' in props_result and props_result['results']:
                properties = props_result['results'][0].get('properties', [])
                label_properties[label] = properties
            else:
                label_properties[label] = []
        
        # 스키마 텍스트 생성
        schema_text = "## Graph Schema\n\n"
        
        # 노드 라벨과 속성
        schema_text += "### Node Labels and Properties:\n"
        for label in sorted(unique_labels):
            properties = label_properties.get(label, [])
            if properties:
                props_str = ", ".join(sorted(properties))
                schema_text += f"- **{label}**: {props_str}\n"
            else:
                schema_text += f"- **{label}**: (no properties found)\n"
        
        # 관계 타입
        schema_text += "\n### Relationship Types:\n"
        relationship_types = []
        if relationships_result and 'results' in relationships_result:
            relationship_types = [record['relationship_type'] for record in relationships_result['results']]
        
        for rel_type in sorted(set(relationship_types)):
            schema_text += f"- **{rel_type}**\n"
        
        return schema_text
        
    except Exception as e:
        return f"Error generating schema: {str(e)}"


def get_schema_summary() -> Dict[str, Any]:
    """그래프 스키마 요약 정보를 반환합니다."""
    try:
        labels_query = "MATCH (n) RETURN DISTINCT labels(n) AS labels"
        labels_result = execute_cypher(labels_query)
        
        relationships_query = "MATCH ()-[r]-() RETURN DISTINCT type(r) AS relationship_type"
        relationships_result = execute_cypher(relationships_query)
        
        unique_labels = set()
        if labels_result and 'results' in labels_result:
            for record in labels_result['results']:
                if record.get('labels'):
                    for label in record['labels']:
                        if label != 'None':
                            unique_labels.add(label)
        
        relationship_types = []
        if relationships_result and 'results' in relationships_result:
            relationship_types = [record['relationship_type'] for record in relationships_result['results']]
        
        return {
            "node_labels": list(unique_labels),
            "relationship_types": list(set(relationship_types)),
            "total_labels": len(unique_labels),
            "total_relationship_types": len(set(relationship_types))
        }
        
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    print("=== Graph Schema ===")
    print(get_graph_schema())
