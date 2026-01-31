"""
Neptune Graph 검색 및 통계 확인
"""
from neptune.cyper_queries import (
    count_nodes_by_label,
    count_relationships_by_type,
    find_duplicate_relationships,
    get_database_stats
)
import json


def print_result(title, result):
    """Pretty print search results."""
    print(f"\n{'='*50}")
    print(f" {title}")
    print('='*50)
    if result and 'results' in result:
        for item in result['results']:
            print(json.dumps(item, ensure_ascii=False, indent=2))
    else:
        print("No results found")


def show_statistics():
    """Show node count statistics."""
    result = count_nodes_by_label()
    print_result("Node Statistics", result)
    return result


def show_relationship_statistics():
    """Show relationship count statistics."""
    result = count_relationships_by_type()
    print_result("Relationship Statistics", result)
    return result


def find_duplicates():
    """Find duplicate relationships."""
    result = find_duplicate_relationships()
    print_result("Duplicate Relationships", result)
    return result


if __name__ == "__main__":
    print("\n" + "="*60)
    print(" Neptune Graph Search")
    print("="*60)
    
    # Show statistics
    show_statistics()
    show_relationship_statistics()
    
    # Find duplicates
    find_duplicates()
