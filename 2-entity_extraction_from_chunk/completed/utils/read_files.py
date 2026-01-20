"""
파일 읽기 유틸리티
"""
import json
from typing import List, Dict, Any


def read_file_list(list_file_path: str) -> List[str]:
    """list.txt에서 파일 경로 목록 읽기"""
    with open(list_file_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def load_json_from_list(list_file_path: str) -> List[Dict[str, Any]]:
    """list.txt에 있는 모든 json 파일을 읽어서 리스트로 반환"""
    file_paths = read_file_list(list_file_path)
    
    results = []
    for file_path in file_paths:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                results.append({
                    "file_path": file_path,
                    "data": data.get("transcript", "")
                })
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
    
    return results


def load_json_files(list_file_path: str) -> Dict[str, Any]:
    """list.txt에 있는 json 파일들을 파일명을 키로 하는 딕셔너리로 반환"""
    file_paths = read_file_list(list_file_path)
    
    results = {}
    for file_path in file_paths:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 파일명만 추출해서 키로 사용
                file_name = file_path.split('/')[-1].replace('.json', '')
                results[file_name] = data
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
    
    return results


def load_single_json(file_path: str) -> Dict[str, Any]:
    """단일 JSON 파일 로드"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return {}
