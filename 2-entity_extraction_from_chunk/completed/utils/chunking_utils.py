"""
문서 청킹 유틸리티
"""
from typing import List, Optional


def chunk_text(
    text: str,
    chunk_size: int = 1500,
    overlap: int = 100
) -> List[str]:
    """
    텍스트를 지정된 크기로 청킹합니다.
    
    Args:
        text: 청킹할 원본 텍스트
        chunk_size: 각 청크의 최대 문자 수 (기본값: 1500)
        overlap: 청크 간 겹치는 문자 수 (기본값: 100)
    
    Returns:
        청크 리스트
    """
    if not text or not text.strip():
        return []
    
    chunks = []
    start = 0
    text_length = len(text)
    
    while start < text_length:
        end = start + chunk_size
        
        # 마지막 청크가 아닌 경우, 문장/단어 경계에서 자르기 시도
        if end < text_length:
            # 문장 끝 찾기 (마침표, 느낌표, 물음표)
            for sep in ['. ', '! ', '? ', '.\n', '!\n', '?\n', '\n\n', '\n']:
                last_sep = text.rfind(sep, start, end)
                if last_sep > start:
                    end = last_sep + len(sep)
                    break
        
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        
        # 다음 시작 위치 (overlap 적용)
        start = end - overlap if end < text_length else text_length
    
    return chunks


def chunk_document(
    document: str,
    chunk_size: int = 1500,
    overlap: int = 100,
    metadata: Optional[dict] = None
) -> List[dict]:
    """
    문서를 청킹하고 메타데이터와 함께 반환합니다.
    
    Args:
        document: 청킹할 문서 텍스트
        chunk_size: 각 청크의 최대 문자 수 (기본값: 1500)
        overlap: 청크 간 겹치는 문자 수 (기본값: 100)
        metadata: 각 청크에 추가할 메타데이터
    
    Returns:
        청크 정보를 담은 딕셔너리 리스트
    """
    chunks = chunk_text(document, chunk_size, overlap)
    
    result = []
    for idx, chunk in enumerate(chunks):
        chunk_data = {
            "chunk_id": idx,
            "content": chunk,
            "char_count": len(chunk),
            "total_chunks": len(chunks)
        }
        if metadata:
            chunk_data["metadata"] = metadata
        result.append(chunk_data)
    
    return result
