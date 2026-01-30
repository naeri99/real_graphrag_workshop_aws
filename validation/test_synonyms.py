"""
동의어 설정 테스트 스크립트
"""
import sys
import os
import json

# 상위 경로 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from opensearch.opensearh_search import (
    get_synonyms,
    analyze_text_with_synonyms,
    test_synonym_expansion
)


def print_separator(title):
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def test_get_synonyms(index_name="entities"):
    """동의어 설정 확인 테스트"""
    print_separator("동의어 설정 확인")
    
    synonym_info = get_synonyms(index_name)
    
    if synonym_info:
        print(f"인덱스: {synonym_info['index_name']}")
        
        print("\n📌 동의어 필터:")
        if synonym_info['filters']:
            for name, config in synonym_info['filters'].items():
                print(f"  - {name}:")
                print(f"    타입: {config.get('type')}")
                synonyms = config.get('synonyms', [])
                if synonyms:
                    print(f"    동의어 (총 {len(synonyms)}개):")
                    for syn in synonyms:  # 전체 출력
                        print(f"      • {syn}")
        else:
            print("  동의어 필터가 없습니다.")
        
        print("\n📌 분석기:")
        for name, config in synonym_info['analyzers'].items():
            print(f"  - {name}:")
            print(f"    토크나이저: {config.get('tokenizer', 'N/A')}")
            print(f"    필터: {config.get('filter', [])}")
    else:
        print("❌ 동의어 설정을 가져올 수 없습니다.")


def test_analyze_text(index_name="entities"):
    """텍스트 분석 테스트"""
    print_separator("텍스트 분석 테스트")
    
    test_texts = [
        "무륵"
    ]
    
    for text in test_texts:
        print(f"\n🔍 '{text}' 분석 결과:")
        tokens = analyze_text_with_synonyms(text, index_name)
        
        if tokens:
            for token in tokens:
                print(f"  - {token['token']} (타입: {token['type']}, 위치: {token['position']})")
        else:
            print("  토큰 없음")


def test_synonym_expansion_comparison(index_name="entities"):
    """동의어 확장 비교 테스트"""
    print_separator("동의어 확장 비교")
    
    test_texts = ["무륵"]
    
    for text in test_texts:
        print(f"\n🔍 '{text}' 분석기별 결과:")
        results = test_synonym_expansion(text, index_name)
        
        for analyzer, tokens in results.items():
            print(f"  [{analyzer}]: {', '.join(tokens)}")


def main():
    """메인 테스트 함수"""
    # 기본 인덱스 이름 (필요시 변경)
    index_name=  "entities"
    try:
        # 1. 동의어 설정 확인
        test_get_synonyms(index_name)
        
        # 2. 텍스트 분석 테스트
        test_analyze_text(index_name)
        
        # 3. 동의어 확장 비교
        test_synonym_expansion_comparison(index_name)
        
        print("\n" + "=" * 60)
        print(" ✅ 테스트 완료")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
