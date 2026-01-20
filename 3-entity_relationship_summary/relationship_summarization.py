"""
2단계: Relationship Summarization
- Neptune에서 요약이 필요한 관계 조회
- LLM으로 description들을 요약
- Neptune에 relationship summary 저장
"""
from datetime import datetime
from utils.generate_entity import get_bedrock_agent
from utils.parse_utils import parse_summary_output
from neptune.cyper_queries import (
    get_all_relationships_for_summary,
    save_relationship_summary
)


def load_summarize_prompt():
    """Load the summarization prompt from file"""
    with open('./prompts/summarization.md', 'r', encoding='utf-8') as f:
        return f.read()


def run_relationship_summarization():
    """
    Relationship Summarization 실행
    1. Neptune에서 요약이 필요한 관계 조회
    2. LLM으로 description 요약
    3. Neptune에 relationship summary 저장
    """
    print("=" * 60)
    print("🚀 Relationship Summarization Start")
    print("=" * 60)
    
    # Bedrock Agent 초기화
    agent = get_bedrock_agent()
    
    # Neptune에서 요약이 필요한 관계 조회
    results = get_all_relationships_for_summary()
    
    if not results or 'results' not in results or not results['results']:
        print("⚠️ 요약이 필요한 관계가 없습니다.")
        return
    
    relationships = results['results']
    total = len(relationships)
    print(f"📋 요약이 필요한 관계: {total}개")
    
    success_count = 0
    fail_count = 0
    
    for i, rel in enumerate(relationships, 1):
        source = rel.get("source", "")
        target = rel.get("target", "")
        source_type_list = rel.get("source_type", [])
        target_type_list = rel.get("target_type", [])
        source_type = source_type_list[0] if source_type_list else "UNKNOWN"
        target_type = target_type_list[0] if target_type_list else "UNKNOWN"
        
        print(f"\n[{i}/{total}] 🔗 {source} ({source_type}) → {target} ({target_type})")
        
        # description_list 가져오기
        description_list = rel.get("description_list", [])
        if not description_list:
            print("   ⚠️ description이 없습니다. 건너뜀.")
            continue
        
        # 프롬프트 생성
        prompt_template = load_summarize_prompt()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_prompt = prompt_template.format(
            CURRENT_TIME=current_time,
            ENTITY_NAME=f"{source} - {target}",
            DESCRIPTION_LIST=",".join(description_list)
        )
        
        # LLM 호출
        try:
            response = agent(formatted_prompt)
            parsed = parse_summary_output(response)
            
            if not parsed:
                print("   ❌ 파싱 실패")
                fail_count += 1
                continue
            
            summary = parsed.get("summary")
            if not summary:
                print("   ❌ summary가 없습니다")
                fail_count += 1
                continue
            
            # Neptune에 relationship summary 저장
            result = save_relationship_summary(source, target, summary, source_type, target_type)
            print(f"   ✅ 저장 완료")
            success_count += 1
            
        except Exception as e:
            print(f"   ❌ 오류: {e}")
            fail_count += 1
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("🎉 Relationship Summarization Complete!")
    print("=" * 60)
    print(f"✅ 성공: {success_count}개")
    print(f"❌ 실패: {fail_count}개")
    print(f"📊 총 처리: {total}개")
    
    return {"success": success_count, "failed": fail_count, "total": total}


if __name__ == "__main__":
    run_relationship_summarization()
