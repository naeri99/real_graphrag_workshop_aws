"""
1단계: Entity Summarization
- Neptune에서 요약이 필요한 엔티티 조회
- LLM으로 description들을 요약
- Neptune에 summary 저장
"""
import json
import re
import uuid
from datetime import datetime
from strands import Agent
from strands.models import BedrockModel
from utils.parse_utils import parse_summary_output
from neptune.cyper_queries import (
    get_all_entities_for_summary,
    save_entity_summary,
    execute_cypher
)


def generate_neptune_id(name, entity_type):
    """Neptune ID 생성: 이름_엔티티타입_UUID"""
    clean_name = re.sub(r'[^\w가-힣]', '_', name)
    clean_name = re.sub(r'_+', '_', clean_name)
    clean_name = clean_name.strip('_')
    unique_id = str(uuid.uuid4())[:8]
    return f"{clean_name}_{entity_type}_{unique_id}"


def update_entity_neptune_id(entity_name, entity_type, neptune_id=None):
    """Neptune에서 엔티티에 neptune_id 속성 추가 (기존 ID가 있으면 유지)"""
    check_query = f"""
    MATCH (n:{entity_type} {{name: $entity_name}})
    RETURN n.name AS name, n.neptune_id AS existing_neptune_id
    """
    check_result = execute_cypher(check_query, entity_name=entity_name)
    
    if check_result and 'results' in check_result and check_result['results']:
        existing_id = check_result['results'][0].get('existing_neptune_id')
        if existing_id:
            return {"existing_id": existing_id, "created_new": False}
    
    if not neptune_id:
        neptune_id = generate_neptune_id(entity_name, entity_type)
    
    update_query = f"""
    MATCH (n:{entity_type} {{name: $entity_name}})
    SET n.neptune_id = $neptune_id
    RETURN n.name AS name, n.neptune_id AS neptune_id
    """
    result = execute_cypher(update_query, entity_name=entity_name, neptune_id=neptune_id)
    return {"neptune_id": neptune_id, "created_new": True, "result": result}


def load_summarize_prompt():
    """Load the summarization prompt from file"""
    with open('./prompts/summarization.md', 'r', encoding='utf-8') as f:
        return f.read()


def run_entity_summarization():
    """
    Entity Summarization 실행
    1. Neptune에서 요약이 필요한 엔티티 조회
    2. LLM으로 description 요약
    3. Neptune에 summary 저장
    """
    print("=" * 60)
    print("🚀 Entity Summarization Start")
    print("=" * 60)
    
    # Bedrock Agent 초기화
    bedrock_model = BedrockModel(
        model_id="apac.anthropic.claude-sonnet-4-20250514-v1:0",
        region_name="ap-northeast-2",
        temperature=0.1,
    )
    agent = Agent(model=bedrock_model)
    
    # Neptune에서 요약이 필요한 엔티티 조회
    results = get_all_entities_for_summary()
    
    if not results or 'results' not in results or not results['results']:
        print("⚠️ 요약이 필요한 엔티티가 없습니다.")
        return
    
    entities = results['results']
    total = len(entities)
    print(f"📋 요약이 필요한 엔티티: {total}개")
    
    success_count = 0
    fail_count = 0
    
    for i, entity in enumerate(entities, 1):
        entity_name = entity.get("name", "")
        entity_type_list = entity.get("entity_type", [])
        entity_type = entity_type_list[0] if entity_type_list else "UNKNOWN"
        
        print(f"\n[{i}/{total}] 📝 {entity_name} ({entity_type})")
        
        # description 파싱
        description_list = entity.get("description", [])
        if isinstance(description_list, str):
            try:
                description_list = json.loads(description_list)
            except:
                description_list = [description_list]
        
        if not description_list:
            print("   ⚠️ description이 없습니다. 건너뜀.")
            continue
        
        # 프롬프트 생성
        prompt_template = load_summarize_prompt()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_prompt = prompt_template.format(
            CURRENT_TIME=current_time,
            ENTITY_NAME=entity_name,
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
            
            # Neptune에 summary 저장
            save_entity_summary(entity_name, summary, entity_type)
            
            # Neptune ID 업데이트
            id_result = update_entity_neptune_id(entity_name, entity_type)
            
            if id_result.get("created_new"):
                print(f"   ✅ 저장 완료 (새 Neptune ID: {id_result.get('neptune_id')})")
            else:
                print(f"   ✅ 저장 완료 (기존 Neptune ID: {id_result.get('existing_id')})")
            
            success_count += 1
            
        except Exception as e:
            print(f"   ❌ 오류: {e}")
            fail_count += 1
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("🎉 Entity Summarization Complete!")
    print("=" * 60)
    print(f"✅ 성공: {success_count}개")
    print(f"❌ 실패: {fail_count}개")
    print(f"📊 총 처리: {total}개")
    
    return {"success": success_count, "failed": fail_count, "total": total}


if __name__ == "__main__":
    run_entity_summarization()
