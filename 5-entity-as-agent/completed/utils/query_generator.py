"""
Cypher 쿼리 생성 유틸리티
"""
from utils.schema import get_graph_schema
from datetime import datetime


def load_prompt_template(prompt_file: str) -> str:
    """프롬프트 템플릿 파일 로드"""
    with open(prompt_file, 'r', encoding='utf-8') as f:
        return f.read()


def generate_cypher_prompt(user_question: str) -> str:
    """
    사용자 질문에 대한 Cypher 쿼리 생성 프롬프트를 만듭니다.
    """
    graph_schema = get_graph_schema()
    prompt_template = load_prompt_template('prompts/query_to_cyper.md')
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    prompt = prompt_template.format(
        CURRENT_TIME=current_time,
        GRAPH_SCHEMA=graph_schema,
        USER_QUESTION=user_question
    )
    
    return prompt


if __name__ == "__main__":
    question = "코브와 멜은 어떤 관계야?"
    prompt = generate_cypher_prompt(question)
    print("Generated Prompt:")
    print("=" * 80)
    print(prompt)
