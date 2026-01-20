---
CURRENT_TIME: {CURRENT_TIME}
---

## Goal
Given a text document that is potentially relevant to this activity and a list of entity types, identify all entities of those types from the text and all relationships among the identified entities.

## Entity Type Definitions

### ACTOR (배우/연기자)
- 영화에서 **연기를 하는 사람**
- 예: 레오나르도 디카프리오, 톰 하디, 마리옹 꼬띠아르, 김윤석, 하정우

### MOVIE_STAFF (제작진)
- 영화 제작에 참여하지만 **연기를 하지 않는 사람**
- 감독 (Director): 크리스토퍼 놀란, 봉준호, 최동훈
- 음악/작곡가 (Composer): 한스 짐머
- 촬영감독, 각본가, 프로듀서 등

### MOVIE_CHARACTER (영화 캐릭터)
- 영화 속 등장인물
- 예: 코브, 맬, 아서, 기택, 기우

### MOVIE (영화)
- 영화 작품 자체
- 예: 인셉션, 기생충, 암살, 도둑들, 외계+인

### REVIEWER (리뷰어)
- 영화를 평가하는 사람
- 영화 평론가, 유튜버, 블로거 등

## Steps
1. Identify all entities. For each identified entity, extract the following information:
- entity_name: Name of the entity, capitalized
- entity_type: One of the following types: [MOVIE, ACTOR, MOVIE_STAFF, MOVIE_CHARACTER, REVIEWER]
- entity_description: Comprehensive description of the entity's attributes and activities

Format each entity as ("entity"|<entity_name>|<entity_type>|<entity_description>)

2. From the entities identified in step 1, identify all pairs of (source_entity, target_entity) that are *clearly related* to each other.
For each pair of related entities, extract the following information:
- source_entity: name of the source entity, as identified in step 1
- source_type: entity type of the source entity
- target_entity: name of the target entity, as identified in step 1
- target_type: entity type of the target entity
- relationship_description: explanation as to why you think the source entity and the target entity are related to each other
- relationship_strength: a numeric score indicating strength of the relationship between the source entity and target entity

Format each relationship as ("relationship"|<source_entity>|<source_type>|<target_entity>|<target_type>|<relationship_description>|<relationship_strength>)

3. Return output as a single list of all the entities and relationships identified in steps 1 and 2. Use **##** as the list delimiter.

4. When finished, output <END>

######################
-Examples-
######################
Example 1:

Text:
"Interstellar" (2014) was directed by Christopher Nolan. Cooper is a former NASA pilot who leaves his daughter Murph behind to travel through a wormhole. Film critic John Smith notes that "the film brilliantly explores how love transcends dimensions."

Output:
("entity"|INTERSTELLAR|MOVIE|Interstellar is a 2014 science fiction film about humanity's search for a new home through a wormhole)##
("entity"|CHRISTOPHER NOLAN|MOVIE_STAFF|Christopher Nolan is the director of Interstellar)##
("entity"|COOPER|MOVIE_CHARACTER|Cooper is a former NASA pilot who leaves Earth to find a new habitable planet)##
("entity"|MURPH|MOVIE_CHARACTER|Murph is Cooper's daughter)##
("entity"|JOHN SMITH|REVIEWER|John Smith is a film critic who reviewed Interstellar)##
("relationship"|CHRISTOPHER NOLAN|MOVIE_STAFF|INTERSTELLAR|MOVIE|Christopher Nolan directed Interstellar|10)##
("relationship"|COOPER|MOVIE_CHARACTER|INTERSTELLAR|MOVIE|Cooper is the protagonist of Interstellar|10)##
("relationship"|COOPER|MOVIE_CHARACTER|MURPH|MOVIE_CHARACTER|Cooper is Murph's father|10)##
("relationship"|JOHN SMITH|REVIEWER|INTERSTELLAR|MOVIE|John Smith reviewed Interstellar|6)<END>

######################
Example 2:

Text:
기생충에서 김기택은 박사장 집에 잠입하여 기사로 일하게 된다. 송강호의 연기가 인상적이었다는 평가를 받았다.

Output:
("entity"|기생충|MOVIE|기생충은 계층 갈등을 다룬 영화이다)##
("entity"|김기택|MOVIE_CHARACTER|김기택은 박사장 집에 잠입하여 기사로 일하는 인물이다)##
("entity"|박사장|MOVIE_CHARACTER|박사장은 부유한 가정의 가장이다)##
("entity"|송강호|ACTOR|송강호는 김기택 역을 맡은 배우로 인상적인 연기를 보여주었다)##
("relationship"|김기택|MOVIE_CHARACTER|기생충|MOVIE|김기택은 기생충의 주인공이다|10)##
("relationship"|김기택|MOVIE_CHARACTER|박사장|MOVIE_CHARACTER|김기택은 박사장 집에서 기사로 일한다|8)##
("relationship"|송강호|ACTOR|김기택|MOVIE_CHARACTER|송강호는 김기택 역을 연기했다|10)<END>

######################
## Extraction Rules

### ACTOR vs MOVIE_STAFF 구분
- **ACTOR (배우)**: 영화에서 연기를 하는 사람
  - 연기력 평가, 캐릭터 연기 언급 시 ACTOR로 분류
  
- **MOVIE_STAFF (제작진)**: 연기를 하지 않는 제작 참여자
  - 감독, 음악/작곡가, 촬영감독, 각본가, 프로듀서 등

### CRITICAL RULE: Story Context = Character, NOT Actor
- 영화 스토리/줄거리를 설명할 때 배우 이름이 언급되더라도 -> MOVIE_CHARACTER로 처리
- 예: "송강호가 박사장을 찔렀다" -> 김기택(캐릭터)의 행동으로 해석
- 배우의 연기력/수상 등 언급이 있을 때만 ACTOR entity 생성

### Language Rules
- **entity_description**: Write in Korean (한국어)
- **relationship_description**: Write in Korean (한국어)

######################
Output:
