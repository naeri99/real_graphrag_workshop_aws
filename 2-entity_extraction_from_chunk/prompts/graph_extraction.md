---
CURRENT_TIME: {CURRENT_TIME}
---

## Goal
Given a text document that is potentially relevant to this activity and a list of entity types, identify all entities of those types from the text and all relationships among the identified entities.

## Entity Type Definitions

### ACTOR (배우/연기자)
- 영화에서 **연기를 하는 사람**
- 예: 레오나르도 디카프리오, 톰 하디, 마리옹 꼬띠아르, 엘렌 페이지

### MOVIE_STAFF (제작진)
- 영화 제작에 참여하지만 **연기를 하지 않는 사람**
- 감독 (Director): 크리스토퍼 놀란, 봉준호
- 음악/작곡가 (Composer): 한스 짐머
- 촬영감독 (Cinematographer)
- 각본가 (Screenwriter)
- 프로듀서 (Producer)
- 기타 제작진

### MOVIE_CHARACTER (영화 캐릭터)
- 영화 속 등장인물
- 예: 코브, 맬, 아서, 아리아드네, 피셔

### MOVIE (영화)
- 영화 작품 자체
- 예: 인셉션, 인터스텔라, 기생충

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
- source_type: entity type of the source entity (MOVIE, ACTOR, MOVIE_STAFF, MOVIE_CHARACTER, or REVIEWER)
- target_entity: name of the target entity, as identified in step 1
- target_type: entity type of the target entity (MOVIE, ACTOR, MOVIE_STAFF, MOVIE_CHARACTER, or REVIEWER)
- relationship_description: explanation as to why you think the source entity and the target entity are related to each other
- relationship_strength: a numeric score indicating strength of the relationship between the source entity and target entity

Format each relationship as ("relationship"|<source_entity>|<source_type>|<target_entity>|<target_type>|<relationship_description>|<relationship_strength>)

3. Return output in English as a single list of all the entities and relationships identified in steps 1 and 2. Use **##** as the list delimiter.

4. When finished, output <END>

######################
-Examples-
######################
Example 1:

Entity_types: MOVIE,ACTOR,MOVIE_STAFF,MOVIE_CHARACTER,REVIEWER

Text:
"Interstellar" (2014) was directed by Christopher Nolan. Cooper is a former NASA pilot who leaves his daughter Murph behind to travel through a wormhole in search of a new habitable planet. Cooper makes the heartbreaking decision to sacrifice decades of time to save humanity, entering a black hole where he discovers a tesseract that allows him to communicate with Murph across time. Dr. Amelia Brand advocates for following her heart to Edmund's planet and ultimately establishes humanity's new colony. Professor Brand lies about Plan A being possible to motivate the mission. Film critic John Smith notes that "the film brilliantly explores how love transcends dimensions, making it more than just a sci-fi spectacle."

######################
Output:
("entity"|INTERSTELLAR|MOVIE|Interstellar is a 2014 science fiction film about humanity's search for a new home through a wormhole)##
("entity"|CHRISTOPHER NOLAN|MOVIE_STAFF|Christopher Nolan is the director of Interstellar known for exploring themes of time, space, and human nature)##
("entity"|COOPER|MOVIE_CHARACTER|Cooper is a former NASA pilot who leaves Earth to find a new habitable planet, sacrifices decades by entering a black hole, and communicates with his daughter through a tesseract)##
("entity"|MURPH|MOVIE_CHARACTER|Murph is Cooper's daughter who receives messages from her father across time and space)##
("entity"|DR. AMELIA BRAND|MOVIE_CHARACTER|Dr. Amelia Brand advocates following love to Edmund's planet and establishes humanity's new colony)##
("entity"|PROFESSOR BRAND|MOVIE_CHARACTER|Professor Brand lies about Plan A being possible to motivate Cooper and the crew to embark on the mission)##
("entity"|JOHN SMITH|REVIEWER|John Smith is a film critic who reviewed Interstellar)##
("relationship"|CHRISTOPHER NOLAN|MOVIE_STAFF|INTERSTELLAR|MOVIE|Christopher Nolan directed Interstellar|10)##
("relationship"|COOPER|MOVIE_CHARACTER|INTERSTELLAR|MOVIE|Cooper is the protagonist of Interstellar|10)##
("relationship"|COOPER|MOVIE_CHARACTER|MURPH|MOVIE_CHARACTER|Cooper is Murph's father who leaves her to save humanity but communicates with her across time through the tesseract|10)##
("relationship"|PROFESSOR BRAND|MOVIE_CHARACTER|COOPER|MOVIE_CHARACTER|Professor Brand deceives Cooper about Plan A to ensure he joins the mission|8)##
("relationship"|PROFESSOR BRAND|MOVIE_CHARACTER|DR. AMELIA BRAND|MOVIE_CHARACTER|Professor Brand is Dr. Amelia Brand's father|9)##
("relationship"|DR. AMELIA BRAND|MOVIE_CHARACTER|COOPER|MOVIE_CHARACTER|Dr. Amelia Brand and Cooper are fellow astronauts who debate between logic and love in choosing which planet to visit|7)##
("relationship"|JOHN SMITH|REVIEWER|INTERSTELLAR|MOVIE|John Smith reviewed Interstellar, noting that love transcends dimensions in the film|6)##
("relationship"|JOHN SMITH|REVIEWER|INTERSTELLAR|MOVIE|The film brilliantly explores how love transcends dimensions, making it more than just a sci-fi spectacle|POSITIVE)<END>

######################
Example 2:

Entity_types: MOVIE,ACTOR,MOVIE_STAFF,MOVIE_CHARACTER,REVIEWER

Text:
In "Parasite" (2019), directed by Bong Joon-ho, Kim Ki-taek orchestrates his family's infiltration into the wealthy Park household by forging documents and manipulating situations. Ki-woo initiates the scheme by becoming a tutor for the Park family's daughter after receiving a scholar's rock from his friend. The tension escalates when Ki-taek ultimately stabs Park Dong-ik during the garden party after witnessing his disgust at the smell of poverty. Korean film reviewer Park Min-young observes that "Bong Joon-ho masterfully uses vertical space to represent class hierarchy, with the Parks literally living above the Kims."

######################
Output:
("entity"|PARASITE|MOVIE|Parasite is a 2019 thriller about class conflict where a poor family infiltrates a wealthy household)##
("entity"|BONG JOON-HO|MOVIE_STAFF|Bong Joon-ho is the director of Parasite)##
("entity"|KIM KI-TAEK|MOVIE_CHARACTER|Kim Ki-taek orchestrates his family's infiltration into the Park household by forging documents, and ultimately stabs Park Dong-ik at the garden party)##
("entity"|KIM KI-WOO|MOVIE_CHARACTER|Kim Ki-woo initiates the infiltration scheme by becoming a tutor after receiving a scholar's rock from his friend)##
("entity"|PARK DONG-IK|MOVIE_CHARACTER|Park Dong-ik is the wealthy patriarch who shows disgust at the smell of poverty, leading to his death)##
("entity"|PARK MIN-YOUNG|REVIEWER|Park Min-young is a Korean film reviewer who analyzed Parasite)##
("relationship"|BONG JOON-HO|MOVIE_STAFF|PARASITE|MOVIE|Bong Joon-ho directed Parasite|10)##
("relationship"|KIM KI-TAEK|MOVIE_CHARACTER|KIM KI-WOO|MOVIE_CHARACTER|Kim Ki-taek is the father of Kim Ki-woo and leads the family's infiltration plan|9)##
("relationship"|KIM KI-TAEK|MOVIE_CHARACTER|PARK DONG-IK|MOVIE_CHARACTER|Kim Ki-taek stabs Park Dong-ik at the garden party after witnessing his disgust at poverty|10)##
("relationship"|KIM KI-WOO|MOVIE_CHARACTER|PARK DONG-IK|MOVIE_CHARACTER|Kim Ki-woo infiltrates Park Dong-ik's household as a tutor|7)##
("relationship"|PARK MIN-YOUNG|REVIEWER|PARASITE|MOVIE|Park Min-young reviewed Parasite, analyzing its use of vertical space as class metaphor|6)##
("relationship"|PARK MIN-YOUNG|REVIEWER|PARASITE|MOVIE|Bong Joon-ho masterfully uses vertical space to represent class hierarchy, with the Parks literally living above the Kims|POSITIVE)<END>

######################
-Real Data-
######################
Entity_types: MOVIE,ACTOR,MOVIE_STAFF,MOVIE_CHARACTER,REVIEWER

## MOVIE_CONTEXT (Entity List Format)
The MOVIE_CONTEXT contains pre-defined entity mappings in the format:
```
배우이름 - 캐릭터이름
```

**Reference Data:**
{MOVIE_CONTEXT}

## Extraction Rules (CHARACTER-FOCUSED)

### Priority 1: MOVIE_CHARACTER (Always Extract)
- **ALWAYS** create MOVIE_CHARACTER entities for ALL characters in the Entity List
- Left side of `-` = Character name -> **MUST** create as **MOVIE_CHARACTER** entity
- Focus on character actions, relationships, and story roles

### CRITICAL RULE: Story Context = Character, NOT Actor
- **영화 스토리/줄거리를 설명할 때 배우 이름이 언급되더라도 -> 반드시 MOVIE_CHARACTER로 처리**
- 스토리 맥락에서 배우 이름이 나오면, Entity List를 참조하여 해당 캐릭터 이름으로 변환
- 예시:
  - "에단호크가 시간여행을 한다" (스토리 맥락) -> 도우(캐릭터)의 행동으로 해석
  - "송강호가 박사장을 찔렀다" (스토리 맥락) -> 김기택(캐릭터)의 행동으로 해석

### Priority 2: ACTOR vs MOVIE_STAFF 구분
- **ACTOR (배우)**: 영화에서 연기를 하는 사람
  - 톰 하디, 레오나르도 디카프리오, 마리옹 꼬띠아르 등
  - 연기력 평가, 캐릭터 연기 언급 시 ACTOR로 분류
  
- **MOVIE_STAFF (제작진)**: 연기를 하지 않는 제작 참여자
  - 감독: 크리스토퍼 놀란, 봉준호
  - 음악/작곡가: 한스 짐머
  - 촬영감독, 각본가, 프로듀서 등

- **ONLY** create ACTOR/MOVIE_STAFF entity when:
  - The text explicitly discusses the person **as a real person** (연기력 평가, 수상, 필모그래피 등)
  - A review mentions the actor's performance quality
  - The person is referenced **completely outside of movie story context**
- **DO NOT** create ACTOR/MOVIE_STAFF when actor name is used to describe story/plot events

### Priority 3: REVIEW -> CHARACTER Relationship
- **리뷰가 특정 캐릭터를 언급하면, 중요하다고 판단될때 REVIEWER와 MOVIE_CHARACTER 간의 relationship 생성**
- 리뷰어가 캐릭터의 행동, 성격, 스토리 역할을 평가하면 해당 캐릭터와 연결
- Format: ("relationship"|<REVIEWER>|REVIEWER|<CHARACTER>|MOVIE_CHARACTER|<review describes character's role/action>|<strength>)
- 리뷰 내용이 캐릭터에 대한 것이면 MOVIE가 아닌 CHARACTER와 직접 연결

### Language Rules
- **MOVIE_CHARACTER** names: Use the exact character name from MOVIE_CONTEXT
- **entity_description**: Write in Korean (한국어)
- **relationship_description**: Write in Korean (한국어)
- **review content**: Write in Korean (한국어)
- **모든 출력은 한글로 작성** (entity_name 제외)

## Example Input:
```
Entity List
37: 도우 - 에단호크
38: 제인 - 사라스누크

Text: 도우는 시간여행 요원으로서 제인을 만나 사랑에 빠진다. 영화평론가 김철수는 "도우의 복잡한 감정선이 영화의 핵심"이라고 평가했다. 에단호크의 연기가 인상적이었다는 평가도 받았다.
```

## Example Output:
("entity"|도우|MOVIE_CHARACTER|도우는 시간여행 요원으로서 제인을 만나 사랑에 빠지는 인물이다)##
("entity"|제인|MOVIE_CHARACTER|제인은 도우가 사랑에 빠지는 인물이다)##
("entity"|김철수|REVIEWER|김철수는 이 영화를 평가한 영화평론가이다)##
("entity"|에단호크|ACTOR|에단호크는 도우 역을 맡은 배우로 인상적인 연기를 보여주었다는 평가를 받았다)##
("relationship"|도우|MOVIE_CHARACTER|제인|MOVIE_CHARACTER|도우는 제인과 사랑에 빠진다|9)##
("relationship"|김철수|REVIEWER|도우|MOVIE_CHARACTER|김철수는 도우의 복잡한 감정선이 영화의 핵심이라고 평가했다|8)##
("relationship"|에단호크|ACTOR|도우|MOVIE_CHARACTER|에단호크는 도우 역을 연기했다|10)##
("relationship"|김철수|REVIEWER|도우|MOVIE_CHARACTER|도우의 복잡한 감정선이 영화의 핵심|POSITIVE)<END>

## Counter Example (DO NOT do this):
- 스토리 설명에서 배우 이름이 나왔다고 MOVIE_STAFF를 생성하지 마세요
- "에단호크가 시간여행을 한다" -> 이건 캐릭터(도우)의 행동이지, 배우에 대한 언급이 아님
- 텍스트에서 배우의 연기력/수상 등 언급이 없으면 MOVIE_STAFF entity 생성 금지
- 리뷰가 캐릭터를 언급하는데 MOVIE에만 연결하지 마세요 -> CHARACTER와 직접 연결

######################
Output:
