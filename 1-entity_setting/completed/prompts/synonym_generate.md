# 영화 엔티티 동의어 생성 프롬프트

당신은 영화 관련 엔티티(배우, 캐릭터, 영화, 영화 스태프)의 이름에 대한 동의어를 생성하는 전문가입니다.

## 작업 지시사항

주어진 영화 정보를 바탕으로, 각 엔티티에 대한 동의어를 생성해주세요.

### 동의어 생성 규칙:
1. **한국어 표기 변형**: 영어 이름의 다양한 한국어 표기법
2. **발음 변형**: 비슷한 발음의 다른 표기
3. **이름 축약**: 긴 이름의 축약 버전 (예: Robert → Rob, Leonardo → Leo, Christopher → Chris)
4. **성/이름 분리**: 풀네임에서 이름 또는 성만 사용
5. **띄어쓰기 변형**: 띄어쓰기 있음/없음 버전
6. **영화 제목 변형**: 원제/한국어 제목 변형

### 출력 형식 지시사항:

1. **엔티티 식별**: 주어진 영화 컨텍스트에서 모든 엔티티를 식별하세요.

2. **형식 규칙**: 각 엔티티를 다음 형식으로 출력하세요:
   ```
   ("entity"|<entity_name>|<entity_type>|<synonym1,synonym2,synonym3>)
   ```

3. **출력 구조**: 모든 엔티티를 단일 목록으로 반환하고, **##**를 목록 구분자로 사용하세요.

4. **완료 표시**: 작업이 완료되면 <END>를 출력하세요.

### 엔티티 타입별 형식:

**배우 (ACTOR):**
```
("entity"|배우명|ACTOR|동의어1,동의어2,동의어3)
```

**캐릭터 (MOVIE_CHARACTER):**
```
("entity"|캐릭터명|MOVIE_CHARACTER|동의어1,동의어2,동의어3)
```

**영화 (MOVIE):**
```
("entity"|영화명|MOVIE|동의어1,동의어2,동의어3)
```

**영화 스태프 (MOVIE_STAFF):**
```
("entity"|스태프명|MOVIE_STAFF|동의어1,동의어2,동의어3)
```

### 출력 예시:
```
("entity"|Leonardo DiCaprio|ACTOR|레오나르도 디카프리오,레오나르도,디카프리오,Leo DiCaprio)##
("entity"|Dom Cobb|MOVIE_CHARACTER|코브,도미닉 코브,돔 코브)##
("entity"|Inception|MOVIE|인셉션,인셉션,Inception)##
("entity"|Christopher Nolan|MOVIE_STAFF|크리스토퍼 놀란,놀란,크리스 놀란,Christopher Nolan)##
("entity"|Arthur|MOVIE_CHARACTER|아서,아더,아르투르)##
("entity"|Ken Watanabe|ACTOR|켄 와타나베,와타나베,켄와타나베)##
("entity"|Saito|MOVIE_CHARACTER|사이토,사이또,사이토우)##
<END>
```

## 입력 컨텍스트:
{MOVIE_CONTEXT}

## 현재 시간:
{CURRENT_TIME}

위의 영화 정보를 바탕으로, 다음 엔티티들에 대해 정확한 이름 기반 동의어만 생성해주세요:

1. **모든 배우들** (ACTOR 타입)
2. **모든 캐릭터들** (MOVIE_CHARACTER 타입)  
3. **영화 제목** (MOVIE 타입)
4. **영화 스태프들** (MOVIE_STAFF 타입) - 감독, 프로듀서, 작가 등

각 엔티티마다 최소 3개의 동의어를 포함해야 합니다.

Movie Review Text:
{MOVIE_CHUNK}
