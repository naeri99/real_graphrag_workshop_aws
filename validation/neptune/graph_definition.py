from neptune.neptune_con import execute_cypher
from neptune.cyper_queries import generate_neptune_id


def create_movies(movies_data):
    """
    Create MOVIE nodes in Neptune with provided data
    
    Args:
        movies_data: List of movie dictionaries with title, genre, release_date, synopsis, etc.
    
    Returns:
        Result of the Cypher query execution
    """
    # Add neptune_id to each movie if not present
    for movie in movies_data:
        if 'neptune_id' not in movie or not movie['neptune_id']:
            movie['neptune_id'] = generate_neptune_id(movie.get('title', 'Unknown'), 'MOVIE')
    
    query = """
    UNWIND $movies AS movie
    CREATE (m:MOVIE {
        title: movie.title,
        genre: movie.genre,
        release_date: movie.release_date,
        synopsis: movie.synopsis,
        neptune_id: movie.neptune_id
    })
    RETURN count(m) as created_count
    """
    
    result = execute_cypher(query, movies=movies_data)
    return result


def create_characters(characters_data):
    """
    Create CHARACTER nodes in Neptune with provided data
    
    Args:
        characters_data: List of character dictionaries
    
    Returns:
        Result of the Cypher query execution
    """
    # Add neptune_id to each character if not present
    for character in characters_data:
        if 'neptune_id' not in character or not character['neptune_id']:
            character['neptune_id'] = generate_neptune_id(character.get('name', 'Unknown'), 'CHARACTER')
    
    query = """
    UNWIND $characters AS character
    CREATE (c:CHARACTER {
        name: character.name,
        movie_title: character.movie_title,
        neptune_id: character.neptune_id
    })
    RETURN count(c) as created_count
    """
    
    result = execute_cypher(query, characters=characters_data)
    return result


def create_actors(actors_data):
    """
    Create ACTOR nodes in Neptune with provided data
    
    Args:
        actors_data: List of actor dictionaries
    
    Returns:
        Result of the Cypher query execution
    """
    # Add neptune_id to each actor if not present
    for actor in actors_data:
        if 'neptune_id' not in actor or not actor['neptune_id']:
            actor['neptune_id'] = generate_neptune_id(actor.get('name', 'Unknown'), 'ACTOR')
    
    query = """
    UNWIND $actors AS actor
    CREATE (a:ACTOR {
        name: actor.name,
        birth_date: actor.birth_date,
        biography: actor.biography,
        neptune_id: actor.neptune_id
    })
    RETURN count(a) as created_count
    """
    
    result = execute_cypher(query, actors=actors_data)
    return result


def create_movie_staff(staff_data):
    """
    Create MOVIE_STAFF nodes in Neptune with provided data
    
    Args:
        staff_data: List of movie staff dictionaries
    
    Returns:
        Result of the Cypher query execution
    """
    # Add neptune_id to each staff member if not present
    for staff in staff_data:
        if 'neptune_id' not in staff or not staff['neptune_id']:
            staff['neptune_id'] = generate_neptune_id(staff.get('name', 'Unknown'), 'MOVIE_STAFF')
    
    query = """
    UNWIND $staff AS s
    CREATE (ms:MOVIE_STAFF {
        name: s.name,
        role: s.role,
        birth_date: s.birth_date,
        nationality: s.nationality,
        biography: s.biography,
        neptune_id: s.neptune_id
    })
    RETURN count(ms) as created_count
    """
    
    result = execute_cypher(query, staff=staff_data)
    return result


def create_reviewers(reviewers_data):
    """
    Create REVIEWER nodes in Neptune with provided data
    
    Args:
        reviewers_data: List of reviewer dictionaries
    
    Returns:
        Result of the Cypher query execution
    """
    # Add neptune_id to each reviewer if not present
    for reviewer in reviewers_data:
        if 'neptune_id' not in reviewer or not reviewer['neptune_id']:
            reviewer['neptune_id'] = generate_neptune_id(reviewer.get('name', 'Unknown'), 'REVIEWER')
    
    query = """
    UNWIND $reviewers AS reviewer
    CREATE (r:REVIEWER {
        name: reviewer.name,
        id: reviewer.id,
        channel: reviewer.channel,
        neptune_id: reviewer.neptune_id
    })
    RETURN count(r) as created_count
    """
    
    result = execute_cypher(query, reviewers=reviewers_data)
    return result


def get_all_movies():
    """Get all MOVIE nodes from Neptune"""
    query = """
    MATCH (m:MOVIE)
    RETURN m.title AS title, 
           m.genre AS genre, 
           m.release_date AS release_date,
           m.synopsis AS synopsis,
           m.neptune_id AS neptune_id
    ORDER BY m.title
    """
    
    result = execute_cypher(query)
    return result


def get_all_characters():
    """Get all CHARACTER nodes from Neptune"""
    query = """
    MATCH (c:CHARACTER)
    RETURN c.name AS name,
           c.description AS description,
           c.movie_title AS movie_title,
           c.neptune_id AS neptune_id
    ORDER BY c.name
    """
    
    result = execute_cypher(query)
    return result


def get_all_actors():
    """Get all ACTOR nodes from Neptune"""
    query = """
    MATCH (a:ACTOR)
    RETURN a.name AS name,
           a.birth_date AS birth_date,
           a.nationality AS nationality,
           a.biography AS biography,
           a.neptune_id AS neptune_id
    ORDER BY a.name
    """
    
    result = execute_cypher(query)
    return result


def get_all_movie_staff():
    """Get all MOVIE_STAFF nodes from Neptune"""
    query = """
    MATCH (ms:MOVIE_STAFF)
    RETURN ms.name AS name,
           ms.role AS role,
           ms.birth_date AS birth_date,
           ms.nationality AS nationality,
           ms.biography AS biography,
           ms.awards AS awards,
           ms.neptune_id AS neptune_id
    ORDER BY ms.name
    """
    
    result = execute_cypher(query)
    return result


def get_all_reviewers():
    """Get all REVIEWER nodes from Neptune"""
    query = """
    MATCH (r:REVIEWER)
    RETURN r.name AS name,
           r.id AS id,
           r.channel AS channel,
           r.platform AS platform,
           r.subscriber_count AS subscriber_count,
           r.specialty AS specialty,
           r.bio AS bio,
           r.neptune_id AS neptune_id
    ORDER BY r.name
    """
    
    result = execute_cypher(query)
    return result


def delete_all_movies():
    """Delete all MOVIE nodes and their relationships"""
    query = """
    MATCH (m:MOVIE)
    DETACH DELETE m
    RETURN count(*) as deleted_count
    """
    
    result = execute_cypher(query)
    return result


def delete_all_characters():
    """Delete all CHARACTER nodes and their relationships"""
    query = """
    MATCH (c:CHARACTER)
    DETACH DELETE c
    RETURN count(*) as deleted_count
    """
    
    result = execute_cypher(query)
    return result


def delete_all_actors():
    """Delete all ACTOR nodes and their relationships"""
    query = """
    MATCH (a:ACTOR)
    DETACH DELETE a
    RETURN count(*) as deleted_count
    """
    
    result = execute_cypher(query)
    return result


def delete_all_movie_staff():
    """Delete all MOVIE_STAFF nodes and their relationships"""
    query = """
    MATCH (ms:MOVIE_STAFF)
    DETACH DELETE ms
    RETURN count(*) as deleted_count
    """
    
    result = execute_cypher(query)
    return result


def delete_all_reviewers():
    """Delete all REVIEWER nodes and their relationships"""
    query = """
    MATCH (r:REVIEWER)
    DETACH DELETE r
    RETURN count(*) as deleted_count
    """
    
    result = execute_cypher(query)
    return result


# Example usage and test functions
def test_movie_creation():
    """Test movie creation with sample data"""
    sample_movies = [
        {
            "title": "Inception",
            "genre": "Sci-Fi",
            "release_date": "2010-07-16",
            "synopsis": "A thief who steals corporate secrets through dream-sharing technology is given the inverse task of planting an idea into the mind of a C.E.O."
        },
        {
            "title": "The Dark Knight",
            "genre": "Action",
            "release_date": "2008-07-18",
            "synopsis": "When the menace known as the Joker wreaks havoc and chaos on the people of Gotham, Batman must accept one of the greatest psychological and physical tests."
        }
    ]
    
    print("Creating sample movies...")
    result = create_movies(sample_movies)
    print(f"Result: {result}")
    
    print("\nRetrieving all movies...")
    movies = get_all_movies()
    print(f"Movies: {movies}")


def test_movie_staff_creation():
    """Test movie staff creation with sample data"""
    sample_staff = [
        {
            "name": "Christopher Nolan",
            "role": "Director",
            "birth_date": "1970-07-30",
            "nationality": "British-American",
            "biography": "Christopher Nolan is a British-American film director, producer, and screenwriter known for his complex narratives.",
            "awards": "Academy Award nominations, BAFTA Awards"
        },
        {
            "name": "Hans Zimmer",
            "role": "Composer",
            "birth_date": "1957-09-12",
            "nationality": "German",
            "biography": "Hans Zimmer is a German film score composer and record producer.",
            "awards": "Academy Award, Grammy Awards"
        }
    ]
    
    print("Creating sample movie staff...")
    result = create_movie_staff(sample_staff)
    print(f"Result: {result}")
    
    print("\nRetrieving all movie staff...")
    staff = get_all_movie_staff()
    print(f"Movie Staff: {staff}")


def test_reviewer_creation():
    """Test reviewer creation with sample data"""
    sample_reviewers = [
        {
            "name": "Movie Critic John",
            "id": "critic_john_001",
            "channel": "John's Movie Reviews",
            "platform": "YouTube",
            "subscriber_count": 150000,
            "specialty": "Sci-Fi and Action Movies",
            "bio": "Professional movie critic with 10 years of experience in film analysis."
        },
        {
            "name": "Cinema Sarah",
            "id": "cinema_sarah_002", 
            "channel": "Sarah's Cinema Corner",
            "platform": "YouTube",
            "subscriber_count": 85000,
            "specialty": "Independent and Art House Films",
            "bio": "Film school graduate specializing in independent cinema and foreign films."
        }
    ]
    
    print("Creating sample reviewers...")
    result = create_reviewers(sample_reviewers)
    print(f"Result: {result}")
    
    print("\nRetrieving all reviewers...")
    reviewers = get_all_reviewers()
    print(f"Reviewers: {reviewers}")


def test_all_entities():
    """Test creation of all entity types"""
    print("=== Testing All Entity Creation ===")
    
    # Test movies
    test_movie_creation()
    print("\n" + "="*50)
    
    # Test movie staff
    test_movie_staff_creation()
    print("\n" + "="*50)
    
    # Test reviewers
    test_reviewer_creation()
    print("\n" + "="*50)
    
    print("All tests completed!")


if __name__ == "__main__":
    test_all_entities()
