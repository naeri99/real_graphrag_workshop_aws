"""
Neptune Connection Module
- AWS Secrets Manager에서 Neptune 엔드포인트 정보 로드
- IAM 인증을 사용한 Neptune 연결
"""
import os
import json
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import ClientError
import requests


# AWS 리전 설정
AWS_REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')

# 전역 변수
_neptune_session = None
_secrets_cache = None


def get_secret(secret_name: str = "opensearch-credentials", region_name: str = None) -> dict:
    """AWS Secrets Manager에서 시크릿을 가져옵니다."""
    global _secrets_cache
    
    if _secrets_cache is not None:
        return _secrets_cache
    
    region = region_name or AWS_REGION
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        _secrets_cache = json.loads(secret)
        return _secrets_cache
    except ClientError as e:
        raise Exception(f"Failed to retrieve secret '{secret_name}': {e}")


# Secrets Manager에서 Neptune 설정 로드
_secrets = get_secret()
NEPTUNE_ENDPOINT = _secrets.get('neptune_endpoint', '')
NEPTUNE_PORT = _secrets.get('neptune_port', '8182')
NEPTUNE_READ_ENDPOINT = _secrets.get('neptune_read_endpoint', '')


def get_neptune_session():
    """Get or create boto3 session for Neptune."""
    global _neptune_session
    if _neptune_session is None:
        _neptune_session = boto3.Session(
            region_name=AWS_REGION,
            profile_name=os.environ.get('AWS_PROFILE')
        )
    return _neptune_session


def sign_request(method: str, url: str, data: str = None) -> dict:
    """Sign request with SigV4 for IAM authentication."""
    session = get_neptune_session()
    credentials = session.get_credentials()
    
    if not credentials:
        raise ValueError("AWS credentials not found. Configure credentials using AWS CLI or environment variables.")
    
    headers = {'Content-Type': 'application/json'}
    request = AWSRequest(method=method, url=url, data=data, headers=headers)
    
    signer = SigV4Auth(credentials, 'neptune-db', AWS_REGION)
    signer.add_auth(request)
    
    return dict(request.headers)


def execute_cypher(query: str, parameters: dict = None, **kwargs) -> dict:
    """
    Execute a Cypher query against Neptune.
    
    Args:
        query: Cypher query string
        parameters: Optional dict of query parameters
        **kwargs: Additional parameters (neo4j style) - will be merged with parameters
    
    Returns:
        Query result as dict
    
    Examples:
        # Style 1: dict parameters
        execute_cypher(query, parameters={"data": nodes, "movie_id": "inception"})
        
        # Style 2: neo4j style kwargs
        execute_cypher(query, data=nodes, movie_id="inception", chunk_id=1)
    """
    url = f"https://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/openCypher"
    
    # Merge parameters dict with kwargs (neo4j style support)
    all_params = {}
    if parameters:
        all_params.update(parameters)
    if kwargs:
        all_params.update(kwargs)
    
    payload = {'query': query}
    if all_params:
        payload['parameters'] = json.dumps(all_params)
    
    headers = sign_request('POST', url, json.dumps(payload))
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            verify=True
        )
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            print(f"Query: {query[:300]}")
            return None
        return response.json()
    except Exception as e:
        print(f"Exception: {e}")
        return None


def test_neptune_connection():
    """Test Neptune connection with a simple query."""
    print(f"🔧 Neptune Connection Configuration:")
    print(f"   Endpoint: {NEPTUNE_ENDPOINT}")
    print(f"   Port: {NEPTUNE_PORT}")
    print(f"   Read Endpoint: {NEPTUNE_READ_ENDPOINT}")
    print(f"   Region: {AWS_REGION}")
    
    query = "MATCH (n) RETURN count(n) as count LIMIT 1"
    result = execute_cypher(query)
    
    if result:
        print("✅ Successfully connected to Neptune")
        print(f"   Result: {result}")
    else:
        print("❌ Failed to connect to Neptune")
    
    return result


def delete_all():
    """Delete all nodes and relationships."""
    query = "MATCH (n) DETACH DELETE n"
    result = execute_cypher(query)
    if result is not None:
        print("✅ All data deleted successfully")
    return result


def count_nodes():
    """Count all nodes."""
    query = "MATCH (n) RETURN count(n) as count"
    result = execute_cypher(query)
    if result and 'results' in result:
        print(f"📊 Node count: {result['results']}")
    return result


def get_connection_info():
    """Display current connection configuration."""
    print("🔧 Neptune Connection Configuration:")
    print(f"   Endpoint: {NEPTUNE_ENDPOINT}")
    print(f"   Port: {NEPTUNE_PORT}")
    print(f"   Read Endpoint: {NEPTUNE_READ_ENDPOINT}")
    print(f"   Region: {AWS_REGION}")
    
    # Test connection
    test_neptune_connection()


if __name__ == "__main__":
    get_connection_info()
