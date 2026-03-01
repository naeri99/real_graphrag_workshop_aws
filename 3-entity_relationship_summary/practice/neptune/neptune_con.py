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
AWS_REGION = os.environ.get('AWS_REAL_REGION', 'us-west-2')

# 전역 변수
_neptune_session = None
_secrets_cache = None


def get_secret(secret_name: str, region_name: str = "us-west-2") -> dict:
    """AWS Secrets Manager에서 시크릿을 가져옵니다."""
    global _secrets_cache
    if _secrets_cache is not None:
        return _secrets_cache
    
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        _secrets_cache = json.loads(secret)
        return _secrets_cache
    except ClientError as e:
        raise Exception(f"Failed to retrieve secret '{secret_name}': {e}")


def get_neptune_config() -> dict:
    """Secrets Manager에서 Neptune 설정을 가져옵니다."""
    secrets = get_secret("opensearch-credentials", region_name = AWS_REGION)
    # Neptune Analytics는 포트 443 사용
    endpoint = secrets.get('neptune_endpoint', '')
    is_analytics = 'neptune-graph' in endpoint
    return {
        'endpoint': endpoint,
        'port': secrets.get('neptune_port', '443'),
        'read_endpoint': secrets.get('neptune_read_endpoint', '')
    }


# Neptune 설정 - Secrets Manager에서 로드
_neptune_config = get_neptune_config()
NEPTUNE_ENDPOINT = _neptune_config['endpoint']
NEPTUNE_PORT = _neptune_config['port']
NEPTUNE_READ_ENDPOINT = _neptune_config['read_endpoint']


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
    credentials = session.get_credentials().get_frozen_credentials()
    
    if not credentials:
        raise ValueError("AWS credentials not found. Configure credentials using AWS CLI or environment variables.")
    
    headers = {'Content-Type': 'application/json'}
    request = AWSRequest(method=method, url=url, data=data, headers=headers)
    
    signer = SigV4Auth(credentials, 'neptune-graph', AWS_REGION)
    signer.add_auth(request)
    
    return dict(request.headers)


def execute_cypher(query: str, parameters: dict = None, **kwargs) -> dict:
    """
    Execute a Cypher query against Neptune Analytics via SDK.
    """
    # Merge parameters dict with kwargs
    all_params = {}
    if parameters:
        all_params.update(parameters)
    if kwargs:
        all_params.update(kwargs)
    
    session = get_neptune_session()
    client = session.client('neptune-graph', region_name=AWS_REGION)
    
    try:
        query_kwargs = {
            'graphIdentifier': NEPTUNE_ENDPOINT.split('.')[0],  # g-ha4s00hi48
            'queryString': query,
            'language': 'OPEN_CYPHER'
        }
        if all_params:
            query_kwargs['parameters'] = all_params
        
        response = client.execute_query(**query_kwargs)
        result = json.loads(response['payload'].read())
        return result
    except Exception as e:
        print(f"Error: {e}")
        print(f"Query: {query[:300]}")
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
