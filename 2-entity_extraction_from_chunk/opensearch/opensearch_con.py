import os
from opensearchpy import OpenSearch, RequestsHttpConnection


# OpenSearch Configuration - Environment variables or defaults
OPENSEARCH_URL = os.environ.get('OPENSEARCH_HOST', 'search-bank-opensearch-domain-7dbca7pwladjxtfeho3zsahdou.us-west-2.es.amazonaws.com')
OPENSEARCH_USER = os.environ.get('OPENSEARCH_USER', 'Bankadmin')
OPENSEARCH_PASSWORD = os.environ.get('OPENSEARCH_PASSWORD', 'Bankadmin123!')
OPENSEARCH_PORT = int(os.environ.get('OPENSEARCH_PORT', '443'))
USE_SSL = os.environ.get('OPENSEARCH_USE_SSL', 'true').lower() == 'true'


# Clean URL (remove protocol if present)
if OPENSEARCH_URL:
    OPENSEARCH_URL = OPENSEARCH_URL.replace('https://', '').replace('http://', '').rstrip('/')

# Global OpenSearch client (lazy initialization)
_opensearch_client = None

def get_opensearch_client():
    """Get or create OpenSearch client singleton with password or IAM authentication."""
    global _opensearch_client
    if _opensearch_client is None:
        
        if not OPENSEARCH_URL:
            raise ValueError("OPENSEARCH_URL environment variable is required")
        
        # Determine authentication method
        if OPENSEARCH_USER and OPENSEARCH_PASSWORD:
            # Use username/password authentication
            print("Using username/password authentication for OpenSearch")
            _opensearch_client = OpenSearch(
                hosts=[{'host': OPENSEARCH_URL, 'port': OPENSEARCH_PORT}],
                http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
                use_ssl=USE_SSL,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=30,
                max_retries=10,
                retry_on_timeout=True
            )
        else:
            # Use AWS IAM authentication (fallback)
            print("Using AWS IAM authentication for OpenSearch")
            try:
                import boto3
                from requests_aws4auth import AWS4Auth
                
                credentials = boto3.Session().get_credentials()
                if not credentials:
                    raise ValueError("No AWS credentials found")
                
                awsauth = AWS4Auth(
                    credentials.access_key,
                    credentials.secret_key,
                    boto3.Session().region_name or 'ap-northeast-2',
                    'es',
                    session_token=credentials.token
                )
                
                _opensearch_client = OpenSearch(
                    hosts=[{'host': OPENSEARCH_URL, 'port': OPENSEARCH_PORT}],
                    http_auth=awsauth,
                    use_ssl=USE_SSL,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection,
                    timeout=30,
                    max_retries=10,
                    retry_on_timeout=True
                )
            except ImportError:
                raise ValueError("boto3 and requests_aws4auth are required for AWS IAM authentication")
            except Exception as e:
                raise ValueError(f"Failed to set up AWS authentication: {e}")
    
    return _opensearch_client


def test_opensearch():
    """Test OpenSearch connection."""
    try:
        client = get_opensearch_client()
        response = client.info()
        print("✅ Successfully connected to OpenSearch")
        print(f"   Cluster: {response.get('cluster_name', 'Unknown')}")
        print(f"   Version: {response.get('version', {}).get('number', 'Unknown')}")
        return True
    except Exception as e:
        print(f"❌ OpenSearch connection failed: {e}")
        return False


def get_connection_info():
    """Display current connection configuration."""
    print("🔧 OpenSearch Connection Configuration:")
    print(f"   URL: {OPENSEARCH_URL}")
    print(f"   Port: {OPENSEARCH_PORT}")
    print(f"   SSL: {USE_SSL}")
    
    if OPENSEARCH_USER and OPENSEARCH_PASSWORD:
        print(f"   Auth: Username/Password ({OPENSEARCH_USER})")
    else:
        print("   Auth: AWS IAM")
  
    # Test connection
    test_opensearch()


if __name__ == "__main__":
    get_connection_info()