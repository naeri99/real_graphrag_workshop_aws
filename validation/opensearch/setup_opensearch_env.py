#!/usr/bin/env python3
"""
OpenSearch Environment Setup Helper
This script helps you configure environment variables for OpenSearch connection.
"""

import os

def setup_password_auth():
    """Setup environment variables for password-based authentication."""
    print("🔧 Setting up OpenSearch Password Authentication")
    print("="*50)
    
    # Get OpenSearch configuration
    url = input("Enter OpenSearch URL (without https://): ").strip()
    if url.startswith('https://'):
        url = url[8:]
    if url.startswith('http://'):
        url = url[7:]
    
    username = input("Enter OpenSearch username: ").strip()
    password = input("Enter OpenSearch password: ").strip()
    port = input("Enter OpenSearch port (default: 443): ").strip() or "443"
    use_ssl = input("Use SSL? (y/n, default: y): ").strip().lower()
    use_ssl = "true" if use_ssl in ['', 'y', 'yes'] else "false"
    
    # Generate export commands
    export_commands = f"""
# OpenSearch Configuration
export OPENSEARCH_URL="{url}"
export OPENSEARCH_USER="{username}"
export OPENSEARCH_PASSWORD="{password}"
export OPENSEARCH_PORT="{port}"
export OPENSEARCH_USE_SSL="{use_ssl}"
"""
    
    print("\n✅ Configuration Complete!")
    print("="*50)
    print("Add these lines to your ~/.bashrc or run them in your terminal:")
    print(export_commands)
    
    # Save to file
    with open('opensearch_env.sh', 'w') as f:
        f.write(export_commands)
    
    print("📁 Configuration saved to 'opensearch_env.sh'")
    print("   Run: source opensearch_env.sh")
    
    return {
        'OPENSEARCH_URL': url,
        'OPENSEARCH_USER': username,
        'OPENSEARCH_PASSWORD': password,
        'OPENSEARCH_PORT': port,
        'OPENSEARCH_USE_SSL': use_ssl
    }


def test_connection_with_config(config):
    """Test connection with provided configuration."""
    print("\n🧪 Testing OpenSearch Connection...")
    
    # Temporarily set environment variables
    for key, value in config.items():
        os.environ[key] = value
    
    # Import and test
    try:
        from opensearch_con import test_opensearch
        success = test_opensearch()
        if success:
            print("🎉 Connection test successful!")
        else:
            print("❌ Connection test failed!")
    except Exception as e:
        print(f"❌ Error during connection test: {e}")


def show_current_config():
    """Show current environment configuration."""
    print("📋 Current OpenSearch Configuration:")
    print("="*40)
    
    env_vars = [
        'OPENSEARCH_URL',
        'OPENSEARCH_USER', 
        'OPENSEARCH_PASSWORD',
        'OPENSEARCH_PORT',
        'OPENSEARCH_USE_SSL'
    ]
    
    for var in env_vars:
        value = os.environ.get(var, 'Not set')
        if var == 'OPENSEARCH_PASSWORD' and value != 'Not set':
            value = '*' * len(value)  # Hide password
        print(f"   {var}: {value}")


def main():
    """Main function."""
    print("🔍 OpenSearch Configuration Helper")
    print("="*50)
    
    while True:
        print("\nOptions:")
        print("1. Setup password authentication")
        print("2. Show current configuration")
        print("3. Test current connection")
        print("4. Exit")
        
        choice = input("\nSelect option (1-4): ").strip()
        
        if choice == '1':
            config = setup_password_auth()
            test_choice = input("\nTest connection now? (y/n): ").strip().lower()
            if test_choice in ['y', 'yes']:
                test_connection_with_config(config)
        
        elif choice == '2':
            show_current_config()
        
        elif choice == '3':
            try:
                from opensearch_con import test_opensearch
                test_opensearch()
            except Exception as e:
                print(f"❌ Error: {e}")
        
        elif choice == '4':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid option. Please select 1-4.")


if __name__ == "__main__":
    main()