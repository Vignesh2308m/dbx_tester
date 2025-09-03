from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Default Auth
def auth_default():
    config = Config()
    
# 1. PAT (Personal Access Token)
def auth_with_pat(host: str, token: str) -> WorkspaceClient:
    config = Config(host=host, token=token)
    return WorkspaceClient(config)

# 2. OAuth / Azure AD (via environment or config)
def auth_with_oauth(host: str) -> WorkspaceClient:
    config = Config(host=host, auth_type='oauth')
    return WorkspaceClient(config)

# 3. Databricks CLI Profile
def auth_with_cli_profile(profile: str = 'DEFAULT') -> WorkspaceClient:
    config = Config(profile=profile)
    return WorkspaceClient(config)

# 4. Google ID Token (for GCP-hosted Databricks)
def auth_with_google_id_token(host: str, google_id_token: str) -> WorkspaceClient:
    config = Config(host=host, google_service_account=google_id_token)
    return WorkspaceClient(config)

# 5. AWS IAM Role (for AWS-hosted Databricks)
def auth_with_aws_iam(host: str) -> WorkspaceClient:
    config = Config(host=host, auth_type='aws')
    return WorkspaceClient(config)

# 6. Username + Password (legacy, discouraged)
def auth_with_user_pass(host: str, username: str, password: str) -> WorkspaceClient:
    config = Config(host=host, username=username, password=password)
    return WorkspaceClient(config)