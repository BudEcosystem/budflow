"""Add credentials tables

Revision ID: d5e6f7a8b9c1
Revises: c4d5e6f7a8b9
Create Date: 2025-06-30 17:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd5e6f7a8b9c1'
down_revision: Union[str, None] = 'c4d5e6f7a8b9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade database schema."""
    # Create credentials table
    op.create_table('credentials',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('type', sa.String(50), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('data', sa.JSON(), nullable=False),  # Encrypted
        sa.Column('settings', sa.JSON(), nullable=True),
        sa.Column('oauth_token_data', sa.JSON(), nullable=True),  # Encrypted
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('no_data_expression', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('last_used_at', sa.DateTime(), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.Column('usage_count', sa.Integer(), nullable=False, server_default='0'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id', 'name', name='uq_credentials_user_name')
    )
    op.create_index('ix_credentials_type', 'credentials', ['type'], unique=False)
    op.create_index('ix_credentials_updated_at', 'credentials', ['updated_at'], unique=False)
    op.create_index('ix_credentials_user_id', 'credentials', ['user_id'], unique=False)
    
    # Create shared_credentials table
    op.create_table('shared_credentials',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('credential_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('permission', sa.String(20), nullable=False, server_default='use'),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['credential_id'], ['credentials.id'], ),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('credential_id', 'user_id', name='uq_shared_credentials_credential_user')
    )
    op.create_index('ix_shared_credentials_credential_id', 'shared_credentials', ['credential_id'], unique=False)
    op.create_index('ix_shared_credentials_user_id', 'shared_credentials', ['user_id'], unique=False)
    
    # Create node_credentials table
    op.create_table('node_credentials',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('node_id', sa.Integer(), nullable=False),
        sa.Column('credential_id', sa.Integer(), nullable=False),
        sa.Column('parameter_key', sa.String(255), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['credential_id'], ['credentials.id'], ),
        sa.ForeignKeyConstraint(['node_id'], ['workflow_nodes.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('node_id', 'parameter_key', name='uq_node_credentials_node_param')
    )
    op.create_index('ix_node_credentials_credential_id', 'node_credentials', ['credential_id'], unique=False)
    op.create_index('ix_node_credentials_node_id', 'node_credentials', ['node_id'], unique=False)
    
    # Create credential_types table
    op.create_table('credential_types',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(50), nullable=False),
        sa.Column('display_name', sa.String(255), nullable=False),
        sa.Column('icon', sa.String(50), nullable=True),
        sa.Column('documentation_url', sa.String(500), nullable=True),
        sa.Column('properties', sa.JSON(), nullable=False),
        sa.Column('oauth_config', sa.JSON(), nullable=True),
        sa.Column('test_function', sa.String(100), nullable=True),
        sa.Column('generic', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name')
    )
    
    # Insert default credential types
    op.execute("""
        INSERT INTO credential_types (name, display_name, properties, created_at, updated_at)
        VALUES
        ('apiKey', 'API Key', '[{"name": "apiKey", "displayName": "API Key", "type": "string", "required": true, "typeOptions": {"password": true}}]', NOW(), NOW()),
        ('basicAuth', 'Basic Auth', '[{"name": "username", "displayName": "Username", "type": "string", "required": true}, {"name": "password", "displayName": "Password", "type": "string", "required": true, "typeOptions": {"password": true}}]', NOW(), NOW()),
        ('bearerToken', 'Bearer Token', '[{"name": "token", "displayName": "Token", "type": "string", "required": true, "typeOptions": {"password": true}}]', NOW(), NOW()),
        ('oauth2', 'OAuth2', '[{"name": "clientId", "displayName": "Client ID", "type": "string", "required": true}, {"name": "clientSecret", "displayName": "Client Secret", "type": "string", "required": true, "typeOptions": {"password": true}}, {"name": "authUrl", "displayName": "Authorization URL", "type": "string", "required": true}, {"name": "accessTokenUrl", "displayName": "Access Token URL", "type": "string", "required": true}]', NOW(), NOW()),
        ('ssh', 'SSH', '[{"name": "host", "displayName": "Host", "type": "string", "required": true}, {"name": "port", "displayName": "Port", "type": "number", "required": true, "default": 22}, {"name": "username", "displayName": "Username", "type": "string", "required": true}, {"name": "privateKey", "displayName": "Private Key", "type": "string", "required": true, "typeOptions": {"password": true, "rows": 10}}]', NOW(), NOW())
    """)


def downgrade() -> None:
    """Downgrade database schema."""
    # Drop tables in reverse order
    op.drop_table('credential_types')
    op.drop_index('ix_node_credentials_node_id', table_name='node_credentials')
    op.drop_index('ix_node_credentials_credential_id', table_name='node_credentials')
    op.drop_table('node_credentials')
    op.drop_index('ix_shared_credentials_user_id', table_name='shared_credentials')
    op.drop_index('ix_shared_credentials_credential_id', table_name='shared_credentials')
    op.drop_table('shared_credentials')
    op.drop_index('ix_credentials_user_id', table_name='credentials')
    op.drop_index('ix_credentials_updated_at', table_name='credentials')
    op.drop_index('ix_credentials_type', table_name='credentials')
    op.drop_table('credentials')