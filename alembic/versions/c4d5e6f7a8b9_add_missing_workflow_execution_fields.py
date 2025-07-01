"""Add missing workflow and execution fields

Revision ID: c4d5e6f7a8b9
Revises: b3d643bccdad
Create Date: 2025-06-30 16:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c4d5e6f7a8b9'
down_revision: Union[str, None] = 'b3d643bccdad'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade database schema."""
    # Add missing fields to workflows table
    op.add_column('workflows', sa.Column('version_id', sa.String(36), nullable=True))
    op.add_column('workflows', sa.Column('project_id', sa.Integer(), nullable=True))
    op.add_column('workflows', sa.Column('folder_id', sa.Integer(), nullable=True))
    op.add_column('workflows', sa.Column('nodes', sa.JSON(), nullable=True))
    op.add_column('workflows', sa.Column('connections', sa.JSON(), nullable=True))
    op.add_column('workflows', sa.Column('active', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('workflows', sa.Column('archived', sa.Boolean(), nullable=False, server_default='false'))
    
    # Create shared_workflows table
    op.create_table('shared_workflows',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('workflow_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('permission', sa.Enum('READ', 'EXECUTE', 'UPDATE', 'DELETE', 'SHARE', 'OWNER', name='workflowpermission'), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.ForeignKeyConstraint(['workflow_id'], ['workflows.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('workflow_id', 'user_id', name='uq_shared_workflows_workflow_user')
    )
    op.create_index('ix_shared_workflows_user_id', 'shared_workflows', ['user_id'], unique=False)
    op.create_index('ix_shared_workflows_workflow_id', 'shared_workflows', ['workflow_id'], unique=False)
    
    # Create workflow_tags table
    op.create_table('workflow_tags',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('workflow_id', sa.Integer(), nullable=False),
        sa.Column('tag', sa.String(50), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['workflow_id'], ['workflows.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('workflow_id', 'tag', name='uq_workflow_tags_workflow_tag')
    )
    op.create_index('ix_workflow_tags_tag', 'workflow_tags', ['tag'], unique=False)
    op.create_index('ix_workflow_tags_workflow_id', 'workflow_tags', ['workflow_id'], unique=False)
    
    # Create projects table (if not exists)
    op.create_table('projects',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('type', sa.String(50), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create folders table (if not exists)
    op.create_table('folders',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('parent_id', sa.Integer(), nullable=True),
        sa.Column('project_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['parent_id'], ['folders.id'], ),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Add missing fields to workflow_executions table (renamed from executions)
    op.add_column('workflow_executions', sa.Column('execution_time_ms', sa.Integer(), nullable=True))
    
    # Add missing fields to node_executions table
    op.add_column('node_executions', sa.Column('execution_time_ms', sa.Integer(), nullable=True))
    
    # Create foreign key constraints
    op.create_foreign_key(None, 'workflows', 'projects', ['project_id'], ['id'])
    op.create_foreign_key(None, 'workflows', 'folders', ['folder_id'], ['id'])
    
    # Migrate existing data - create owner entries in shared_workflows for all existing workflows
    op.execute("""
        INSERT INTO shared_workflows (workflow_id, user_id, permission, created_at, updated_at)
        SELECT id, user_id, 'OWNER', NOW(), NOW()
        FROM workflows
        WHERE NOT EXISTS (
            SELECT 1 FROM shared_workflows sw 
            WHERE sw.workflow_id = workflows.id AND sw.user_id = workflows.user_id
        )
    """)
    
    # Set default values for new columns
    op.execute("UPDATE workflows SET version_id = gen_random_uuid()::text WHERE version_id IS NULL")
    op.execute("UPDATE workflows SET nodes = '[]'::json WHERE nodes IS NULL")
    op.execute("UPDATE workflows SET connections = '[]'::json WHERE connections IS NULL")


def downgrade() -> None:
    """Downgrade database schema."""
    # Drop foreign key constraints
    op.drop_constraint(None, 'workflows', type_='foreignkey')
    op.drop_constraint(None, 'workflows', type_='foreignkey')
    
    # Drop columns from node_executions
    op.drop_column('node_executions', 'execution_time_ms')
    
    # Drop columns from workflow_executions
    op.drop_column('workflow_executions', 'execution_time_ms')
    
    # Drop tables
    op.drop_table('folders')
    op.drop_table('projects')
    op.drop_index('ix_workflow_tags_workflow_id', table_name='workflow_tags')
    op.drop_index('ix_workflow_tags_tag', table_name='workflow_tags')
    op.drop_table('workflow_tags')
    op.drop_index('ix_shared_workflows_workflow_id', table_name='shared_workflows')
    op.drop_index('ix_shared_workflows_user_id', table_name='shared_workflows')
    op.drop_table('shared_workflows')
    
    # Drop columns from workflows
    op.drop_column('workflows', 'archived')
    op.drop_column('workflows', 'active')
    op.drop_column('workflows', 'connections')
    op.drop_column('workflows', 'nodes')
    op.drop_column('workflows', 'folder_id')
    op.drop_column('workflows', 'project_id')
    op.drop_column('workflows', 'version_id')
    
    # Drop enum type
    op.execute("DROP TYPE IF EXISTS workflowpermission")