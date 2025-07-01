"""Add job_id to workflow_execution

Revision ID: e6f7a8b9c1d2
Revises: d5e6f7a8b9c1
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e6f7a8b9c1d2'
down_revision = 'd5e6f7a8b9c1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add job_id column to workflow_executions table."""
    op.add_column(
        'workflow_executions',
        sa.Column('job_id', sa.String(36), nullable=True)
    )
    
    # Create index for job_id
    op.create_index(
        'ix_workflow_executions_job_id',
        'workflow_executions',
        ['job_id']
    )


def downgrade() -> None:
    """Remove job_id column from workflow_executions table."""
    op.drop_index('ix_workflow_executions_job_id', table_name='workflow_executions')
    op.drop_column('workflow_executions', 'job_id')