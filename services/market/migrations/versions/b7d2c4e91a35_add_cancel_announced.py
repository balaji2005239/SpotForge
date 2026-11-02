"""add cancel_announced

Revision ID: b7d2c4e91a35
Revises: f2270f1752a9
Create Date: 2026-10-31 11:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = 'b7d2c4e91a35'
down_revision: str | Sequence[str] | None = 'f2270f1752a9'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column('book_bids', sa.Column('cancel_announced', sa.Boolean(), nullable=False, server_default=sa.false()))
    # cancels that happened before this revision were refunded the old way: don't announce them again
    op.execute("UPDATE book_bids SET cancel_announced = %s WHERE status = 'CANCELLED'"
               % ("true" if op.get_bind().dialect.name == "postgresql" else "1"))


def downgrade() -> None:
    op.drop_column('book_bids', 'cancel_announced')
