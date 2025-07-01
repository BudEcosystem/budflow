"""Credential REST API routes."""

from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from budflow.auth.dependencies import get_current_user
from budflow.auth.models import User
from budflow.credentials.exceptions import (
    CredentialNotFoundError,
    CredentialAlreadyExistsError,
    CredentialAccessDeniedError,
    CredentialEncryptionError,
    InvalidCredentialTypeError,
)
from budflow.credentials.schemas import (
    CredentialCreate,
    CredentialUpdate,
    CredentialResponse,
    CredentialListResponse,
    CredentialShare,
    CredentialTestResponse,
    CredentialTypesResponse,
    CredentialTypeDefinition,
    OAuthAuthUrlResponse,
    OAuthCallbackRequest,
    BulkDeleteRequest,
    BulkDeleteResponse,
)
from budflow.credentials.service import credential_service
from budflow.database_deps import get_db

router = APIRouter(prefix="/credentials", tags=["credentials"])


@router.post("", response_model=CredentialResponse, status_code=status.HTTP_201_CREATED)
async def create_credential(
    credential_data: CredentialCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Create a new credential."""
    try:
        credential = await credential_service.create_credential(
            db, current_user.id, credential_data
        )
        return CredentialResponse.from_orm(credential)
    except CredentialAlreadyExistsError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except InvalidCredentialTypeError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except CredentialEncryptionError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to encrypt credential data"
        )


@router.get("", response_model=CredentialListResponse)
async def list_credentials(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=500, description="Number of items to fetch"),
    type: Optional[str] = Query(None, description="Filter by credential type"),
    include_shared: bool = Query(True, description="Include shared credentials"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """List credentials for the current user."""
    credentials, total = await credential_service.list_credentials(
        db,
        current_user.id,
        skip=skip,
        limit=limit,
        type_filter=type,
        include_shared=include_shared
    )
    
    return CredentialListResponse(
        items=[CredentialResponse.from_orm(c) for c in credentials],
        total=total,
        skip=skip,
        limit=limit
    )


@router.get("/types", response_model=CredentialTypesResponse)
async def list_credential_types(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user)
):
    """List available credential types."""
    types = await credential_service.get_credential_types(db)
    
    # Convert to response format
    type_defs = []
    for t in types:
        if isinstance(t, dict):
            type_defs.append(CredentialTypeDefinition(**t))
        else:
            type_defs.append(CredentialTypeDefinition(**t.to_dict()))
    
    return CredentialTypesResponse(types=type_defs)


@router.get("/{credential_id}", response_model=CredentialResponse)
async def get_credential(
    credential_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get a specific credential."""
    try:
        credential = await credential_service.get_credential(
            db, credential_id, current_user.id
        )
        return CredentialResponse.from_orm(credential)
    except CredentialNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Credential {credential_id} not found"
        )
    except CredentialAccessDeniedError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to credential"
        )


@router.patch("/{credential_id}", response_model=CredentialResponse)
async def update_credential(
    credential_id: int,
    update_data: CredentialUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Update a credential."""
    try:
        credential = await credential_service.update_credential(
            db, credential_id, current_user.id, update_data
        )
        return CredentialResponse.from_orm(credential)
    except CredentialNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Credential {credential_id} not found"
        )
    except CredentialAccessDeniedError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No edit permission for credential"
        )
    except CredentialAlreadyExistsError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/{credential_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_credential(
    credential_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Delete a credential."""
    try:
        await credential_service.delete_credential(
            db, credential_id, current_user.id
        )
    except CredentialNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Credential {credential_id} not found"
        )
    except CredentialAccessDeniedError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only owner can delete credential"
        )


@router.post("/{credential_id}/test", response_model=CredentialTestResponse)
async def test_credential(
    credential_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Test a credential."""
    try:
        result = await credential_service.test_credential(
            db, credential_id, current_user.id
        )
        return CredentialTestResponse(**result)
    except CredentialNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Credential {credential_id} not found"
        )
    except CredentialAccessDeniedError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to credential"
        )
    except Exception as e:
        return CredentialTestResponse(
            success=False,
            message=f"Test failed: {str(e)}"
        )


@router.put("/{credential_id}/share", response_model=CredentialResponse)
async def share_credential(
    credential_id: int,
    share_data: CredentialShare,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Share a credential with other users."""
    try:
        credential = await credential_service.share_credential(
            db, credential_id, current_user.id, share_data
        )
        return CredentialResponse.from_orm(credential)
    except CredentialNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Credential {credential_id} not found"
        )
    except CredentialAccessDeniedError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only owner can share credential"
        )


@router.get("/{credential_id}/oauth/auth-url", response_model=OAuthAuthUrlResponse)
async def get_oauth_auth_url(
    credential_id: int,
    redirect_uri: str = Query(..., description="OAuth redirect URI"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Get OAuth authorization URL for a credential."""
    try:
        result = await credential_service.get_oauth_auth_url(
            db, credential_id, current_user.id, redirect_uri
        )
        return OAuthAuthUrlResponse(**result)
    except CredentialNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Credential {credential_id} not found"
        )
    except CredentialAccessDeniedError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to credential"
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{credential_id}/oauth/callback", response_model=CredentialResponse)
async def handle_oauth_callback(
    credential_id: int,
    callback_data: OAuthCallbackRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Handle OAuth callback and store tokens."""
    try:
        credential = await credential_service.handle_oauth_callback(
            db,
            credential_id,
            current_user.id,
            callback_data.code,
            callback_data.state
        )
        return CredentialResponse.from_orm(credential)
    except CredentialNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Credential {credential_id} not found"
        )
    except CredentialAccessDeniedError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to credential"
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_credentials(
    delete_request: BulkDeleteRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Bulk delete credentials."""
    deleted_count = await credential_service.bulk_delete_credentials(
        db, delete_request.credentialIds, current_user.id
    )
    
    return BulkDeleteResponse(deleted=deleted_count)