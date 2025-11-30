"""
Authentication Middleware

Handles API key and JWT token authentication
"""

from fastapi import HTTPException, Security, Depends, status
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
import os
import jwt
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Security schemes
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)

# JWT settings
JWT_SECRET = os.getenv("JWT_SECRET", "your-secret-key-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24


def verify_api_key(api_key: Optional[str] = Security(api_key_header)) -> bool:
    """
    Verify API key authentication
    
    Args:
        api_key: API key from request header
    
    Returns:
        True if valid
    
    Raises:
        HTTPException: If authentication is enabled and key is invalid
    """
    # Check if authentication is enabled
    auth_enabled = os.getenv("AUTH_ENABLED", "false").lower() == "true"
    
    if not auth_enabled:
        logger.debug("Authentication disabled - allowing request")
        return True
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required"
        )
    
    # Validate API key (in production, check against database)
    valid_api_keys = os.getenv("VALID_API_KEYS", "").split(",")
    
    if api_key not in valid_api_keys:
        logger.warning(f"Invalid API key attempted: {api_key[:10]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    
    logger.debug("API key authentication successful")
    return True


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Create JWT access token
    
    Args:
        data: Data to encode in token
        expires_delta: Token expiration time
    
    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)
    
    return encoded_jwt


def verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme)) -> dict:
    """
    Verify JWT token
    
    Args:
        credentials: Bearer token from request header
    
    Returns:
        Decoded token data
    
    Raises:
        HTTPException: If token is invalid or expired
    """
    # Check if authentication is enabled
    auth_enabled = os.getenv("AUTH_ENABLED", "false").lower() == "true"
    
    if not auth_enabled:
        return {"authenticated": False, "reason": "auth_disabled"}
    
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    token = credentials.credentials
    
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    except jwt.JWTError as e:
        logger.error(f"JWT validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"}
        )


def get_current_user(token_data: dict = Depends(verify_token)) -> dict:
    """
    Get current authenticated user from token
    
    Args:
        token_data: Decoded token data
    
    Returns:
        User information
    """
    if not token_data.get("authenticated", True):
        return {"user_id": "anonymous", "authenticated": False}
    
    user_id = token_data.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token - no user ID"
        )
    
    return {
        "user_id": user_id,
        "authenticated": True,
        "roles": token_data.get("roles", [])
    }


# Optional authentication dependency (doesn't raise error if not authenticated)
async def optional_authentication(
    api_key: Optional[str] = Security(api_key_header),
    credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme)
) -> Optional[dict]:
    """
    Optional authentication - doesn't fail if credentials not provided
    
    Returns:
        User info if authenticated, None otherwise
    """
    try:
        if api_key:
            verify_api_key(api_key)
            return {"authenticated": True, "method": "api_key"}
        
        if credentials:
            token_data = verify_token(credentials)
            return {"authenticated": True, "method": "jwt", "data": token_data}
        
        return None
    
    except HTTPException:
        return None
