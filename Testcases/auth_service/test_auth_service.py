"""
Authentication API test cases.

This module contains integration tests for authentication-related
endpoints including user registration, login, token refresh,
and user profile retrieval (/auth/me).
"""

from datetime import timedelta
from fastapi.testclient import TestClient
from models.models import Users
from services.auth_service import hash_password
from utils.security import create_refresh_token,create_access_token

# API ROUTES
AUTH_REGISTER_URL = "/auth/register"
LOGIN_URL = "/auth/login"
REFRESH_URL = "/auth/refresh"
ME_URL = "/auth/me"

# /auth/register API TESTCASES
def test_register_success(client: TestClient, db_session):
    """
    Verify successful user registration with valid payload.
    """
    db_session.query(Users).delete()
    db_session.commit()

    payload = {
        "username": "testuser",
        "email": "testuser@mouritech.com",
        "password": "Password123"
    }

    resp = client.post(AUTH_REGISTER_URL, json=payload)

    assert resp.status_code == 200
    assert resp.json()["username"] == "testuser"


def test_register_invalid_email_domain(client: TestClient):
    """
    Verify registration fails for unsupported email domains.
    """
    payload = {
        "username": "invaliddomainuser",
        "email": "user@gmail.com",   
        "password": "Password123"
    }

    resp = client.post(AUTH_REGISTER_URL, json=payload)

    assert resp.status_code == 422


def test_register_email_exists(client: TestClient, db_session):
    """
    Verify registration fails when email already exists.
    """
    payload = {
        "username": "testuser",
        "email": "testuser@mouritech.com",
        "password": "Password123"
    }

    client.post(AUTH_REGISTER_URL, json=payload)

    resp = client.post(
        AUTH_REGISTER_URL,
        json={
            "username": "newuser",
            "email": "testuser@mouritech.com",
            "password": "Password123"
        }
    )

    assert resp.status_code == 400
    assert "Email already registered" in resp.json()["detail"]


def test_register_username_exists(client: TestClient, db_session):
    """
    Verify registration fails when username already exists.
    """
    payload = {
        "username": "testuser",
        "email": "testuser@mouritech.com",
        "password": "Password123"
    }

    client.post(AUTH_REGISTER_URL, json=payload)

    resp = client.post(
        AUTH_REGISTER_URL,
        json={
            "username": "testuser",
            "email": "other@mouritech.com",
            "password": "Password123"
        }
    )

    assert resp.status_code == 400
    assert "Username already registered" in resp.json()["detail"]

# /auth/login API TESTCASES

def test_login_success(client: TestClient, db_session):
    """
    Verify successful login with correct credentials.
    """
    db_session.query(Users).delete()
    db_session.commit()
    db_session.add(Users(username="u1", email="u1@mouritech.com",
                         password_hash=hash_password("Password123"))); db_session.commit()

    r = client.post(LOGIN_URL, json={"email": "u1@mouritech.com", "password": "Password123"})
    assert r.status_code == 200


def test_login_wrong_password(client: TestClient, db_session):
    """
    Verify login fails with incorrect password.
    """
    db_session.query(Users).delete()
    db_session.commit()
    db_session.add(Users(username="u1", email="u1@mouritech.com",
                         password_hash=hash_password("Password123"))); db_session.commit()

    r = client.post(LOGIN_URL, json={"email": "u1@mouritech.com", "password": "Wrong123"})
    assert r.status_code == 401


def test_login_user_not_found(client: TestClient, db_session):
    """
    Verify login fails for non-existent user.
    """
    db_session.query(Users).delete()
    db_session.commit()

    r = client.post(LOGIN_URL, json={"email": "no@mouritech.com", "password": "Password123"})
    assert r.status_code == 401


def test_login_missing_field(client: TestClient):
    """
    Verify login fails when required fields are missing.
    """

    r = client.post(LOGIN_URL, json={"email": "u1@mouritech.com"})
    assert r.status_code == 422

# /auth/refresh API TESTCASES

def test_refresh_success(client: TestClient):
    """
    Verify access token is issued for valid refresh token.
    """
    token = create_refresh_token({"user_id": 1})

    r = client.post(REFRESH_URL, json={"refresh_token": token})
    assert r.status_code == 200
    assert "access_token" in r.json()


def test_refresh_invalid_token(client: TestClient):
    """
    Verify refresh fails for invalid token.
    """
    r = client.post(REFRESH_URL, json={"refresh_token": "invalid.token"})
    assert r.status_code == 401


def test_refresh_access_token_used(client: TestClient):
    """
    Verify refresh endpoint rejects access tokens.
    """
    token = create_access_token({"user_id": 1})

    r = client.post("/auth/refresh", json={"refresh_token": token})
    assert r.status_code == 401


def test_refresh_missing_token(client: TestClient):
    """
    Verify refresh fails when token is missing.
    """
    r = client.post(REFRESH_URL, json={})
    assert r.status_code == 422


# /auth/me API TESTCASES

# HELPER FUNCTION

def auth(token):
    """
    Helper to generate Authorization header.
    """
    return {"Authorization": f"Bearer {token}"}


def test_get_me_success(client: TestClient):
    """
    Verify authenticated user profile retrieval.
    """
    r = client.get(ME_URL)
    assert r.status_code == 200
    assert "email" in r.json()


def test_get_me_no_token(unauth_client):
    """
    Verify profile access fails without authentication token.
    """
    r = unauth_client.get(ME_URL)
    assert r.status_code in (401, 403)


def test_get_me_invalid_token(unauth_client):
    """
    Verify profile access fails with invalid token.
    """
    r = unauth_client.get(ME_URL, headers=auth("invalid"))
    assert r.status_code == 401


def test_get_me_expired_token(unauth_client):
    """
    Verify profile access fails with expired token.
    """
    token = create_access_token({"user_id": 1}, expires_delta=timedelta(seconds=-1))
    r = unauth_client.get(ME_URL, headers=auth(token))
    assert r.status_code == 401
