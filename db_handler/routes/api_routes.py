import re
import uuid

import requests
from flask import request

from db_handler.gql import get_user_info
from db_handler.models.user import User, UserAccess

from .. import db
from . import routes

login_action_pattern = re.compile(r'(?P<LoginActionURL>https://.+?)"')
oauth_code_pattern = re.compile(r'code=(?P<OAuthCode>[^&$]+)[&$]?')


def get_token(login, password):
    state = str(uuid.uuid4())
    nonce = str(uuid.uuid4())

    session = requests.Session()

    response = requests.get(
        f"https://auth.sberclass.ru/auth/realms/EduPowerKeycloak/protocol/openid-connect/auth?client_id=school21&redirect_uri=https%3A%2F%2Fedu.21-school.ru%2F&state={state}&response_mode=fragment&response_type=code&scope=openid&nonce={nonce}",
        timeout=5)

    session.cookies.update(response.cookies)

    new_url = login_action_pattern.search(response.text).group("LoginActionURL").replace("amp;", "")

    response = session.post(new_url, data={"username": login, "password": password},
                            allow_redirects=False)

    session.cookies.update(response.cookies)

    location = response.headers.get("location")

    response = session.post(location, allow_redirects=False)

    location = response.headers.get("location")

    auth_code = oauth_code_pattern.search(location).group("OAuthCode")
    session.cookies.update(response.cookies)

    response = session.post("https://auth.sberclass.ru/auth/realms/EduPowerKeycloak/protocol/openid-connect/token",
                            data={
                                "code": auth_code,
                                "grant_type": "authorization_code",
                                "client_id": "school21",
                                "redirect_uri": "https://edu.21-school.ru/"
                            })

    token = response.json()["access_token"]

    print(token)
    print(response.json())
    print(response.cookies)
    return response


@routes.get('/check_uuid/<capy_uuid>')
def get_user(capy_uuid):
    user = User.query.filter_by(capy_uuid=capy_uuid).first()
    if not user:
        return {
            "status": "FAIL",
            "status_code": 1,
            "message": "Not authorized",
            "data": {}
        }, 401
    return {
        "status": "OK",
        "status_code": 0,
        "message": "Success",
        "data": {}
    }


@routes.post('/login')
def login():
    data = request.json
    login = data["login"]
    password = data["password"]
    if not login or not password:
        return {
            "status": "FAIL",
            "status_code": 2,
            "message": "Not such data",
            "data": {}
        }, 400
    auth_data = get_token(login, password)
    if not auth_data:
        return {
            "status": "FAIL",
            "status_code": 3,
            "message": "Error get school access data",
            "data": {}
        }, 400
    school_user_id = get_user_info(auth_data.json()["access_token"])
    if not school_user_id:
        return {
            "status": "FAIL",
            "status_code": 4,
            "message": "Error get school user id",
            "data": {}
        }, 400
    user = User.query.filter_by(school_user_id=school_user_id).first()
    if not user:
        user = User(school_user_id=school_user_id, capy_uuid=str(uuid.uuid4()))
        db.session.add(user)
        db.session.commit()
        user_access = UserAccess(user_id=user.id, access_token=auth_data.json()["access_token"],
                                 refresh_token=auth_data.json()["refresh_token"], session_state=auth_data.json()["session_state"],
                                 expires_in=auth_data.json()["expires_in"])
        db.session.add(user_access)
        db.session.commit()
    return {
        "status_code": 0
    }
