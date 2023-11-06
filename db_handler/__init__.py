import os
import grpc
import uuid
import requests
import re

from concurrent import futures
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from db_handler.gql import get_user_info
from db_handler.models.user import User, UserAccess

import db_service.db_handler_pb2_grpc as pb2_grpc
import db_service.db_handler_pb2 as pb2

load_dotenv()
engine = create_engine(os.getenv("DB_ENGINE"))
Session = sessionmaker(bind=engine)


class DBService(pb2_grpc.DBServiceServicer):
    # def get_school_id(self, request, context):
    #     response = get_token(request.username, request.password)
    #     if not response:
    #         return pb2.GetSchoolIdResponse(school_id=-1, status=1, description="Not match")
    #     school_user_id = get_user_info(response.json()["access_token"])
    #     session = Session()
    #     user = session.query(User).filter(User.school_user_id == school_user_id).first()
    #     print(user, user.id)
    #     return pb2.GetSchoolIdResponse(school_id=school_user_id, status=0, description="Success")

    def check_user_exists(self, request, context):
        session = Session()
        user = session.query(User).filter(User.school_user_id == request.school_user_id).first()
        session.close()
        if not user:
            return pb2.CheckUserExistsResponse(exists=False)
        return pb2.CheckUserExistsResponse(exists=True)

    # def get_expire_time(self, request, context):
    #     session = Session()
    #     # make query to db join User and UserAccess and return expire_time and time_created
    #     user = (
    #         session.query(UserAccess)
    #         .join(User, User.id == UserAccess.user_id)
    #         .filter(User.school_user_id == request.school_user_id)
    #         .first()
    #     )
    #     session.close()
    #
    #     print(user.time_create.timestamp())
    #     if not user:
    #         return pb2.GetExpireTimeResponse(expire_time=-1, time_created=-1)
    #     time_create = int(user.time_create.timestamp())
    #
    #     return pb2.GetExpireTimeResponse(expire_time=user.expires_in, time_created=time_create)

    def get_uuid(self, request, context):
        session = Session()
        user = session.query(User).filter(User.school_user_id == request.school_user_id).first()
        session.close()
        if not user:
            return pb2.GetUUIDResponse(uuid="None")
        return pb2.GetUUIDResponse(uuid=user.capy_uuid)

    def set_access_data(self, request, context):
        session = Session()
        user = session.query(User).filter(User.school_user_id == request.school_user_id).first()
        if not user:
            return pb2.SetAccessDataResponse(status=1, description="Not match")
        user_access = session.query(UserAccess).filter(UserAccess.user_id == user.id).first()
        user_access.access_token = request.access_token
        user_access.refresh_token = request.refresh_token
        user_access.expires_in = request.expires_in
        user_access.session_state = request.session_state
        session.commit()
        session.close()
        return pb2.SetAccessDataResponse(status=0, description="Success")

    def set_new_user(self, request, context):
        session = Session()
        capy_uuid = str(uuid.uuid4())
        user = User(
            school_user_id=request.school_user_id,
            capy_uuid=capy_uuid
        )
        session.add(user)
        session.commit()
        user_access = UserAccess(
            user_id=user.id,
            access_token=request.access_token,
            refresh_token=request.refresh_token,
            expires_in=request.expires_in,
            session_state=request.session_state,
        )
        session.add(user_access)
        session.commit()
        session.close()
        return pb2.SetNewUserResponse(status=0, description="Success", capy_uuid=capy_uuid)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_DBServiceServicer_to_server(DBService(), server)
    server.add_insecure_port(f'[::]:{os.getenv("GRPC_PORT")}')
    print("start on", os.getenv("GRPC_PORT"))
    server.start()
    server.wait_for_termination()
