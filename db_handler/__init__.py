import os
import grpc
import uuid
import logging

from concurrent import futures
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from db_handler.gql import get_user_info
from db_handler.models.user import User, UserAccess

import db_service.db_handler_pb2_grpc as pb2_grpc
import db_service.db_handler_pb2 as pb2

load_dotenv()
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - '
                           '%(levelname)s - %(message)s')
engine = create_engine(os.getenv("DB_ENGINE"))
Session = sessionmaker(bind=engine)


class DBService(pb2_grpc.DBServiceServicer):
    def check_user_exists(self, request, context):
        logging.info("[ Check user exists ] - Check user exists request. ----- START -----")
        try:
            logging.info(f"[ Check user exists ] - school_user_id: {request.school_user_id}")
        except Exception as e:
            logging.info(f"[ Check user exists ] - school_user_id:None")
        session = Session()
        user = session.query(User).filter(User.school_user_id == request.school_user_id).first()
        session.close()
        if not user:
            logging.info("[ Check user exists ] - Not such user. ----- END -----")
            return pb2.CheckUserExistsResponse(exists=False)
        logging.info("[ Check user exists ] - User exist. ----- END -----")
        return pb2.CheckUserExistsResponse(exists=True)

    def get_uuid(self, request, context):
        logging.info("[ Get UUID ] - Get UUID request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.school_user_id == request.school_user_id).first()
        session.close()
        if not user:
            logging.info("[ Get UUID ] - Not such user. ----- END -----")
            return pb2.GetUUIDResponse(uuid="None")
        logging.info(f"[ Get UUID ] - User uuid. uuid: {user.capy_uuid} ----- END -----")
        return pb2.GetUUIDResponse(uuid=user.capy_uuid)

    def set_access_data(self, request, context):
        logging.info("[ Set access data ] - Set access data request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.school_user_id == request.school_user_id).first()
        if not user:
            logging.info("[ Set access data ] - Not such user. ----- END -----")
            return pb2.SetAccessDataResponse(status=1, description="Not match")
        user_access = session.query(UserAccess).filter(UserAccess.user_id == user.id).first()
        user_access.access_token = request.access_token
        user_access.refresh_token = request.refresh_token
        user_access.expires_in = request.expires_in
        user_access.session_state = request.session_state
        session.commit()
        session.close()
        logging.info("[ Set access data ] - Success. ----- END -----")
        return pb2.SetAccessDataResponse(status=0, description="Success")

    def set_new_user(self, request, context):
        logging.info("[ Set new user ] - Set new user request. ----- START -----")
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
        logging.info("[ Set new user ] - Success. ----- END -----")
        return pb2.SetNewUserResponse(status=0, description="Success", capy_uuid=capy_uuid)

    def get_access_token_by_uuid(self, request, context):
        logging.info("[ Get access token by uuid ] - Get access token by uuid request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.capy_uuid == request.uuid).first()
        if not user:
            logging.info("[ Get access token by uuid ] - Not such user. ----- END -----")
            return pb2.GetAccessTokenByUUIDResponse(
                status=1,
                description="Not match"
            )
        user_access = session.query(UserAccess).filter(UserAccess.user_id == user.id).first()
        session.close()
        if not user_access:
            logging.info("[ Get access token by uuid ] - Not such user access. ----- END -----")
            return pb2.GetAccessTokenByUUIDResponse(
                status=1,
                description="Not match"
            )
        logging.info("[ Get access token by uuid ] - Success. ----- END -----")
        return pb2.GetAccessTokenByUUIDResponse(
            access_token=user_access.access_token,
            school_user_id=user.school_user_id,
            expires_in=user_access.expires_in,
            time_create=int(user_access.time_create.timestamp()),
            status=0,
            description="Success"
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_DBServiceServicer_to_server(DBService(), server)
    server.add_insecure_port(f'[::]:{os.getenv("GRPC_PORT")}')
    print("start on", os.getenv("GRPC_PORT"))
    server.start()
    server.wait_for_termination()
