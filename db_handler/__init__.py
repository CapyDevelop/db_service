import datetime
import logging
import os
from concurrent import futures

import db_service.db_handler_pb2 as pb2
import db_service.db_handler_pb2_grpc as pb2_grpc
import grpc
from dotenv import load_dotenv
from orm_models import Capybara, Friend, User, UserAccess, UserAvatar
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from db_handler.gql import get_user_info

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
        except Exception:
            logging.info("[ Check user exists ] - school_user_id:None")
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
        logging.info(f"[ Set access data ] - school_user_id: {request.school_user_id}")
        user = session.query(User).filter(User.school_user_id == request.school_user_id).first()
        if not user:
            logging.info("[ Set access data ] - Not such user. ----- END -----")
            return pb2.SetAccessDataResponse(status=1, description="Not match")
        user_access = session.query(UserAccess).filter(UserAccess.user_id == user.id).first()
        user_access.access_token = request.access_token
        user_access.refresh_token = request.refresh_token
        user_access.expires_in = request.expires_in
        user_access.session_state = request.session_state
        user_access.time_create = datetime.datetime.now()
        session.commit()
        session.close()
        logging.info("[ Set access data ] - Success. ----- END -----")
        return pb2.SetAccessDataResponse(status=0, description="Success")

    def set_new_user(self, request, context):
        logging.info("[ Set new user ] - Set new user request. ----- START -----")
        session = Session()
        user = User(
            school_user_id=request.school_user_id,
            capy_uuid=request.uuid
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
        return pb2.SetNewUserResponse(status=0, description="Success", capy_uuid=request.uuid)

    def get_access_token_by_uuid(self, request, context):
        logging.info("[ Get access token by uuid ] - Get access token by uuid request. ----- START -----")
        session = Session()
        logging.info(f"[ Get access token by uuid ] - uuid: {request.uuid}")
        user = session.query(User).filter(User.capy_uuid == request.uuid).first()
        if not user:
            session.close()
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

    def set_avatar(self, request, context):
        uuid = request.uuid
        avatar = request.avatar
        logging.info("[ Set avatar ] - Set avatar request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.capy_uuid == uuid).first()
        if not user:
            logging.info("[ Set avatar ] - Not such user. ----- END -----")
            return pb2.SetAvatarResponse(
                status=1,
                description="Not match"
            )
        user_avatar = UserAvatar(
            user_id=user.id,
            avatar=avatar
        )
        session.add(user_avatar)
        session.commit()
        session.close()
        logging.info("[ Set avatar ] - Success. ----- END -----")
        return pb2.SetAvatarResponse(
            status=0,
            description="Success"
        )

    def get_avatar(self, request, context):
        logging.info("[ Get avatar ] - Get avatar request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.capy_uuid == request.uuid).first()
        if not user:
            logging.info("[ Get avatar ] - Not such user. ----- END -----")
            return pb2.GetAvatarResponse(
                status=1,
                description="Not match"
            )
        user_avatar = session.query(UserAvatar).filter(UserAvatar.user_id == user.id).order_by(UserAvatar.id.desc()).first()
        session.close()
        if not user_avatar:
            logging.info("[ Get avatar ] - No set user avatar. ----- END -----")
            return pb2.GetAvatarResponse(
                status=0,
                description="Default avatar"
            )
        logging.info("[ Get avatar ] - Success. ----- END -----")
        return pb2.GetAvatarResponse(
            avatar=user_avatar.avatar,
            status=0,
            description="Success"
        )

    def get_peer_info(self, request, context):
        uuid = request.request_uuid
        logging.info("[ Get peer info ] - Get peer info request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.capy_uuid == uuid).first()
        if not user:
            logging.info("[ Get peer info ] - Not such user. ----- END -----")
            return pb2.GetPeerInfoResponse(
                status=1,
                description="Not such user"
            )
        session.close()
        nickname = request.nickname
        session = Session()
        print(f"{nickname}@student,21-school.ru")
        capybara = session.query(Capybara).filter(Capybara.login == f"{nickname}@student.21-school.ru").first()
        if not capybara:
            logging.info("[ Get peer info ] - Not such capybara peer. ----- END -----")
            session.close()
            return pb2.GetPeerInfoResponse(
                status=1,
                description="Запрашиваемый пользователь не найден в капибарах"
            )
        user = session.query(User).filter(User.school_user_id == capybara.school_user_id).first()
        if not user:
            session.close()
            logging.info("[ Get peer info ] - Not such user peer. ----- END -----")
            return pb2.GetPeerInfoResponse(
                status=1,
                description="Запрашиваемый пользователь не зарегистрирован на Платформе"
            )
        avatar = session.query(UserAvatar).filter(UserAvatar.user_id == user.id).order_by(UserAvatar.id.desc()).first()
        if not avatar:
            avatar_path = "https://capyavatars.storage.yandexcloud.net/avatar/default/default.webp"
        else:
            avatar_path = "https://capyavatars.storage.yandexcloud.net/avatar/" + str(user.capy_uuid) + "/" + avatar.avatar
        session.close()
        logging.info("[ Get peer info ] - Success. ----- END -----")
        return pb2.GetPeerInfoResponse(
            login=capybara.login,
            avatar=avatar_path,
            status=0,
            description="Success"
        )

    def get_friend_stats(self, request, context):
        uuid = request.uuid
        logging.info("[ Get friend stats ] - Get friend stats request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.capy_uuid == uuid).first()
        if not user:
            session.close()
            logging.info("[ Get friend stats ] - Not such user. ----- END -----")
            return pb2.GetFriendStatsResponse(
                status=1,
                description="Пользователь не найден"
            )

        friends_count = session.query(
            func.count()
        ).select_from(Friend).filter(Friend.peer_1 == user.id).scalar()

        subscribers_count = session.query(func.count()).filter(
            Friend.peer_2 == user.id
        ).scalar()

        session.close()
        logging.info("[ Get friend stats ] - Success. ----- END -----")
        return pb2.GetFriendStatsResponse(
            friends=friends_count,
            subscribers=subscribers_count,
            status=0,
            description="Success"
        )

    def search_user(self, request, context):
        logging.info("[ Search user ] - Search user request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.capy_uuid == request.uuid).first()
        if not user:
            session.close()
            logging.info("[ Search user ] - Not such user. ----- END -----")
            return pb2.SearchUserResponse(
                status=1,
                description="Пользователь не найден"
            )
        friends = []
        on_platform = []
        out_platform = []
        # ищу среди капибар тех, у кого в нике есть nickname
        users = session.query(Capybara).filter(Capybara.login.like(f"%{request.nickname}%@student.21-school.ru"), Capybara.is_student == True).all()
        for user_ in users:
            user_school_id = user_.school_user_id
            tmp = session.query(User).filter(User.school_user_id == user_school_id).first()
            if not tmp:
                out_platform.append(pb2.SearchedUser(
                    login=user_.login,
                    avatar="https://capyavatars.storage.yandexcloud.net/avatar/default/no-user.webp"
                ))
            else:
                print(user.id, tmp.id)
                friend = session.query(Friend).filter(Friend.peer_1 == user.id, Friend.peer_2 == tmp.id).first()
                avatar = session.query(UserAvatar).filter(UserAvatar.user_id == tmp.id).order_by(
                    UserAvatar.id.desc()).first()
                if not avatar:
                    avatar_path = "https://capyavatars.storage.yandexcloud.net/avatar/default/default.webp"
                else:
                    avatar_path = "https://capyavatars.storage.yandexcloud.net/avatar/" + str(
                        tmp.capy_uuid) + "/" + avatar.avatar
                if friend:
                    friends.append(pb2.SearchedUser(
                        login=user_.login,
                        avatar=avatar_path
                    ))
                else:
                    if (user.id == tmp.id):
                        continue
                    on_platform.append(pb2.SearchedUser(
                        login=user_.login,
                        avatar=avatar_path
                    ))
        logging.info("[ Search user ] - Success. ----- END -----")
        session.close()
        return pb2.SearchUserResponse(
            status=0,
            description="Success",
            friends=friends,
            on_platform=on_platform,
            out_platform=out_platform
        )

    def add_friend(self, request, context):
        logging.info("[ Add friend ] - Add friend request. ----- START -----")
        session = Session()
        user = session.query(User).filter(User.capy_uuid == request.uuid).first()
        if not user:
            session.close()
            logging.info("[ Add friend ] - Not such user. ----- END -----")
            return pb2.AddFriendResponse(
                status=1,
                description="Пользователь не найден"
            )
        capybara = session.query(Capybara).filter(Capybara.login == request.login).first()
        if not capybara:
            session.close()
            logging.info("[ Add friend ] - Not such capybara. ----- END -----")
            return pb2.AddFriendResponse(
                status=1,
                description="Пользователь не найден"
            )
        user_peer = session.query(User).filter(User.school_user_id == capybara.school_user_id).first()
        if not user_peer:
            session.close()
            logging.info("[ Add friend ] - Not such user peer. ----- END -----")
            return pb2.AddFriendResponse(
                status=1,
                description="Пользователь не найден"
            )
        friend = session.query(Friend).filter(Friend.peer_1 == user.id, Friend.peer_2 == user_peer.id).first()
        if friend:
            session.close()
            logging.info("[ Add friend ] - Already friends. ----- END -----")
            return pb2.AddFriendResponse(
                status=1,
                description="Пользователь уже в друзьях"
            )
        friend = Friend(
            peer_1=user.id,
            peer_2=user_peer.id
        )
        session.add(friend)
        session.commit()
        session.close()
        logging.info("[ Add friend ] - Success. ----- END -----")
        return pb2.AddFriendResponse(
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
