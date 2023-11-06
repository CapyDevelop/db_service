from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, func
from sqlalchemy.orm import relationship

from . import Base


class User(Base):
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    school_user_id = Column(String)
    capy_uuid = Column(String, nullable=False)


class UserAccess(Base):
    __tablename__ = 'user_access'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('user.id'))
    access_token = Column(String, nullable=False)
    refresh_token = Column(String, nullable=False)
    expires_in = Column(Integer, nullable=False)
    session_state = Column(String, nullable=False)
    time_create = Column(DateTime, nullable=False, server_default=func.now())

    # user = relationship('User', back_populates='user_access')
