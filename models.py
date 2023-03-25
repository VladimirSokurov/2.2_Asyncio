from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

engine = create_async_engine('postgresql+asyncpg://postgres:DB_PASSWORD@127.0.0.1:5432/DB_NAME')
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


class Person(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)
