import asyncio
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URL = "sqlite:///star_wars_characters.db"

Base = declarative_base()


class Character(Base):
    __tablename__ = 'characters'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(Text)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(Text)
    starships = Column(Text)
    vehicles = Column(Text)


async def migrate():
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    print("Миграция завершена, таблица 'characters' создана.")


if __name__ == "__main__":
    asyncio.run(migrate())
