import asyncio
import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
from migrate import Character

DATABASE_URL = "sqlite+aiosqlite:///star_wars_characters.db"


async def fetch_character(http_session, id):
    url = f"https://swapi.dev/api/people/{id}/"
    async with http_session.get(url) as response:
        return await response.json()


async def fetch_film_titles(http_session, film_urls):
    film_titles = []
    for film_url in film_urls:
        try:
            async with http_session.get(film_url) as response:
                response.raise_for_status()
                film_data = await response.json()
                film_titles.append(film_data['title'])
        except asyncio.TimeoutError:
            print(f"Запрос к {film_url} завершился таймаутом")
        except Exception as e:
            print(f"Произошла ошибка при получении данных: {e}")
    return film_titles


async def load_character_to_db(db_session: AsyncSession, http_session, character_data):
    if 'url' not in character_data:
        print(f"No URL found for character data: {character_data}")
        return

    character_id = character_data['url'].split('/')[-2]
    character_name = character_data['name'] if 'name' in character_data else "Unknown"

    film_titles = await fetch_film_titles(http_session, character_data['films'])

    result = await db_session.execute(select(Character).where(Character.id == character_id))
    existing_character = result.scalars().first()

    if existing_character:
        print(f"Персонаж с ID {character_id} уже существует. Обновление данных...")
        existing_character.name = character_name
        existing_character.films = ', '.join(film_titles)
    else:
        character = Character(
            id=character_id,
            name=character_name,
            films=', '.join(film_titles)
        )
        db_session.add(character)

    await db_session.commit()


async def main():
    engine = create_async_engine(DATABASE_URL, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as db_session:
        async with aiohttp.ClientSession() as http_session:
            for id in range(1, 100):
                character_data = await fetch_character(http_session, id)
                if character_data.get('detail', None) == 'Not found':
                    continue
                await load_character_to_db(db_session, http_session, character_data)
                print(f"Загружен персонаж: {character_data['name']}")


if __name__ == "__main__":
    asyncio.run(main())
