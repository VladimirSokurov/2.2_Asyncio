import asyncio
from aiohttp import ClientSession

from models import engine, Base, Session, Person


async def number_of_persons(client_session):
    async with client_session.get('https://swapi.dev/api/people/') as response:
        json_data = await response.json()
        return json_data.get('count')


async def download_links(links, key, client_session):
    values_list = []
    for link in links:
        async with client_session.get(link) as response:
            json_data = await response.json()
            values_list.append(json_data.get(key))
    values = ', '.join(values_list)
    return values


async def add_in_db(person_info):
    async with Session() as session:
        session.add(Person(**person_info))
        await session.commit()


async def get_person(person_id, client_session):
    async with client_session.get(f'https://swapi.dev/api/people/{person_id}') as response:
        json_data = await response.json()

        if json_data.get('name'):
            films_coro = download_links(json_data.get('films'), 'title', client_session)
            homeworld_coro = download_links([json_data.get('homeworld')], 'name', client_session)
            species_coro = download_links(json_data.get('species'), 'name', client_session)
            starships_coro = download_links(json_data.get('starships'), 'name', client_session)
            vehicles_coro = download_links(json_data.get('vehicles'), 'name', client_session)

            fields = await asyncio.gather(films_coro, homeworld_coro, species_coro, starships_coro, vehicles_coro)
            films, homeworld, species, starships, vehicles = fields

            json_data['id'] = person_id
            json_data['films'] = films
            json_data['homeworld'] = homeworld
            json_data['species'] = species
            json_data['starships'] = starships
            json_data['vehicles'] = vehicles

            del json_data['created']
            del json_data['edited']
            del json_data['url']

            return json_data

        else:
            return None


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with ClientSession() as client_session:
        number = await number_of_persons(client_session)

    async with ClientSession() as client_session:
        for i in range(1, number + 2):
            person = await get_person(i, client_session)
            if person:
                asyncio.create_task(add_in_db(person))

    all_tasks = asyncio.all_tasks()
    all_tasks = all_tasks - {asyncio.current_task()}

    await asyncio.gather(*all_tasks)


if __name__ == '__main__':
    asyncio.run(main())
