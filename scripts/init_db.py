"""Creates schema/tables in Postgres"""

import asyncio

import aiofiles
from neptunedb import AsyncDBHandler
from neptunedb.db_config import DBConfig


async def schema_exists(db_reader: AsyncDBHandler, schema_name: str) -> bool:
    """Checks if schema exists in PostGreSQL database

    Parameters
    ----------
    schema_name : str
        the name of the schema

    Returns
    -------
    bool
        True if schema exists given by schema_name
    """
    query = """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name = $1;
    """
    result = await db_reader.fetch(query, schema_name)
    return result is not None


async def read_sql_file(file_path: str):
    """Executes an SQL file using an asyncpg connection

    Parameters
    ----------
    file_path : str
        _description_

    Returns
    -------
    _type_
        _description_
    """
    async with aiofiles.open(file_path, mode="r") as f:
        contents = await f.read()
        return contents


async def main():
    config = DBConfig.from_env()
    schema_name = "commodity"
    file_path: str = "./oil_dashboard/queries/init_db_commodity.sql"

    async with AsyncDBHandler(config) as reader:
        if await schema_exists(reader, schema_name):
            print(
                f"Schema {schema_name} already exists.  Skipping initialization"  # noqa
            )
        else:
            print(f"Schema {schema_name} not found.  Running init_db sql")
            sql = await read_sql_file(file_path)
            await reader.execute(sql)


if __name__ == "__main__":
    # used for debugging purposes
    asyncio.run(main())
