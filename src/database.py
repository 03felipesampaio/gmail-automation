import pendulum
import logging

import psycopg
from pathlib import Path
import os

logger = logging.getLogger("gmail_automation")
DIR_DATABASE_SCRIPTS = Path( os.getenv("DIR_DATABASE_SCRIPTS", 'src/database') )

if not DIR_DATABASE_SCRIPTS.exists():
    logger.error(f"Directory {DIR_DATABASE_SCRIPTS} does not exist")
    raise ValueError(f"Directory {DIR_DATABASE_SCRIPTS} does not exist")


async def init_database(postgres_url: str) -> psycopg.AsyncConnection:
    """"Initializes the database by running the init scripts.
    
    Args:
        postgres_url (str): Postgres URL with database.
        
    Returns:
        psycopg.AsyncConnection: Connection to the database.
    """
    if not postgres_url:
        logger.error("Postgres URL not provided")
        raise ValueError("Postgres URL not provided")
    
    logger.info(f"Connecting to database {postgres_url}")
    
    conn = await psycopg.AsyncConnection.connect(postgres_url)
    
    logger.info("Sucefully connected to database")
    
    init_scripts_dir = DIR_DATABASE_SCRIPTS / 'init'

    for script_file in init_scripts_dir.iterdir():
        if script_file.suffix != '.sql':
            continue
        
        with script_file.open(encoding='utf8') as file:
            script_content = file.read()
            async with conn.cursor() as cursor:
                try:
                    await cursor.execute(script_content)
                except psycopg.Error as e:
                    logger.error(f"Error running script {script_file.name}: {e}")
                    await conn.rollback()
                    raise e
                
    await conn.commit()
    
    return conn
