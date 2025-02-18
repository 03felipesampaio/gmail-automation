import pendulum
import logging

import psycopg
import psycopg_pool
from psycopg.rows import dict_row

from pathlib import Path
import os

logger = logging.getLogger("gmail_automation")
DIR_DATABASE_SCRIPTS = Path( os.getenv("DIR_DATABASE_SCRIPTS", 'src/database/sql') )

if not DIR_DATABASE_SCRIPTS.exists():
    logger.error(f"Directory {DIR_DATABASE_SCRIPTS} does not exist")
    raise ValueError(f"Directory {DIR_DATABASE_SCRIPTS} does not exist")


async def connect_to_database(postgres_url: str):
    """Connects to the database.
    
    Args:
        postgres_url (str): Postgres URL with database.
        
    Returns:
        psycopg_pool.AsyncConnectionPool: Connection pool to the database.
    """
    if not postgres_url:
        logger.error("Postgres URL not provided")
        raise ValueError("Postgres URL not provided")
    
    logger.info(f"Connecting to database {postgres_url}")
    
    pool = psycopg_pool.AsyncConnectionPool(
        postgres_url, 
        min_size=4, 
        max_size=16, 
        open=False, 
        kwargs={"row_factory": dict_row}
    )
    await pool.open()
    
    logger.info("Sucefully connected to database")
    
    return pool


async def init_database(conn: psycopg.AsyncConnection):
    """"Initializes the database by running the init scripts.
    
    Args:
        conn (psycopg.AsyncConnection): Connection to the database.
        
    Returns:
        psycopg.AsyncConnection: Connection to the database.
    """
    
    init_scripts_dir = DIR_DATABASE_SCRIPTS / 'init'

    for script_file in init_scripts_dir.iterdir():
        if script_file.suffix != '.sql':
            continue
        
        file_content = script_file.read_text()
        
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(file_content)
            except psycopg.Error as e:
                logger.error(f"Error running script {script_file.name}: {e}")
                await conn.rollback()
                raise e
        
                
    await conn.commit()
    
    return conn
