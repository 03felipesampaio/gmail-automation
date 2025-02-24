import psycopg_pool
import logging

logger = logging.getLogger("gmail_automation")


async def get_existing_parameters(pool: psycopg_pool.AsyncConnectionPool, action_name: str) -> list[dict]:
    """Get existing parameters for the action from the database."""
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            query = """
            SELECT *
            FROM action_parameter_templates
            WHERE action_name = %s
            """
            await cursor.execute(query, (action_name,))
            existing_parameters = await cursor.fetchall()
            return existing_parameters


async def add_action(
    pool: psycopg_pool.AsyncConnectionPool,
    action: dict
) -> dict:
    """Add action to the database."""
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            query = "SELECT * FROM action_templates WHERE action_name = %s"
            await cursor.execute(query, (action["action_name"],))
            action_returned = await cursor.fetchone()

            if action_returned:
                logger.warning(f"Action with name {action['action_name']} already exists in the database.")
                return action_returned
            else:
                query = """
                INSERT INTO action_templates (action_name, format)
                VALUES (%s, %s)
                RETURNING *
                """
                await cursor.execute(query, (
                    action["action_name"],
                    action["format"],
                ))
                action_created = await cursor.fetchone()

                return action_created


async def add_action_parameters(
    pool: psycopg_pool.AsyncConnectionPool,
    action_name: str,
    parameters: list[dict]
) -> list:
    """Add parameters to the action."""
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            existing_parameters = await get_existing_parameters(pool, action_name)
            
            returned_parameters = []
            for param in parameters:
                existing_parameter = next((existing_param for existing_param in existing_parameters if existing_param["parameter_name"] == param["parameter_name"]), None)
                
                if existing_parameter:
                    existing_parameter_copy = existing_parameter.copy()
                    logger.debug(f"Parameter with name '{param['parameter_name']}' already exists for action '{action_name}'.")
                    returned_parameters.append(existing_parameter)
                    continue

                query = """
                INSERT INTO action_parameter_templates (action_name, parameter_name, parameter_type, parameter_is_nullable, parameter_default)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *
                """
                    
                await cursor.execute(query, (action_name, param["parameter_name"], str(param["parameter_type"]), param["parameter_is_nullable"], str(param["parameter_default"])))
                inserted_param = await cursor.fetchone()
                returned_parameters.append(inserted_param)

            return returned_parameters
