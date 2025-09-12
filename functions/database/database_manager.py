import asyncpg
import os
from asyncpg import UniqueViolationError
from dotenv import load_dotenv
from logger.logger import logger
from entities.product.product import ProductCreate

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self._pool: asyncpg.Pool = None
        self._database_url = os.getenv("DATABASE_URL")

    async def connect(self):
        if not self._pool:
            self._pool = await asyncpg.create_pool(self._database_url)

    async def disconnect(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def create_product(self, product: ProductCreate) -> dict:

        query = """
                INSERT INTO products (name, description, price, in_stock)
                VALUES ($1, $2, $3, $4) RETURNING id, name, description, price, in_stock \
                """

        async with self._pool.acquire() as connection:
            try:
                record = await connection.fetchrow(query, product.name, product.description, product.price,
                                                   product.in_stock)
                if record:
                    return {"status": True, "message": "Produto criado com sucesso", "data":{**record}}
                else:
                    return{"status": False, "message": "Falha ao criar produto", "data": dict()}
            except UniqueViolationError as ve:
                return {"status": False, "message": "Nome repetido", "data": dict()}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno do servidor", "data": dict()}

    async def get_product(self, product_id: int) -> dict:

        query = """
                SELECT id, name, description, price, in_stock
                FROM products
                WHERE id = $1 \
                """

        async with self._pool.acquire() as connection:
            try:
                record = await connection.fetchrow(query, product_id)
                if record:
                    return {"status": True, "message": "Produto retornado com sucesso", "data":{**record}}
                else:
                    return {"status": False, "message": "Produto não encontrado", "data": dict()}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno do servidor", "data": dict()}

    async def get_products(self, filters:dict, skip: int = 0, limit: int = 10) -> dict:
        query = "SELECT id, name, description, price, in_stock FROM products"
        params = []
        index = 1

        for key, value in filters.items():
            if key == "category":
                query += f" WHERE category LIKE ${index}"
                params.append(f"%{value}%")
                index += 1
            elif key == "sortby":
                allowed_sort_fields = {"name", "price", "in_stock"}
                if value in allowed_sort_fields:
                    query += f" ORDER BY {value}"
                else:
                    query += " ORDER BY id"

        query += f" OFFSET ${index} LIMIT ${index + 1}"
        params.extend([skip, limit])

        async with self._pool.acquire() as connection:
            try:
                records = await connection.fetch(query, *params)

                if records:
                    return{"status": True, "message": "Produtos retornados com sucesso", "data": [{**row} for row in records]}
                else:
                    return {"status": True, "message": "Nenhum produto registrado", "data": list()}
            except Exception as e:
                logger.exception(e)
                return{"status": False, "message": "Erro interno do servidor", "data": list()}

    async def update_product(self, product_id: int, product: ProductCreate) -> dict:

        query = """UPDATE products
                   SET name        = $1,
                       description = $2,
                       price       = $3,
                       in_stock    = $4
                   WHERE id = $5 RETURNING id, name, description, price, in_stock"""

        async with self._pool.acquire() as connection:
            try:
                record = await connection.fetchrow(query, product.name, product.description, product.price,
                                                   product.in_stock, product_id)
                if record:
                    return {"status": True, "message": "Produto atualizado com sucesso", "data": {**record}}
                else:
                    return {"status": False, "message": "Nenhum produto encontrado com o id informado.", "data": dict()}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno com a tabela de produtos.", "data": dict()}

    async def delete_product(self, product_id: int) -> dict:

        query = """
                DELETE
                FROM products
                WHERE id = $1 \
                """

        async with self._pool.acquire() as connection:
            try:
                result = await connection.execute(query, product_id)

                if result.endswith("1"):
                    return {"status": True, "message": "Item excluído com sucesso"}
                else:
                    return {"status": False, "message": "Nenhum produto encontrado com o id informado.", "data": dict()}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno", "data": dict()}


db_instance = DatabaseManager()