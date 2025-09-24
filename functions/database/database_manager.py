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

        query = """INSERT INTO products (name, description, price, in_stock, category)
                VALUES ($1, $2, $3, $4, $5) RETURNING id, name, description, price, in_stock, category"""

        async with self._pool.acquire() as connection:
            try:
                record = await connection.fetchrow(query, product.name, product.description, product.price,
                                                   product.in_stock, product.category)
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

        query = """SELECT id, name, description, price, in_stock, category
                FROM products
                WHERE id = $1"""

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

    async def get_products(self, skip: int = 0, limit: int = 10) -> dict:
        query = """SELECT id, name, description, price, in_stock, category
                FROM products
                ORDER BY id
                OFFSET $1 LIMIT $2"""

        async with self._pool.acquire() as connection:
            try:
                records = await connection.fetch(query, skip, limit)
                if records:
                    return {"status": True, "message": "Produtos retornados com sucesso", "data": [{**record} for record in records]}
                else:
                    return {"status": True, "message": "Nenhum produto encontrado", "data": []}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno do servidor", "data": dict()}

    async def get_products_by_category(self, category:str, skip: int = 0, limit: int = 10) -> dict:
        query= """SELECT id, name, description, price, in_stock
                FROM products
                WHERE category = $1
                ORDER BY id
                OFFSET $2 LIMIT $3 """

        async with self._pool.acquire() as connection:
            try:
                records = await connection.fetch(query, category, skip, limit)
                if records:
                    return {"status": True, "message": "Produtos retornados com sucesso", "data": [{**record} for record in records]}
                else:
                    return {"status": True, "message": "Nenhum produto encontrado", "data": []}
            except Exception as e:
                logger.error(e)
                return {"status": False, "message": "Erro interno do servidor", "data": dict()}

    async def get_products_by_name(self, name: str, skip: int = 0, limit: int = 10) -> dict:
        query = """SELECT id, name, description, price, in_stock, category
                FROM products
                WHERE name ILIKE $1
                ORDER BY id
                OFFSET $2 LIMIT $3"""

        async with self._pool.acquire() as connection:
            try:
                records = await connection.fetch(query, f"%{name}%", skip, limit)
                if records:
                    return {"status": True, "message": "Produtos retornados com sucesso", "data": [{**record} for record in records]}
                else:
                    return {"status": True, "message": "Nenhum produto encontrado", "data": []}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno do servidor", "data": dict()}

    async def get_products_by_price_range(self, min_price: float, max_price: float, skip: int = 0, limit: int = 10) -> dict:
        query = """SELECT id, name, description, price, in_stock, category
                FROM products
                WHERE price BETWEEN $1 AND $2
                ORDER BY id
                OFFSET $3 LIMIT $4"""

        async with self._pool.acquire() as connection:
            try:
                records = await connection.fetch(query, min_price, max_price, skip, limit)
                if records:
                    return {"status": True, "message": "Produtos retornados com sucesso", "data": [{**record} for record in records]}
                else:
                    return {"status": True, "message": "Nenhum produto encontrado", "data": []}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno do servidor", "data": dict()}

    async def update_product(self, product_id: int, product: ProductCreate) -> dict:

        query = """UPDATE products
                   SET name        = $1,
                       description = $2,
                       price       = $3,
                       in_stock    = $4,
                       category   = $5
                   WHERE id = $6 RETURNING id, name, description, price, in_stock, category"""

        async with self._pool.acquire() as connection:
            try:
                record = await connection.fetchrow(query, product.name, product.description, product.price,
                                                   product.in_stock, product.category, product_id)
                if record:
                    return {"status": True, "message": "Produto atualizado com sucesso", "data": {**record}}
                else:
                    return {"status": False, "message": "Nenhum produto encontrado com o id informado.", "data": dict()}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno com a tabela de produtos.", "data": dict()}

    async def delete_product(self, product_id: int) -> dict:

        query = """DELETE
                FROM products
                WHERE id = $1"""

        async with self._pool.acquire() as connection:
            try:
                result = await connection.execute(query, product_id)

                if result.endswith("1"):
                    return {"status": True, "message": "Item excluído com sucesso", "data": product_id}
                else:
                    return {"status": False, "message": "Nenhum produto encontrado com o id informado.", "data": dict()}
            except Exception as e:
                logger.exception(e)
                return {"status": False, "message": "Erro interno", "data": dict()}


db_instance = DatabaseManager()
