import asyncpg
import os
from typing import Optional, List, Dict

from asyncpg import UniqueViolationError
from dotenv import load_dotenv

from logger import logger
from entities.product.product import Product
from entities.product.product import ProductCreate
from functions.filters.filter import build_products_query


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
        async with self._pool.acquire() as connection:


            query = """
                    INSERT INTO products (name, description, price, in_stock)
                    VALUES ($1, $2, $3, $4) RETURNING id, name, description, price, in_stock \
                    """
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
                logger.logger.error(e)
                return {"status": False, "message": "Erro interno do servidor", "data": dict()}

    async def get_product(self, product_id: int) -> dict:
        async with self._pool.acquire() as connection:
            query = """
                    SELECT id, name, description, price, in_stock
                    FROM products
                    WHERE id = $1 \
                    """
            try:
                record = await connection.fetchrow(query, product_id)
                if record:
                    return {"status": True, "message": "Produto criado com sucesso", "data":{**record}}
                else:
                    return {"status": False, "message": "Falha ao criar produto", "data": dict()}
            except Exception as e:
                logger.logger.error(e)
                return {"status": False, "message": "Erro interno do servidor", "data": dict()}

    async def get_products(self, skip: int = 0, limit: int = 10, category: str = None, sortby: str = None) -> dict:
        async with self._pool.acquire() as connection:
            query, params = build_products_query(category, skip, limit, sortby)
            try:
                records = await connection.fetch(query, *params)

                if records:
                    return{"status": True, "message": "Produtos retornados com sucesso", "data": [{**row} for row in records]}
                else:
                    return {"status": True, "message": "Nenhum produto registrado", "data": list()}
            except Exception as e:
                logger.logger.error(e)
                return{"status": False, "message": "Erro interno do servidor", "data": list()}

    async def update_product(self, product_id: int, product: ProductCreate) -> dict:
        async with self._pool.acquire() as connection:
            query = """UPDATE products
                    SET name        = $1, 
                        description = $2, 
                        price       = $3, 
                        in_stock    = $4
                    WHERE id = $5 RETURNING id, name, description, price, in_stock"""

            try:
                record = await connection.fetchrow(query, product.name, product.description, product.price,
                                                   product.in_stock, product_id)
                if record:
                    return {"status": True, "message": "Produto atualizado com sucesso", "data": {**record}}
                else:
                    return {"status": False, "message": "Nenhum produto encontrado com o id informado.", "data": dict()}
            except Exception as e:
                # logger.excpetion(e)
                logger.logger.error(e)
                return {"status": False, "message": "Erro interno com a tabela de produtos.", "data": dict()}

    async def delete_product(self, product_id: int) -> dict:
        async with self._pool.acquire() as connection:
            query = """
                    DELETE \
                    FROM products
                    WHERE id = $1 \
                    """
            try:
                result = await connection.execute(query, product_id)

                if result.endswith("1"):
                    return {"status": True, "message": "Item Excluido com sucesso"}
                else:
                    return {"status": False, "message": "Nenhum produto encontrado com o id informado.", "data": dict()}
            except Exception as e:
                logger.logger.error(e)
                return {"status": False, "message": "Erro interno", "data": dict()}

db_instance = DatabaseManager()