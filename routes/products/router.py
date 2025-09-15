from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from functions.database.database_manager import db_instance
from entities.product.product import Product, ProductCreate
from logger import logger

router = APIRouter()

@router.post("/products")
async def create_product_endpoint(product: ProductCreate):
    try:
        response_create_product = await db_instance.create_product(product)

        if not response_create_product["status"]:
            return JSONResponse(status_code=404, content={"message": response_create_product["message"]})

        return JSONResponse(status_code=201, content={"message": response_create_product["message"], "data": response_create_product["data"]})

    except Exception as e:
        logger.logger.error(e)
        return JSONResponse(status_code=500, content={"message": "Erro interno com o servidor."})


@router.get("/products/{product_id}", response_model=Product)
async def get_product_endpoint(product_id: int):
    try:
        response_get_product = await db_instance.get_product(product_id)
        if not response_get_product["status"]:
            return JSONResponse(status_code=404, content={"message": response_get_product["message"]})

        return JSONResponse(status_code=200, content=response_get_product["data"])

    except Exception as e:
        logger.logger.error(e)
        return JSONResponse(status_code=500, content={"message": "Erro interno com o servidor."})

@router.get("/products")
async def get_products_endpoint(skip: int = 0, limit: int = 10, category: Optional[str] = None):
    if category:
        try:
            response_get_products_by_category = await db_instance.get_products_by_category(category=category, skip=skip, limit=limit)

            if not response_get_products_by_category["status"]:
                raise HTTPException(status_code=404, detail=response_get_products_by_category["message"])
            return JSONResponse(status_code=200, content={"message": response_get_products_by_category["message"],"data": response_get_products_by_category["data"]})
        except Exception as e:
            logger.logger.error(e)
            return JSONResponse(status_code=500, content={"message": "Erro interno com o servidor."})
    else:
        try:
            response_get_products = await db_instance.get_products(skip=skip, limit=limit)

            if not response_get_products["status"]:
                raise HTTPException(status_code=404, detail=response_get_products["message"])
            return JSONResponse(status_code=200, content={"message": response_get_products["message"],"data": response_get_products["data"]})
        except Exception as e:
            logger.logger.error(e)
            return JSONResponse(status_code=500, content={"message": "Erro interno com o servidor."})

@router.put("/products/{product_id}")
async def update_product_endpoint(product_id: int, product: ProductCreate):
    try:
        response_updated_product = await db_instance.update_product(product_id, product)

        if not response_updated_product["status"]:
            raise HTTPException(status_code=404, detail=response_updated_product["message"])

        return JSONResponse(status_code=200, content={"message": response_updated_product["message"],
                                                      "data": response_updated_product["data"]})
    except Exception as e:
        logger.logger.error(e)
        return JSONResponse(status_code=500, content={"message": "Erro interno com o servidor."})

@router.delete("/products/{product_id}")
async def delete_product_endpoint(product_id: int):
    try:
        success = await db_instance.delete_product(product_id)
        if not success["status"]:
            return JSONResponse(status_code=404, content={"message": success["message"]})

        return JSONResponse(status_code=200, content={"message": success["message"],})

    except Exception as e:
        logger.logger.error(e)
        return JSONResponse(status_code=500, content={"message": "Erro interno com o servidor."})