from pydantic import BaseModel


class ProductBase(BaseModel):
    name: str
    description: str
    price: float
    in_stock: bool
    category: str


class ProductCreate(ProductBase):
    pass


class Product(ProductBase):
    id: int

    model_config = {
        "from_attributes": True
    }