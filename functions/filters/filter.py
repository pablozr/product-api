from typing import Tuple, List


def build_products_query(category: str = None, skip: int = 0, limit: int = 10, sortby: str = None) -> Tuple[str, List]:
    base_query = """
    SELECT id, name, description, price, in_stock
    FROM products
    """

    params = []

    if category:
        base_query += " WHERE category LIKE $1"
        params.append(f"%{category}%")

    allowed_sort_fields = {"name", "price", "in_stock"}
    if sortby in allowed_sort_fields:
        base_query += f" ORDER BY {sortby}"
    else:
        base_query += " ORDER BY id"

    base_query += f" OFFSET ${len(params) + 1} LIMIT ${len(params) + 2}"
    params.extend([skip, limit])
    return base_query, params