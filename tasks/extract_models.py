import pydantic

class ReviewModel(pydantic.BaseModel):

    product_id: str = pydantic.Field(regex='^[A-Z0-9]+$') # alphanumeric
    review_id: str = pydantic.Field(regex='^[A-Z0-9]+$') # alphanumeric
    name: str = pydantic.Field(regex='[^|]*') # no '|' delimineter
    rating: int = pydantic.Field(..., ge=1, le=5) # ints 1-5 inclusive
    title: str = pydantic.Field(regex='[^|]*') # no '|' delimineter
    location: str = pydantic.Field(regex='^[A-Za-z ]+$') # alphabetical or spaces
    date: str = pydantic.Field(regex='^[0-9]{8}$') # 8 digits
    other: str = pydantic.Field(regex='[^|]*') # no '|' delimineter
    verified: bool
    body: str = pydantic.Field(regex='[^|]*') # no '|' delimineter
