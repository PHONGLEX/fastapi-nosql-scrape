from fastapi import FastAPI
from cassandra.cqlengine.management import sync_table
from typing import List

from . import (
    db,
    config,
    models,
    schemas,
    crud
)


app = FastAPI()
settings = config.get_settings()
session = None
Product = models.Product
ProductScrapeEvent = models.ProductScrapeEvent


@app.on_event("startup")
def on_startup():
    global session
    session = db.get_session()
    sync_table(Product)
    sync_table(ProductScrapeEvent)
    
    
@app.get('/products', response_model=List[schemas.ProductListSchema])
def products_list_view():
    return list(Product.objects.all())


@app.post('/events/scrape')
def events_scrape_create_view(data: schemas.ProductListSchema):
    product, _ = crud.add_scrape_event(data.dict())
    return product


@app.get('/products/{asin}')
def products_detail_view(asin: str):
    data = dict(Product.objects.get(asin=asin))
    events = list(ProductScrapeEvent.objects().filter(asin=asin))
    events = [schemas.ProductScrapeEventDetailSchema(**x) for x in events]
    data['events'] = events
    data['events_url'] = f'/products/{asin}/events'
    return data


@app.get('/products/{asin}/events', response_model=List[schemas.ProductScrapeEventDetailSchema])
def products_scrapes_list_view(asin: str):
    return list(ProductScrapeEvent.objects().filter(asin=asin).limit(5))