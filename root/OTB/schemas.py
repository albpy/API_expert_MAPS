from pydantic import BaseModel
from typing import List,Union,Dict

class DateFromTo(BaseModel):
    fro : str
    to  : str


# class SecondaryFilter(BaseModel):
#     budget_year: List[Union[int,str]]
#     historical_year: List[Union[int,str]]
#     store: List[str]
#     city: List[str]
#     country: List[str]
#     region: List[str]
#     season: List[str]
#     month: List[str]
#     week: List[str]
#     date: List[str]
#     channel: List[str]

class SecondaryFilter(BaseModel):
    
    HistoricalYear: List[Union[int,str]] 
    history_dates: List[str]
    history_Quarter:List[str]
    history_month: List[str]
    history_week: List[str]
    history_Day: List[str]
    BudgetYear: List[Union[int,str]] 
    BudgetDate: List[str]
    Quarter: List[str]
    month: List[str]
    week: List[str]
    Day: List[str]
    region: List[str]
    country: List[str]
    city: List[str]
    Store_Name: List[str]  
    season: List[str]
    Channel: List[str]
    article_score : List[str]

class Echelons(BaseModel):

    family                : List[str]
    sub_family            : List[str]
    supplier              : List[str]
    category              : List[str]
    dom_comm              : List[str]
    sub_category          : List[str]
    extended_sub_category : List[str]
    sub_category_supplier : List[str]

class Filters(BaseModel):
    page_size      : int
    page_number    : int
    history_date_range  : DateFromTo
    forecast_date_range : DateFromTo
    sales_channel       : list
    product_family      : list
    sub_families        : list
    category            : list
    sub_category        : list
    suppliers           : list
    sku                 : list
    top_items           : list
    store_class         : list 
    secondary_filter: SecondaryFilter



#input credential class
class LoginData(BaseModel):
    mail: str
    name: str
   
# ra_kpi_joined = ra_kpi_joined.with_columns((pl.when(ra_kpi_joined['Budget_date']==datetime(2024,3,27,0,0))
#                                             .then(datetime(2024,4,3,00,00,00))
#                                             .otherwise(datetime(2024,4,4,00,00,00))).alias('Budget_date'))