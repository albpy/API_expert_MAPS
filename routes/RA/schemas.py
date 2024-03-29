from pydantic import BaseModel
from typing import List,Union

class DateFromTo(BaseModel):
    fro : str
    to  : str


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
    article_score: List[str]
   
    

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
    # article_score       : list
    secondary_filter    : SecondaryFilter




#input credential class
class LoginData(BaseModel):
    mail: str
    name: str

# class users_tables():

