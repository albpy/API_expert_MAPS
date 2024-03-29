import pandas as pd
import json
import numpy as np
import polars as pl
import os

import traceback




from fastapi import APIRouter, Depends, WebSocket
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text


from core.database import  get_db
from .schemas import  Filters,LoginData
from .parameters import Parameters,Budget
from .Operations import Operations
from ra_calc import create_ra,form_base_data,inititalize_columns
from rapidframes import QueryBuilder, RapiDF

# openpyxl
# xlrd



range = APIRouter(prefix="/range")      


gloabal_vars = Parameters()
Budget = Budget()
Operation = Operations()
tabs = json.dumps(gloabal_vars.tabs)
editable_cols = json.dumps(gloabal_vars.editable_cols)
heirarchy = gloabal_vars.heirarchy




DATA = pd.DataFrame()
ds = pd.DataFrame()

TEMP = {'key':DATA}
channel_flag = False


@range.get("/sub_filters")
async def sub_filters():
    return JSONResponse(content=Budget.SUB_FILTER)


@range.post("/save")
async def save_data(login_data:LoginData,db: Session = Depends(get_db)):
    df = TEMP['key']
    df = Budget.calculate_initial_frame(df,['ITEMID','Channel','INVENTLOCATIONID','Budget_date'])
    table_name = 'ra_table'
    Budget.save_to_db(df,table_name,db)

    return JSONResponse(content={"message":"done"})

    


@range.websocket("/get_data_ws")
async def get_data_ws(websocket: WebSocket,  db: Session = Depends(get_db)):
    DATA = Budget.DATA
    await websocket.accept()
    while True:
        data_filter = await websocket.receive_json()
        print(data_filter)
        filters = Filters(**data_filter)
        # print(filters,"fils")

        # fetch data from rapidframe and do initial calculation
        if data_filter["fetch_from_db"]:
            DATA = create_ra(filters)
            DATA = form_base_data(DATA)
            DATA = inititalize_columns(DATA,gloabal_vars)
            DATA,ds = Budget.initial_frame_calculation(DATA)
            data = DATA

        group = []
        sub_filter_state = False
        filter_condition = None 
        TEMP['key'] = DATA

        # prepare sub filtrers 
        filter_condition,sub_filter_state,group = Budget.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)
        if filter_condition is not None:
            DATA = DATA.with_columns(filter_index=filter_condition)
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))

        # calculate kpi coefficinet score
        if filters.secondary_filter.article_score != []:
            DATA = Budget.call_kpi(DATA,filters.secondary_filter.article_score)

        # performe operations if column has chanages
        if data_filter['table_changes'] != {}:
            row = data_filter["table_changes"]["row"]
            columnID = data_filter["table_changes"]["columnId"]
            newValue = data_filter["table_changes"]["newValue"]

            # prepare filters for editing
            child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA = Budget.table_change_filter(group,heirarchy,data_filter,DATA,
                        row,filter_condition)

            # Handle KPI checkbox
            if columnID == 'Check_box':
                DATA = Operation.manage_check_box_selection(DATA,columnID,newValue,data_filter,parent,child,other_filter_condition,filter_condition)
            else:
                # change column values using filters and newvalue
                DATA,data = Operation.edit_tables(DATA,data,row,group,newValue,columnID,columns_to_filter,sub_filter_state,
                        parent,child,other_filter_condition,filter_condition)

        # handle user input option
        if data_filter["input_field"]["status"]:
            DATA = Operation.user_input(data_filter=data_filter,DATA=DATA)

        # group the data based on certain condition
        if data_filter["group_by"]['status'] and (data_filter['table_changes'] == {} or sub_filter_state == True):
            print("here")
            data,filter_condition = Operation.apply_group_by(DATA,data,data_filter,sub_filter_state,group,filters,filter_condition)
        
        # perform expand/contract operations
        if data_filter["expand"]['status']:
            DATA,data,filter_condition = Operation.expand_hierarchy(DATA,data_filter,sub_filter_state,group,filters,filter_condition) 

        # do calculations on aggreagted dataframe
        try: 
            if data_filter['tab_name'] == 'raBudgetValue':
                print("here2")
                data,channel_flag,dtemp = Budget.second_grouping(data,ds,DATA.clone())
                print("here3")
                data = Budget.calculate_df(data)
            if data_filter['tab_name'] == 'raBudgetMargin':
                data = Budget.calculate_margin(data)
            if data_filter['tab_name'] == 'raBudgetQuantity':
                data = Budget.calculate_quantity(data)
            if data_filter['tab_name'] == 'raBudgetCost':
                data = Budget.calculate_cost(data)
            
            # calculate total row
            bottom_column,dtemp,channel_flag = Operation.find_total_row(data,dtemp,channel_flag,data_filter)

        except Exception as e:
            print(traceback.format_exc())
            bottom_column = data

        #store edited main daa to the variable ,used for saving functionality
        TEMP['key'] = DATA
 
        # handle editable column display
        size = len(data)
        if size == 1:
            editable_cols = json.dumps(["budget_amount","budget_vpy","budget_vppy"])
        else:
            editable_cols = json.dumps(gloabal_vars.editable_cols)
        
        if "sort" in data_filter:
            # order the rows
            if "order_index" in data.columns:
                data = data.sort(by="order_index")

            # preparing final display data
            datas = Operation.sort_and_clean(data_filter,data,filters)
            bottom_column = bottom_column.drop(columns=["temp_purchase","temp_sold"])
            data_json = f"""{datas.to_json(orient='split')[:-1]},"editable_cols":{editable_cols},"percent_col":{json.dumps(gloabal_vars.percent_cols)},"tabs":{tabs} ,"items":{size},"total":{bottom_column.to_json()} {datas.to_json(orient='split')[-1]}"""

            await websocket.send_text(data_json)


            