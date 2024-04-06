import pandas as pd
import numpy as np
import json
import polars as pl

from fastapi import APIRouter, Depends, WebSocket
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session, aliased

from core.database import  get_db
from .schemas import  Filters, DateFromTo,LoginData, Echelons
from .parameters import OTB
from decimal import Decimal
from .operations import Operations
from .kpi_analysis import kp_Operations
# from .query import otb_query, filter_details, change_percent, change_value
from sqlalchemy import text

import traceback
from otb_calc import create_ra, form_base_data, inititalize_columns
from rapidframes import QueryBuilder, RapiDF

import time
otb =APIRouter(prefix='/otb')                               # session = Session()
OTB = OTB()     
Operations = Operations()
kp_operations = kp_Operations()

# import sys, os
# sys.stdout = open(os.devnull, 'w')
# Revising stores
            #older data store
updation_data = pl.DataFrame
want_to_update_new_otb_mix = {'update' : updation_data}

revised_budget_to_update = pl.DataFrame
new_revised = {'revised_budget' :  revised_budget_to_update} # revised_budget

do_revise = None
do_revise_dict = {'do_revise':do_revise} # to do revice 
message = ''
gp = None
current_group_kpi = {'revising_group' : gp}

data_store = {}

max_col     = OTB.MAX_COLS
avg_col     = OTB.AVG_COLS
sum_col     = OTB.SUM_COLS
float_cols  = OTB.FLOAT_COLS
int_cols    = OTB.INT_COLS
HEIRARCHY   = OTB.HEIRARCHY
percent_col = OTB.PERCENT_COLS
tabs        = json.dumps(OTB.TABS)
arts        = OTB.SCORES
rank_col    = OTB.RANK_COLS

DATA = pl.DataFrame()
TEMP = {'key':DATA}


drill_down_cor = OTB.drill_down_cor
drill_down_display = OTB.drill_down_display
get_session_id = OTB.get_session_id
update_users_tables = OTB.update_users_tables
save_table = OTB.save_table

@otb.get('/sub_filters')
async def sub_filters():
    return JSONResponse(content = OTB.SUB_FILTER)

@otb.post('/save')
async def save_data(login_data:LoginData,db: Session = Depends(get_db)):
    data = TEMP['key']
    wr=False
    # data = pl.from_pandas(data)
    print(type(data),'cdcd')
    data = OTB.calculate_df(data,wr)   
    # module_id = 'otb_table'
    module_id = 'otb_table'
    table_name = module_id
    OTB.save_table_to_db(db,data,table_name)
    return JSONResponse(content={'message':'done'})

# Whether user need to update the revised  
@otb.post("/update_otb_amount")
async def user_want_revised_amount(update:bool=True):

    data_ = want_to_update_new_otb_mix['update']
    print(data_, 'The Data')
    print(group, 'the group in updation')
    existing_child = data_['Check_box'] == 1
    if list(existing_child).count(True) == 1:
        revised_budget_to_update = data_.filter(list(existing_child))['revised_budget_amount'][0]
        new_revised['revised_budget'] = revised_budget_to_update
        # We set a flag to indicate weather the 'revised_budget_to_update' is need to be updated
        do_revise = True
        do_revise_dict['do_revise'] = do_revise
    elif list(existing_child).count(True) > 1:
        columns_to_select = ['revised_budget_amount', 'budget_amount'] + group
        revised_budget_to_update = data_.filter(list(existing_child)).select(pl.col(['revised_budget_amount', group[0], 'budget_amount']))
        # .select(pl.col(['revised_budget_amount', group[0], 'budget_amount']))
        new_revised['revised_budget'] = revised_budget_to_update
        # Also the flag works here
        do_revise = True
        do_revise_dict['do_revise'] = do_revise
    else:
        do_revise = False
        do_revise_dict['do_revise'] = do_revise
    
    return{
        message : 'new otb mix updated'
    }

@otb.websocket('/get_data_ws')
async def get_data_ws(websocket: WebSocket,  db: Session = Depends(get_db)):
    DATA = OTB.DATA
    await websocket.accept()
    while True:
        data_filter = await websocket.receive_json()
        print(data_filter,'DFD')
        filters =Filters(**data_filter)
        print(filters)
        secondary = data_filter['secondary_filter']
        group_by_id = Echelons(**secondary)

        secondary_filter = data_filter['secondary_filter']
        if data_filter['fetch_from_db']:
            KPI_DICT = {'kpi_selection_channel' : [], 'kpi_selection_value' : []}
            DATA = create_ra(filters.forecast_date_range.fro,filters.forecast_date_range.to,filters.history_date_range.fro,filters.history_date_range.to, filters)
            DATA = form_base_data(DATA, filters)
            # print(round(DATA.estimated_size('mb'),2)," MB memory size of data step3")
            # print(round(DATA.estimated_size('gb'),2)," GB memory size of data step3")

            DATA = OTB.initial_frame_calculation(DATA)
            print(round(DATA.estimated_size('mb'),2)," MB memory size of data step4")
            print(round(DATA.estimated_size('gb'),2)," GB memory size of data step4")
            data = DATA
            # Item_ser = data['ITEMID']
            do_revise_dict['do_revise'] = False
            revised_budget_to_update = None
        global group
        group  = []
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        DATA = DATA.with_columns(order_index=pl.lit(0))
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        sub_filter_state = False
        filter_condition = None
        filter_condition,sub_filter_state,group = OTB.secondary_filter(DATA, filters, sub_filter_state,group, filter_condition)
        
        if filter_condition is not None:
            DATA = DATA.with_columns(filter_index=filter_condition)
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))

        if not sub_filter_state == True:
            group = []
            filter_condition = None
            sub_filter_state = False
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`    
        if secondary_filter['article_score'] != []:
            DATA = OTB.call_kpi(DATA, data_filter)
            # print('there is article score')
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        if data_filter['table_changes'] != {}:
            row = data_filter['table_changes']['row']
            current_tire = list(row)[0]
            columnID = data_filter['table_changes']['columnId']
            newValue = data_filter['table_changes']['newValue']
            print(columnID, newValue, 'colid and new value')
            # prepare filters for editing.
            print(DATA.columns)
            child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA = OTB.table_change_filter(group, HEIRARCHY, data_filter, DATA,
                        row, filter_condition)
            if filter_condition is not None:
            # Add filter_index column based on filter_condition.
                DATA = DATA.with_columns(filter_index=filter_condition)
                # DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))
            # time.sleep(5)
            DATA,data = Operations.edit_tables(DATA,data,row,group,newValue,columnID,columns_to_filter,sub_filter_state,
                    parent,child,other_filter_condition,filter_condition)
     
            data_kpi = DATA.clone()
            if columnID == 'Check_box':
                print('column_id is check_box')
                row = data_filter["table_changes"]["row"]
                child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA = OTB.table_change_filter(group, HEIRARCHY, data_filter, DATA,
                        row, filter_condition)
                if child is None: #and parent == None:
                    DATA = kp_operations.apply_kpi_for_main_data(DATA, newValue, row)
                if child is not None:
                    DATA = kp_operations.calculate_revised_budget(DATA, child, other_filter_condition, DATA, newValue)
        
        if data_filter['group_by']['status']:
            
            print('df_groupby_stats')
            print(type(DATA), 'data_cols_g')
            print(data_filter['group_by']['columns'], 'g_cols' )
            filter_condition = None
            filter_condition,sub_filter_state,group = OTB.apply_heirarchial_filters(DATA, group_by_id,
                                sub_filter_state, group,filter_condition)
            data, filter_condition = Operations.apply_group_by(DATA, data, data_filter, sub_filter_state, group, filters, filter_condition)
            
            print(DATA.columns, 'in group_by___')
            
        if data_filter['expand']['status']: # Function to expand the channel
            filter_condition = None
            filter_condition,sub_filter_state,group = OTB.apply_heirarchial_filters(DATA, group_by_id,
                                sub_filter_state, group,filter_condition)
            data,filter_condition,group = Operations.expand_hierarchy(DATA,data_filter,sub_filter_state,group,filters,filter_condition) 
            

        try:
            wr=False
            print(data.columns, 'aggregation bef')

            data = OTB.calculate_df(data,wr)
# @@@        
            print(data.select(pl.col(['budget_amount', 'SupplyCost', 'FirstMargin%', 'act_forecast_vs_budget_percent'])), 'the data after agg')

            print(group, 'group from processes')

            if data_filter["group_by"]["status"] == True or (data_filter["expand"]["status"] == True and data_filter['table_changes'] == {}) or filters.sku !=[]:
                data_kpi = data.clone()

            if data_filter['table_changes']!={}:  
                row = data_filter["table_changes"]["row"]
                columnID = data_filter["table_changes"]["columnId"]
                newValue = data_filter["table_changes"]["newValue"]
                print(row, 'checking row for passed revised')
                if columnID == 'Check_box':
                    newValue_kpi = newValue
                    # child, group = kp_operations.kpi_inner_child_selection(data, data_filter, group, HEIRARCHY)
                    data = OTB.call_kpi(data, data_filter)
                    child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA = OTB.table_change_filter(group, HEIRARCHY, data_filter, data,
                        row, filter_condition)
#                   Include main group
                    if child is not None:
                        if newValue_kpi == 1:                
                            # if scores_m != []:
                            data = kp_operations.calculate_revised_budget(data, child, other_filter_condition, data_kpi, row)
                        if newValue_kpi == 0:
                            # if scores_m != []:
                            data = kp_operations.user_uncheck_kpi_selection(data, child, newValue_kpi)
            if data_filter['select_all_kpi'] == True and 'coefficient_score_mix_percent' in data.columns:
                print('select all kpi true')
                # The child will be of the condition to select full and apply the integration totaly
                
                row = None
                data_kpi = data.clone()
                child, group = kp_operations.kpi_inner_child_selection(data, data_filter, group, HEIRARCHY)
                print(group, 'group_in_select_all_kpi')
                
                data = data.with_columns(Check_box = pl.lit(1).cast(pl.Int8))
                data = OTB.call_kpi(data, data_filter)
                child,other_filter_condition,filter_condition,parent,columns_to_filter,values_to_filter,group,DATA = OTB.table_change_filter(group, HEIRARCHY, data_filter, data,
                        row, filter_condition)
                data = kp_operations.calculate_revised_budget(data, child, other_filter_condition, data_kpi, row)
                if data_filter['expand']['status'] == True:
                    newValue = 1
                    DATA = kp_operations.apply_kpi_for_main_data(DATA, newValue, row)
            elif data_filter['select_all_kpi'] == False and data_filter['table_changes']=={}:
                
                child, group = kp_operations.kpi_inner_child_selection(data, data_filter, group, HEIRARCHY)
                # child = ~(child)
                data = data.with_columns(Check_box = pl.lit(0).cast(pl.Int8))
                data = OTB.call_kpi(data, data_filter)
                newValue_kpi = 0
                data = kp_operations.user_uncheck_kpi_selection(data, child, newValue_kpi)
            # Here we store our data we need to update
            if 'revised_budget_amount' in data.columns:
                want_to_update_new_otb_mix['update'] = data
            # And we get do_revise flag from /update_otb_amount
            # r=1
            # if r==1:
            # else:
            #     print(traceback.format_exc(), 'error in do revose dict')
            #!!!!!!!!!!!
            if do_revise_dict['do_revise'] == True:
                if type(new_revised['revised_budget']) != type(pl.DataFrame({'check' : [1,2,3]})):
                    ida_of_otb_mix = data.find_idx_by_name('budget_amount')
                    # data.replace('budget_amount', pl.Series([new_revised['revised_budget']]*len(data)) * (data['otb_percent']/100))
                    data = data.with_columns(budget_amount = (data['budget_percent'] * new_revised['revised_budget'])/100)
                    # DATA = DATA.with_columns(budget_amount = (DATA["otb_percent"] * new_revised['revised_budget'])/100)
                    filter_condition,sub_filter_state,group = OTB.secondary_filter(DATA, filters, sub_filter_state,group, filter_condition)
                    DATA = kp_operations.destribute_otb_(new_revised['revised_budget'], DATA, group, data_filter, HEIRARCHY, sub_filter_state)
                    do_revise_dict['do_revise'] = False
                elif type(new_revised['revised_budget']) == type(pl.DataFrame({'check' : [1,2,3]})):
                    row = data_filter["expand"]['row']
                    ida_of_otb_mix = data.find_idx_by_name('budget_amount')
                    #------------------------need to 
                    # xmn = new_revised['revised_budget'].filter(new_revised['revised_budget'][list(row)[0]] == row[list(row)[0]])['revised_budget_amount']
                    # print(xmn, 'user_selected_budget_amount_that_cant_be_get_item')
                    user_selected_rows_ = new_revised['revised_budget'].filter(new_revised['revised_budget'][list(row)[0]] == row[list(row)[0]])['revised_budget_amount']
                    if len(user_selected_rows_) > 1: 
                        user_selected_rows_[0].item()
                    else:
                        user_selected_rows_.item()
                    print(user_selected_rows_, 'user_selected_rows_')
                    print(data.select(['budget_amount' ,'budget_percent']))
                    
                    data = data.with_columns(budget_amount = (data['budget_percent'] * user_selected_rows_)/100)
                    #------------------------need to
                    print(data)
                    filter_condition,sub_filter_state,group = OTB.secondary_filter(DATA, filters, sub_filter_state,group, filter_condition)
                    DATA = kp_operations.destribute_otb_total(new_revised['revised_budget'], DATA, group, data_filter, HEIRARCHY, sub_filter_state)
                    do_revise_dict['do_revise'] = False
                                   
            else:
                pass

            secondary = data_filter['secondary_filter']
            scores_m = secondary['article_score']
            art_cols = [f"pl.col('{col}').sum()" for a, col in arts.items() if a in scores_m]
            agg_dict = [
                eval(expr) for expr in 
                [f"pl.col('{col}').mean()" for col in avg_col if col in data.columns] + 
                [f"pl.col('{col}').sum()" for col in sum_col if col in data.columns] +
                [f"pl.col('{col}').max()" for col in rank_col] +
                [f"pl.col('{col}').sum()" for col in ['new_budget_mix', 'revised_budget_amount'] if col in data.columns] +
                [f"pl.col('{col}').mean()" for a, col in arts.items() if a in scores_m] +
                [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0] +
                [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
                #+ [f"pl.col('total_squ_count').n_unique()"]
            ]
            bottom_column = data.select(agg_dict)
            if type(bottom_column) != dict:
                bottom_column = bottom_column.to_dict()
                bottom_column = {key:bottom_column[key][0] for key in list(bottom_column.keys())}
            bottom_column = pd.Series(bottom_column)
            bottom_column[int_cols]    = bottom_column[int_cols].fillna(0).astype(int)
            # bottom_column[int_cols]    = bottom_column[int_cols].fill_nan(0).cast(pl.Int64)
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
            # bottom_column[float_cols]  = bottom_column[float_cols].astype(float).round(2)
            bottom_column[float_cols]  = bottom_column[float_cols].astype(float)
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\//\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
            # bottom_column = bottom_column.to_pandas()
        except Exception as e:
            print(traceback.format_exc())
            print(f'error is {e}')
            print('We are conjugating data to bottom column because of an exception')
            bottom_column = data

        TEMP['key'] = DATA
        size = len(data)
        if size == 1:
            editable_cols = json.dumps(['Logistic%', 'DisplayItemQty', 'COR_EOLStock_value', 'Markdown%', 'ProposedSellThru%', 'DisplayItemValue'])
        else:
            editable_cols = json.dumps(OTB.EDITABLE_COLS)
        #print(DATA['PurchaseRetailValueatGrossSale'].sum(),'PURDATAlast')
        if 'sort' in data_filter:
            datas = Operations.sort_and_clean(data_filter, data, filters)
            sel_all_kpi = data_filter['select_all_kpi']
            if sel_all_kpi == '':
                sel_all_kpi = False
            else:
                sel_all_kpi

            # print(datas.to_json(orient='split'))           
            data_json = f"""{datas.to_json(orient='split')[:-1]}, "select_all_kpi":{json.dumps(sel_all_kpi)},"editable_cols":{editable_cols}, "percent_col":{json.dumps(percent_col)},"tabs":{tabs} ,"items":{size},"total":{bottom_column.to_json()} {datas.to_json(orient='split')[-1]}"""
            await websocket.send_text(data_json)

"""Size calculation in polars"""

# print(DATA.columns, 'of create_ra')
# print(round(DATA.estimated_size('mb'),2)," MB memory size of data step2")
# print(round(DATA.estimated_size('gb'),2)," GB memory size of data step2")

"""Function saves to db"""

# @otb.post("/save")
# async def save_data(login_data:LoginData):
#     df = TEMP['key']
#     table_name = 'otb_table'
#     OTB.save_table(df,table_name,'RA_DATA')
#     return JSONResponse(content={"message":"done"})