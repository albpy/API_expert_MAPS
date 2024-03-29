
import pandas as pd
import json
import numpy as np
import polars as pl
import os

import traceback
import subprocess
import time

from fastapi import APIRouter, Depends, WebSocket
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text

from core.database import  get_db
from .schemas import  Filters,LoginData
from .parameters import Parameters,Otb
from ap_calc import create_ra,form_base_data,inititalize_columns
from rapidframes import QueryBuilder, RapiDF

from .schemas import  Filters, DateFromTo,LoginData,Echelons

from .parameters import Parameters,Otb

from .Operations import Operations

from .kpi_analysis import kp_Operations

from .OTBDistribution import OTBDistribute

Operations = Operations()
kp_Operations = kp_Operations()
OTBDistribute = OTBDistribute()

session = Session()
aplan = APIRouter(prefix="/aplan")  

updation_data = pl.DataFrame
want_to_update_new_otb_mix = {'update' : updation_data}

do_revise = None
do_revise_dict = {'do_revise' : do_revise}
revised_otb_to_update = pl.DataFrame
new_revised = {'revised_otb' :  revised_otb_to_update}


DATA = pd.DataFrame()
DATA_2 = pd.DataFrame()
ds = pd.DataFrame()
Channel_flag = False
TEMP = {'key':DATA}
message = '' 

gloabal_vars = Parameters()
Otb = Otb()



change_percent = Otb.change_percent
change_value = Otb.change_value
save_table = Otb.save_table_to_db
tabs = json.dumps(gloabal_vars.tabs)
drill_down = Otb.drill_down
heirarchy = gloabal_vars.heirarchy
columns = gloabal_vars.columns 
max_col = gloabal_vars.max_col
sum_col = gloabal_vars.sum_col
avg_col = gloabal_vars.avg_col
int_cols = gloabal_vars.int_cols
float_cols = gloabal_vars.float_cols
rank_col = gloabal_vars.rank_col
kpi_col = gloabal_vars.kpi_col
art = gloabal_vars.SCORES
hier_ap = gloabal_vars.hier_ap

cut_cols =[]





@aplan.post("/save")
async def save_data(login_data:LoginData,db: Session = Depends(get_db)):
    print(23)
    df = TEMP['key']
    print(type(df),"SAVEDF")
    ds=pd.DataFrame
    Channel_flag = False

    data,ds,Channel_flag = Otb.calculate_df(df,ds,Channel_flag)
    print(type(data),"SAVEDF1")
    module_id = 'ap_table'
    table_name = module_id
    Otb.save_table_to_db(data,db,table_name)

    return JSONResponse(content={"message":"done"})


@aplan.post("/update_otb_amount")
async def user_want_revised_amount(update:bool = True):
    # 1180
    # print(single_row_flag, 'values of single row flag')
    data_ = want_to_update_new_otb_mix['update']
    print(data_.columns, 'in update otb amount')
    print(data_, 'The Data')
    print(group, 'the group in updation')
    existing_child = data_['check_kpi'] == 1
    if list(existing_child).count(True) == 1:
        revised_otb_to_update = data_.filter(list(existing_child))['revised_otb_amount'][0]
        new_revised['revised_otb'] = revised_otb_to_update
        # We set a flag to indicate weather the 'revised_otb_to_update' is revised
        do_revise = True
        do_revise_dict['do_revise'] = do_revise
        # Operations.save_do_revise_flag(do_revise)
        print(do_revise, 'do_revise in updateotbmix_flag =true')
    elif list(existing_child).count(True) > 1:
        print(data_.columns, 'columns_of_data_to_transfer len > 1')
        # bulks of primals
        # we got serious
        revised_otb_to_update = data_.filter(list(existing_child)).select(pl.col(['revised_otb_amount', group[0], 'otb_amount']))
        new_revised['revised_otb'] = revised_otb_to_update        
        # Also the flag works here
        do_revise = True
        # Operations.save_do_revise_flag(do_revise)
        do_revise_dict['do_revise'] = do_revise
        print(do_revise, 'do_revise in updateotbmix_flag =false')

    else:
        print(data_.columns, 'columns_of_data_to_transfer else')
        do_revise = False
        do_revise_dict['do_revise'] = do_revise

    return{
        message : 'new otb mix updated'
    }

@aplan.get("/sub_filters")
async def sub_filters():
    return JSONResponse(content=Otb.SUB_FILTER)

@aplan.websocket("/get_data_ws")
async def get_data_ws(websocket: WebSocket,  db: Session = Depends(get_db)):
    print(24)
    DATA = Otb.DATA
    item_flag = False
    
    await websocket.accept()
    while True:
        data_filter = await websocket.receive_json()
        filters = Filters(**data_filter)
        # print(data_filter,"DATAFILTER")
        #print(filters)
        secondary = data_filter['secondary_filter']
        group_by_id = Echelons(**secondary)
        print(group_by_id, 'Itemwise grouping')
        scores_m = secondary['article_score']               

        
        if data_filter["fetch_from_db"]:
            # append and remove user selected itemcode
            SELECTION_DICT = {"item_code":[],"multi_level":{}} 
            KPI_DICT = {'kpi_selection_channel' : [], 'kpi_selection_value' : []}
            DATA = create_ra(filters)
            DATA = form_base_data(DATA)
            DATA = inititalize_columns(DATA,gloabal_vars)
            DATA,ds = Otb.initial_frame_calculation(DATA)
            do_revise_dict['do_revise'] = False
            revised_otb_to_update = None

            # print(DATA.columns,"DATAAA")


            if filters.sku !=[]:
                print("user selected the the ITEMID we need to destribute")
                print("rank_col is now showing")

                item_flag = True
                mean_cols = [f"pl.col('{col}').mean()" for col in avg_col]
                sums_cols = [f"pl.col('{col}').sum()" for col in sum_col]
                ranks_cols = [f"pl.col('{col}').max()" for col in rank_col]
                maxs_cols = [f"pl.col('{col}').max()" for col in list(set(max_col)-set(["ITEMID","Check_box"]))]
                kpi_cols = [f"pl.col('{col}').max()" for col in kpi_col]
                dist_cols = [f"pl.col('{col}').sum()" for col in ['new_otb_mix','revised_otb_amount'] if col in DATA.columns]
                art_cols = [f"pl.col('{col}').mean()" for a,col in art.items() if col in scores_m]
                cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
                cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
                agg_dict = [eval(expr) for expr in mean_cols+sums_cols+maxs_cols+ranks_cols+kpi_cols+ dist_cols +art_cols +cumulatives_1+cumulatives_2]
                DATA = DATA.with_columns(order_index=pl.lit(0))
                DATA = DATA.groupby(["ITEMID"]).agg(agg_dict)
                DATA_2 = DATA.clone() # Used for itemcode logic and otb destribution

            else:
                # mean_cols = [f"pl.col('{col}').mean()" for col in avg_col]
                # sums_cols = [f"pl.col('{col}').sum()" for col in sum_col]
                # maxs_cols = [f"pl.col('{col}').max()" for col in list(set(max_col)-set(["ITEMID"]))]
                # kpi_cols =[f"pl.col('{col}').max()" for col in kpi_col]
                # dist_cols = [f"pl.col('{col}').sum()" for col in ['new_otb_mix','revised_otb_amount'] if col in DATA.columns]
                # art_cols = [f"pl.col('{col}').mean()" for a,col in art.items() if col in scores_m]
                # cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
                # cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
                # agg_dict = [eval(expr) for expr in maxs_cols + sums_cols+mean_cols+kpi_cols + dist_cols +art_cols +cumulatives_1+cumulatives_2]
            
                # create new column order index
                # print(art_cols,"AGGREGATIONART")
                # print(DATA.columns,"First_data columns")
                # print(DATA.columns,"second_data columns")
                DATA = DATA.with_columns(order_index=pl.lit(0))
                DATA_2 = DATA.clone()
                print(DATA.columns,"DATACOLUMN2")
                
                # Addon data for KPI check selection
                DATA_KPI = DATA.clone()

        # DATA = data
        data = DATA
        print(data.columns,"small_data columns")
        # print(data_filter,"DATAFILTER")

        print(data.columns,"DATACOLUMN2")
        # sub_data = DATA 

        print(scores_m,"SCORE") 
        global group
        group = []
        sub_filter_state = False
        filter_condition = None 
        # print(DATA.columns,"DATASbeforesecondary")
        TEMP['key'] = DATA
        print(TEMP['key']['otb_amount'].sum(), 'temp key 1')
        print(DATA['otb_amount'].sum(), 'temp key after org DATA')
        
        if filter_condition is not None:
            DATA = DATA.with_columns(filter_index=filter_condition)
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))
        
        # calculate kpi coefficinet score    
       #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`    
        if secondary['article_score'] != []:
            print('kpi call')
            DATA = Otb.call_kpi(DATA, data_filter) 

        if data_filter['table_changes'] != {}:
            
            row = data_filter["table_changes"]["row"]
            print('table_change_row_is', row)
            columnID = data_filter["table_changes"]["columnId"]
            newValue = data_filter["table_changes"]["newValue"]
            
            # To determine which tire we are on KPI Selection box. row[0] determine which tire we are on.
            current_tire = list(row)[0]
                        
            if columnID == 'check_kpi':
                newValue_kpi = newValue
            elif columnID == 'Check_box':
                newValue_box = newValue

            child,other_filter_condition,  filter_condition,parent, \
                columns_to_filter, \
                    values_to_filter, group,DATA = Otb.table_change_filter(group, heirarchy, data_filter, DATA,row, filter_condition)              
            
#**************************************************************otb_check_box*****************************************************************            
            if columnID =="Check_box":
                # newValue = data_filter["table_changes"]["newValue"]
                # newValue = newValue_box
                selected_row = data_filter['table_changes']["row"]
                print(selected_row,"SELECT")

                if item_flag:
                    DATA =OTBDistribute.destribute_otb(DATA, DATA_2, item_flag, selected_row, SELECTION_DICT, newValue_box)
                   
                    mean_cols = [f"pl.col('{col}').mean()" for col in avg_col]
                    sums_cols = [f"pl.col('{col}').sum()" for col in sum_col]
                    rank_cols = [f"pl.col('{col}').max()" for col in rank_col]
                    kpi_cols = [f"pl.col('{col}').max()" for col in kpi_col]
                    dist_cols = [f"pl.col('{col}').sum()" for col in ['new_otb_mix','revised_otb_amount'] if col in DATA.columns]
                    art_cols = [f"pl.col('{col}').mean()" for a,col in art.items() if col in scores_m]
                    cumulatives_1 = [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]
                    cumulatives_2 = [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
                    agg_dict = [eval(expr) for expr in mean_cols+sums_cols+rank_cols+kpi_cols+dist_cols+cumulatives_1+cumulatives_2]

                    data = DATA.group_by('ITEMID',maintain_order=True).agg(agg_dict)
            
                # Assembly chekbox
                else:                   
                    asm_code = selected_row["AssemblyCodeNickName"]
                    item_code = selected_row["ITEMID"]
                    current_amount = selected_row["otb_amount"]

                    # hier_ap = ['Channel', 'Family', 'SubFamily', 'Supplier', 'Category','DOM_COMM','SubCategory']
                    hier_ap = gloabal_vars.hier_ap
                    # Funciton to form filter condtion based on avilability of otb value.
                    # This function retrieve the 
                    def iter_filter(group,heirarchy,row,hier_ap,DATA):
                        print('we are in iter filter')
                        print(group, 'group_iter_filter')
                        columns_to_filter = []
                        values_to_filter = []
                        print(row,"ROWS")
                        print(heirarchy, 'heirarch')
                  #  heirarchy = ["Channel","Family","SubFamily","Supplier","Category","DOM_COMM","SubCategory","ExtendedSubCategory",
                  # "SubCategorySupplier" ,"AssemblyCodeNickName","Status","ENDOFLife","Description","ITEMID"]

                        for i in group+heirarchy:
                            if i in (row and hier_ap):
                                columns_to_filter.append(i)
                                values_to_filter.append(data_filter['table_changes']["row"][i])
                                
                                print(i,"cols in the iter filter")
                                print(data_filter['table_changes']["row"][i], "values in the iter filter")
                                print(columns_to_filter,"COLUMNS")
                                print(values_to_filter,"VALUESSS")
                                print(hier_ap,"HEIRAPP")

                        child = None
                        other_filter_condition = None
                        parent = None
                        for col, val in zip(columns_to_filter, values_to_filter):
                            # if val == 'unknown' or 'NON':
                            #     continue

                            try:
                                if child is None:
                                    child = (DATA[col] == val)
                                    other_filter_condition = ~(DATA[col] == val)
                                    parent = None

                                else:
                                    try:
                                        parent = child
                                        other_filter_condition = child & (DATA[col] != val)
                                        child = child & (DATA[col] == val)
                                    except:
                                        print(col, val, 'iter destribution err')
                                        print(traceback.format_exc())
                            except:
                                print(col, val, 'iter destribution full')
                                print(traceback.format_exc())
                        print(hier_ap[-1], 'finding last one')
                        
                        return child,hier_ap[-1]


                    # Get the filters
                    if newValue_box == 0:
                        
                        for i in range(len(hier_ap)):
                            if DATA.filter(list(child))["otb_amount"].sum() == current_amount:
                                child,last_level = iter_filter(group,heirarchy,row,hier_ap,DATA) # , data_filter
                                print(last_level,"LASTLEVEL")

                                hier_ap.pop(-1)
                            else:
                                break
                        child,last_level = iter_filter(group,heirarchy,row,hier_ap,DATA) #, data_filter

                        # seperate data into group elements , selected element of group,deleted element in group
                        selected_element = child & (DATA['ITEMID'] == item_code) 
                        selected_group_element = child & (DATA['ITEMID'] != item_code) 
                        non_group_elements = child.not_()

                        # get deleted elements filter if values in dict and select deleted elements othr than the delted in  selected group
                        deleted_elements = DATA["ITEMID"].is_in(list(SELECTION_DICT["multi_level"].keys()))
                        filter_sets = set(DATA.filter(list(selected_group_element))["ITEMID"].unique()) - set(DATA.filter(list(deleted_elements))["ITEMID"].unique()) 
                        filtered_group_elements = DATA["ITEMID"].is_in(filter_sets)
                        print(last_level,"LASTLEVEL")
                        # update item code with the level on which the new amount updated in dict
                        SELECTION_DICT["multi_level"].update({item_code:last_level})

                        # distribute new value to filtered data and set selected item amount as 0 and also set checkbox with newvlaue
                        DATA_filtered_group_elements = DATA.filter(list(filtered_group_elements))
                        DATA_filtered_group_elements = Otb.drill_down_otb(DATA_filtered_group_elements,"otb_amount",current_amount)
                        DATA_selected_element = DATA.filter(list(selected_element))
                        DATA_selected_element = DATA_selected_element.with_columns(otb_amount = pl.lit(0.0))
                        DATA_selected_element = DATA_selected_element.with_columns(Check_box=pl.lit(newValue_box))

                        # data deleted and non group
                        DATA_non_group_elements = DATA.filter(list(non_group_elements))
                        DATA_deleted_group_elements = DATA.filter(list(deleted_elements))
                        DATA_deleted_group_elements = DATA_deleted_group_elements.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64))
                        # Final data
                        # DATA_filtered_group_elements  = DATA_filtered_group_elements.cast
                        print(DATA_non_group_elements.dtypes)
                        # DATA_deleted_group_elements = DATA_deleted_group_elements.cast(DATA_selected_element.dtypes)
                        print(DATA_selected_element.dtypes, 'ddddypes')
                        # for column,type_1, types_2, types_3, types_4 in zip(DATA_filtered_group_elements.columns, DATA_filtered_group_elements.dtypes, DATA_deleted_group_elements.dtypes, DATA_selected_element.dtypes, DATA_non_group_elements.dtypes):
                        #     print(column,type_1, types_2, types_3, types_4)
                        DATA_filtered_group_elements = DATA_filtered_group_elements.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))
                        DATA_deleted_group_elements = DATA_deleted_group_elements.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))
                        DATA_selected_element = DATA_selected_element.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))
                        DATA_non_group_elements = DATA_non_group_elements.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))

                        DATA = pl.concat([DATA_filtered_group_elements,DATA_deleted_group_elements,DATA_selected_element, DATA_non_group_elements])

                    
                    else:
                        print("newValue", "newvalue is here")
                        hier_ap = ['Channel', 'Family', 'SubFamily', 'Supplier', 'Category','DOM_COMM','SubCategory']

                        # get current amount, level on which selected items amount distributed  
                        current_amount = DATA_2.filter(list(DATA_2["ITEMID"] == item_code))["otb_amount"][0]
                        selection_level = SELECTION_DICT["multi_level"][item_code]
                        selection_level_value = selected_row[selection_level]

                        # deleted selected itemcode info from dict
                        del(SELECTION_DICT["multi_level"][item_code])

                        # get filter for the level on which previous amount distributed 
                        index_hier_ap = hier_ap.index(selection_level)
                        selected_group,level = iter_filter(group,heirarchy,row,hier_ap[:index_hier_ap+1],DATA) # , data_filter

                        # Form filters for selected element ,selected group, non gorup elements, 
                        # undeleted elements in the selected group

                        selected_element = selected_group & (DATA['ITEMID'] == item_code) 
                        selected_group_element = selected_group & (DATA['ITEMID'] != item_code) 
                        non_group_elements = selected_group.not_()
                        deleted_elements = DATA["ITEMID"].is_in(list(SELECTION_DICT["multi_level"].keys()))
                        filter_sets = set(DATA.filter(list(selected_group_element))["ITEMID"].unique()) - set(DATA.filter(list(deleted_elements))["ITEMID"].unique()) 
                        filtered_group_elements = DATA["ITEMID"].is_in(filter_sets)
            
                        #  remove selected otb amount from selected group and restore selected item value and set checkbox with new value
                        DATA_filtered_group_elements = DATA.filter(list(filtered_group_elements))
                        DATA_filtered_group_elements = Otb.drill_down_otb(DATA_filtered_group_elements,"otb_amount",-current_amount)
                        DATA_selected_element = DATA.filter(list(selected_element))
                        DATA_selected_element = DATA_selected_element.with_columns(otb_amount = current_amount)
                        DATA_selected_element = DATA_selected_element.with_columns(Check_box=pl.lit(newValue_box).cast(pl.Float64))

                        # non group elements data and deleted group data
                        DATA_non_group_elements = DATA.filter(list(non_group_elements))
                        DATA_deleted_group_elements = DATA.filter(list(deleted_elements))

                        # final data
                        # for column,type_1, types_2, types_3, types_4 in zip(DATA_filtered_group_elements.columns, DATA_filtered_group_elements.dtypes, DATA_deleted_group_elements.dtypes, DATA_selected_element.dtypes, DATA_non_group_elements.dtypes):
                        #     print(column,type_1, types_2, types_3, types_4)
                        DATA_filtered_group_elements = DATA_filtered_group_elements.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))
                        DATA_deleted_group_elements = DATA_deleted_group_elements.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))
                        DATA_selected_element = DATA_selected_element.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))
                        DATA_non_group_elements = DATA_non_group_elements.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))

                        
                        DATA = pl.concat([DATA_filtered_group_elements,DATA_deleted_group_elements,DATA_selected_element, DATA_non_group_elements])

                # columnID = None
                print(columnID,"columnid")

            print(newValue,"ZROWS1")

            if columnID =="check_kpi":
                row = data_filter["table_changes"]["row"]
                if child is None:
                    DATA = kp_Operations.apply_kpi_for_main_data(DATA, newValue_kpi, row)
                
            child,other_filter_condition,  filter_condition,parent, \
                            columns_to_filter, \
                                values_to_filter, group,DATA = Otb.table_change_filter(group, heirarchy, data_filter, DATA,row, filter_condition)
            
            DATA = Operations.edit_tables(DATA, data, row, group, newValue, columnID, 
                                            columns_to_filter, sub_filter_state, parent, child,other_filter_condition,filter_condition)  
            # 
        if data_filter["group_by"]['status']: #and data_filter['table_changes'] == {}:
            
            
            limit = 0
            filter_condition  = None           
            
            # Apply secondary filters and update filter_condition, sub_filter_state, and group
            filter_condition,sub_filter_state,group = Otb.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)
            filter_condition,sub_filter_state,group = Otb.apply_heirarchial_filters(data, group_by_id,
                                sub_filter_state, group,filter_condition)
                
            print(DATA.columns, 'columns_of_data_')
            data = Operations.apply_group_by(DATA, data_filter, group, sub_filter_state, filter_condition)


        print(DATA.columns,"DATACOLUMN3")
        print(data.columns,"DATACOLUMN4")

        if data_filter["expand"]['status']:
            print(group, 'in exp status')    
            print('we are in expand status')
           
            filter_condition,sub_filter_state,group = Otb.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)
            filter_condition,sub_filter_state,group = Otb.apply_heirarchial_filters(data, group_by_id,
                                sub_filter_state, group,filter_condition)
            
            data = Operations.apply_expand_heirarchy(DATA, data_filter, sub_filter_state, group, filters, heirarchy,limit, filter_condition)
 
 
        try:
            Channel_flag =False
            
            print(data.columns, 'wee cut coefff')
            print(data.shape, 'sizes of col data')

            # data = Otb.call_kpi(data, data_filter) 

            data,dtemp,Channel_flag = Otb.calculate_df(data,ds,Channel_flag) 
            print(group, 'group from processes')

            # print(filters, 'fflt')
            if data_filter["group_by"]["status"] == True or (data_filter["expand"]["status"] == True and data_filter['table_changes'] == {}) or filters.sku !=[]:
                data_kpi = data.clone()

            if data_filter['table_changes']!={}:  
                row = data_filter["table_changes"]["row"]
                columnID = data_filter["table_changes"]["columnId"]
                newValue = data_filter["table_changes"]["newValue"]
                print(row, 'checking row for passed revised')
                if columnID == 'check_kpi':
                    newValue_kpi = newValue
                    child, group = kp_Operations.kpi_inner_child_selection(data, data_filter, group, heirarchy)
                    data = Otb.call_kpi(data, data_filter)
#                   Include main group
                    if child is not None:
                        if newValue_kpi == 1:                
                            # if scores_m != []:
                            data = kp_Operations.calculate_revised_budget(data, child, data_kpi, KPI_DICT, row)
                        if newValue_kpi == 0:
                            # if scores_m != []:
                            data = kp_Operations.user_uncheck_kpi_selection(data, child, newValue_kpi)
            if data_filter['select_all_kpi'] == True and 'coefficient_score_mix_percent' in data.columns:
                print('select all kpi true')
                # The child will be of the condition to select full and apply the integration totaly
                
                row = None
                data_kpi = data.clone()
                child, group = kp_Operations.kpi_inner_child_selection(data, data_filter, group, heirarchy)
                data = data.with_columns(check_kpi = pl.lit(1).cast(pl.Int8))
                data = Otb.call_kpi(data, data_filter)
                data = kp_Operations.calculate_revised_budget(data, child, data_kpi, KPI_DICT, row)
            elif data_filter['select_all_kpi'] == False and data_filter['table_changes']=={}:
                print('select all kpi false')
                
                child, group = kp_Operations.kpi_inner_child_selection(data, data_filter, group, heirarchy)
                # child = ~(child)
                data = data.with_columns(check_kpi = pl.lit(0).cast(pl.Int8))
                data = Otb.call_kpi(data, data_filter)
                newValue_kpi = 0
                data = kp_Operations.user_uncheck_kpi_selection(data, child, newValue_kpi)
            # Here we store our data we need to update
            if 'revised_otb_amount' in data.columns:
                want_to_update_new_otb_mix['update'] = data
            # And we get do_revise flag from /update_otb_amount
            print(do_revise_dict['do_revise'], 'the_do revise flag')
            #!!!!!!!!!!!
            if do_revise_dict['do_revise'] == True:
                if type(new_revised['revised_otb']) != type(pl.DataFrame({'check' : [1,2,3]})):
                    ida_of_otb_mix = data.find_idx_by_name('otb_amount')
                    print(data.columns, 'columns_of_data_to_transfer')
                    print(want_to_update_new_otb_mix['update'].columns, 'the saved rev otb data')
                    # data.replace('otb_amount', pl.Series([new_revised['revised_otb']]*len(data)) * (data['otb_percent']/100))
                    data = data.with_columns(otb_amount = (data['otb_percent'] * new_revised['revised_otb'])/100)
                    # DATA = DATA.with_columns(otb_amount = (DATA["otb_percent"] * new_revised['revised_otb'])/100)
                    print(group, 'group at do revise 1')
                    filter_condition,sub_filter_state,group = Otb.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)

                    DATA = kp_Operations.destribute_otb_(new_revised['revised_otb'], DATA, group, data_filter, heirarchy, sub_filter_state)
                    print('Success....')
                    print(DATA['otb_amount'].sum(), 'DATA changed otb amount')
                    print(data.columns, 'data_columns_aft_replace')
                    do_revise_dict['do_revise'] = False
                elif type(new_revised['revised_otb']) == type(pl.DataFrame({'check' : [1,2,3]})):
                    row = data_filter["expand"]['row']
                    ida_of_otb_mix = data.find_idx_by_name('otb_amount')
                    print(data.columns, 'columns_of_data_to_transfer')
                    print(want_to_update_new_otb_mix['update'].columns, 'the saved rev otb data')
                    print(new_revised['revised_otb'].columns, 'the saved rev otb data cols')
                    print(new_revised['revised_otb'], 'the saved mix data')
                    #------------------------need to 
                    # user_selected_rows_ = new_revised['revised_otb'].filter(new_revised['revised_otb'][list(row)[0]] == row[list(row)[0]])['revised_otb_amount'].item()
                    # data = data.with_columns(otb_amount = (data['otb_percent'] * user_selected_rows_)/100)
                    #------------------------need to
                    filter_condition,sub_filter_state,group = Otb.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)
                    DATA = kp_Operations.destribute_otb_total(new_revised['revised_otb'], DATA, group, data_filter, heirarchy, sub_filter_state)
                    do_revise_dict['do_revise'] = False
                                   
            else:
                pass
            if 'renewed_otb_percent' in data.columns:
                print(data.select(pl.col(['renewed_otb_percent', 'revised_otb_amount', 'new_otb_mix'])), 'at last')
            #!!!!!!!!!!!
            data = Otb.calculate_margin(data)
            data = Otb.calculate_quantity(data)
            data = Otb.calculate_cost(data)
        
            print(data.columns, 'columns of bot col calculations')
            # aggregators for bottom columns
            art_cols = [f"pl.col('{col}').mean()" for a,col in art.items() if col in scores_m]

            agg_dict = [eval(expr) for expr in 
                        [f"pl.col('{col}').mean()" for col in avg_col if col in data.columns] + 
                        [f"pl.col('{col}').sum()" for col in sum_col if col in data.columns] +
                        [f"pl.col('{col}').max()" for col in kpi_col if col in data.columns] +
                        [f"pl.col('{col}').max()" for col in rank_col if col in data.columns] +
                        [f"pl.col('{col}').mean()" for a,col in art.items() if col in scores_m] +
                        [f"pl.col('{col}').sum()" for col in ['coefficient_score'] if len(art_cols)!=0]+
                        [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0] + 
                        [f"pl.col('{col}').sum()" for col in ['new_otb_mix','revised_otb_amount'] if col in data.columns]
                        ]

            if Channel_flag == False:
                print('False channel flag')
                bottom_column = (data.select(agg_dict))        
                bottom_column = Otb.calculate_bottom_first_tab(bottom_column)               
                bottom_column = Otb.calculate_bottom_margin_tab(bottom_column)
                bottom_column = Otb.calculate_bottom_cost_tab(bottom_column)
                bottom_column = Otb.calculate_bottom_qty_tab(bottom_column)
               

            else:
                print('There is channel flag')
                bottom_column = (dtemp.agg(agg_dict))
                bottom_column = Otb.calculate_bottom_first_tab(bottom_column.to_dict())
                bottom_column = Otb.calculate_bottom_margin_tab(bottom_column)
                bottom_column = Otb.calculate_bottom_cost_tab(bottom_column)
                bottom_column = Otb.calculate_bottom_qty_tab(bottom_column)

            bottom_column              = data.select(agg_dict).to_dict()
            bottom_column = {key:bottom_column[key][0] for key in list(bottom_column.keys())}
            bottom_column = pd.Series(bottom_column)
            bottom_column[int_cols]    = bottom_column[int_cols].replace([np.inf,-np.inf],0).fillna(0).astype(int)
            # print(art_cols, 'aaaaaaa7')
            # print(DATA.select(pl.col(['coefficient_score', 'coefficient_score_mix_percent']).mean()), 'aaaaaaa7')
            
        except:
            print(traceback.format_exc())
            bottom_column = data
        
        TEMP['key'] = DATA
        print(TEMP['key']['otb_amount'].sum(), 'temp key 2')

        print(type(DATA),"DATATYPE")
        size = len(data) 
        if size == 1:
            editable_cols = json.dumps(["otb_amount","otb_vs_py_percent","otb_vs_ppy_percent"])
        else:
            editable_cols = json.dumps(gloabal_vars.editable_cols)
           
        # order data as pwer the dicitonary elements
        if "order_index" in data.columns:
            print('order index in before sort')
            data = data.sort(by = "order_index",descending =True)
        
        print(data.columns, 'the data_before_sort')

        if "sort" in data_filter:
            datas = Operations.sort_and_clean(data_filter, data, filters)

            datas = datas.to_pandas()
            column = datas.columns        
#********************************Bugging**********************************
            print(column,'xcol')
            if "coefficient_score_mix_percent" in column:
                print(datas["coefficient_score"].mean(),"ckbok45")
                if 'Channel' in column:
                    print(datas[["coefficient_score", 'Channel', 'coefficient_score_mix_percent']],"ckbok4")
            if "Check_box" in column:
                print(DATA["Check_box"].unique(),"ckbok45")
                print(DATA.filter(list(DATA["Check_box"] == 0.0)))
            print(datas.columns, 'dsdsdsdsdsdsd')
#*****************************************************************       
            sel_all_kpi = data_filter['select_all_kpi']
            if sel_all_kpi == '':
                sel_all_kpi = False
            else:
                sel_all_kpi
                
            data_json = f"""{datas.to_json(orient='split')[:-1]}, "select_all_kpi": {json.dumps(sel_all_kpi)}, "percent_col":{json.dumps(gloabal_vars.percent_col)}, "editable_cols":{editable_cols},"tabs":{tabs} ,"items":{size},"total":{bottom_column.to_json()} {datas.to_json(orient='split')[-1]}"""
            await websocket.send_text(data_json)
#***************************************************************************************************************