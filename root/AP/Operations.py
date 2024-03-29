import polars as pl
import numpy as np
import traceback
import subprocess

import time
from .parameters import Parameters,Otb
from .schemas import Filters
from typing import Dict, List, Optional, Union, Tuple

Otb = Otb()
gloabal_vars = Parameters()

max_col = gloabal_vars.max_col
sum_col = gloabal_vars.sum_col
avg_col = gloabal_vars.avg_col
int_cols = gloabal_vars.int_cols
float_cols = gloabal_vars.float_cols
rank_col = gloabal_vars.rank_col
kpi_col = gloabal_vars.kpi_col
art = gloabal_vars.SCORES

class Operations:
        
    def edit_tables(self, DATA:pl.DataFrame, data : pl.DataFrame, row:dict, group : list, 
                    newValue : int, columnID :str, columns_to_filter : list, 
                    sub_filter_state : bool, parent : [Optional] = None, 
                    child : [Optional] = None, other_filter_condition : [Optional] = None, 
                    filter_condition : [Optional] = None) -> pl.DataFrame:
        """
        This function edits tables based on the provided parameters and returns the updated DataFrame.

        Args:
            DATA (pl.DataFrame): The main DataFrame to be edited.
            data (pl.DataFrame): The output DataFrame.
            row (Dict): The selected row from front end.
            group (List): List of selected subfilters columns.
            newValue (int): The new value to be set.
            columnID (str): The ID of the column to be edited.
            columns_to_filter (List): Columns to be used for filtering/hierachical columns.
            sub_filter_state (bool): Sub-filter state , indicating sub_filter selected or not.
            parent: [Optional] = None): Parent filter condition ,series of bool/None.
            child: [Optional] = None): Child filter condition/child location, series of bool/None.
            other_filter_condition: [Optional] = None): Other filter condition/sibling location, series of bool/None.
            filter_condition: [Optional] = None): Filter condition ,series of bool/None..

            Returns:
                pl.DataFrame: The updated DataFrame.

            """
        print('the columnID is: ', columnID)
        
               
        original = row[columnID]

        if row[columnID] == None:
            original = 0
        

        increase = newValue - float(original)

        print("We are in edit columns")
        print("newValue is_ :",newValue )
        print("OriginalValue is: ", original)
        print("increase is :", increase)

        if columnID == "otb_vs_act_forecast_percent":
           
            otb_amount = (newValue*row["sales_act_vs_forecast"])/100
            # print(otb_amount,"ott")
            if parent is None: otb_amount_sum = DATA["otb_amount"].sum()
            else: otb_amount_sum = DATA.filter(list(parent))["otb_amount"].sum()
            newValue = (100*otb_amount)/otb_amount_sum
            columnID = "otb_percent"
    
        if columnID == "otb_amount":
            if child is None:
                DATA = DATA.with_columns(otb_amount = (DATA["otb_percent"] * newValue)/100)

        if columnID == "otb_vs_ppy_percent": 
            
            if (len(columns_to_filter)) ==1 and ((sub_filter_state ==True)):
                DATA = DATA.with_columns(((DATA["net_sales_lly"] * newValue)/100).alias("otb_amount"))

            elif child is None:
                DATA = DATA.with_columns(((DATA["net_sales_lly"] * newValue)/100).alias("otb_amount"))
                columnID == "otb_amount"

            else:

                ''' for child/grouped '''
                otb_amount = (row["net_sales_lly"] * newValue)/100

                if parent is None: otb_amount_sum = DATA["otb_amount"].sum()
                else: otb_amount_sum = DATA.filter(list(parent))["otb_amount"].sum()
                if otb_amount < otb_amount_sum:
                    newValue = (100*otb_amount)/otb_amount_sum
                    columnID = "otb_percent"

        if columnID == "units_vs_sku":
            otb_amount = (newValue * row["initial_average_retail_price"]*row["total_sku_count"])
            if parent is None: otb_amount_sum = DATA["otb_amount"].sum()
            else: otb_amount_sum = DATA.filter(list(parent))["otb_amount"].sum()
            if otb_amount < otb_amount_sum:
                newValue = (100*otb_amount)/otb_amount_sum
                columnID = "otb_percent" 

        
        if columnID == "units_buy_by_sku":
            otb_amount = newValue * row['initial_average_retail_price']
            if parent is None: otb_amount_sum = DATA["otb_amount"].sum()
            else: otb_amount_sum = DATA.filter(list(parent))["otb_amount"].sum()
            if otb_amount < otb_amount_sum:
                newValue = (100*otb_amount)/otb_amount_sum
                columnID = "otb_percent"

        if columnID == "otb_vs_sku":
            otb_amount = newValue * row["total_sku_count"]
            if parent is None: otb_amount_sum = DATA["otb_amount"].sum()
            else: otb_amount_sum = DATA.filter(list(parent))["otb_amount"].sum()

            if otb_amount < otb_amount_sum:
                newValue = (100*otb_amount)/otb_amount_sum
                columnID = "otb_percent"

        if columnID == "otb_vs_py_percent":
            if (len(columns_to_filter)) ==1 and ((sub_filter_state ==True)):
                DATA = DATA.with_columns(((DATA["net_sales_ly"] * newValue)/100).alias("otb_amount"))
            elif child is None:
                DATA = DATA.with_columns(((DATA["net_sales_ly"] * newValue)/100).alias("otb_amount"))
                columnID == "otb_amount"

            else:
                ''' for child/grouped '''
                otb_amount = (row["net_sales_ly"] * newValue)/100

                if parent is None: otb_amount_sum = DATA["otb_amount"].sum()
                else: otb_amount_sum = DATA.filter(list(parent))["otb_amount"].sum()
                if otb_amount < otb_amount_sum:
                    newValue = (100*otb_amount)/otb_amount_sum
                    columnID = "otb_percent"
    
        if columnID == 'otb_percent':
            s = data['relative_otb_percent'].sum()
            increase = (s*newValue/100) - (s*row[columnID]/100)

            if sub_filter_state ==True:
                DATA = DATA.with_columns(((DATA['otb_percent']/DATA['otb_percent'].sum())*100).alias('otb_percent'))
                DATA = DATA.with_columns(DATA['otb_percent'].alias("relative_otb_percent"))
                
                DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                DATA_siblings = DATA.filter(list(parent.not_()))
                DATA = pl.concat([DATA_parent,DATA_siblings])
            else:
                if len(columns_to_filter) == 1:
                    DATA = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                else:
                    DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                    DATA_siblings = DATA.filter(list(parent.not_()))
                    DATA = pl.concat([DATA_siblings,DATA_parent])
                
#---------------------------------------------------------What you are doing here idk--------------------------------------------------------
    #distribute change in budget_amount

        elif columnID != None :


            if child is None:
                print('we are in child is empty---')
                ''' since grouping column is empty it won't go to grouping block, so we had to change the 'data' to DATA
        manually, also need to do aggregation inside here,though it's messy '''

            else:
                if sub_filter_state == True:
                    print('we are in child is empty--- sub_filter_state == True')


                    if len(columns_to_filter) == 1:
                        ''' to manage 1 inner block,if 1 group'''
                        print('len(columns_to_filter) == 1:')
                        print(columns_to_filter, 'columns_to filter')
                        DATA = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(sibling_condition)),increase= increase, colID= columnID)
                        data = DATA.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)
                        DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))
                    else:
                        print('len(columns_to_filter) is other')
                        print(columns_to_filter, 'columns_to filter2')


                        DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                        DATA_siblings = DATA.filter(list(parent.not_()))
                        DATA = pl.concat([DATA_siblings,DATA_parent])
                        summation = DATA["otb_amount"].sum()
                        DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))

                else:
                    print('we are in child is empty--- sub_filter_state == False')

                    if len(columns_to_filter) == 1:
                        print('len(columns_to_filter) == 1:')
                        print(columns_to_filter, 'columns_to filter')

                        ''' to manage 1 inner block,if 1 group'''
                        DATA = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                        summation = DATA["otb_amount"].sum()
                        DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))
                    else:
                        print('len(columns_to_filter) is other')
                        print(columns_to_filter, 'columns_to filter2')

                        ''' to manage more than 1 inner block,if more than 1 group'''
                        DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= columnID)
                        DATA_siblings = DATA.filter(list(parent.not_()))
                        print('concat issues')
                        # for column,type_siblings, types_parent in zip(DATA_siblings.columns, DATA_siblings.dtypes, DATA_parent.dtypes):
                        #     print(column, type_siblings, types_parent)
                        # types =zip(DATA_siblings.columns, DATA_siblings)
                        DATA_siblings = DATA_siblings.with_columns(check_kpi = pl.col('check_kpi').cast(pl.Float64).fill_nan(0).fill_null(0))
                        DATA_parent = DATA_parent.with_columns(check_kpi = pl.col('check_kpi').cast(pl.Float64).fill_nan(0).fill_null(0))

                        DATA_siblings = DATA_siblings.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))
                        DATA_parent = DATA_parent.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64).fill_nan(0).fill_null(0))

                        DATA = pl.concat([DATA_siblings,DATA_parent])
                        
                        summation = DATA["otb_amount"].sum()
                        DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------       

        if columnID == 'otb_percent':
            summation = DATA["otb_amount"].sum()
            DATA = DATA.with_columns(otb_amount = DATA[columnID]*summation/100)
            DATA = DATA.with_columns((pl.col("otb_amount")/pl.col('initial_average_retail_price')).alias('units_buy_by_sku'))
            DATA = DATA.with_columns(((pl.col("otb_amount") - (pl.col('initial_average_retail_price') * pl.col('units_buy_by_sku')))).alias('Deficit'))
            DATA = DATA.with_columns(((DATA['otb_amount']/DATA['otb_amount'].sum())*100).alias("relative_otb_percent"))
            DATA = DATA.with_columns(((pl.col('otb_amount')/pl.col("sales_actual"))*100).alias("otb_vs_act_Forecast_percent"))
            DATA = DATA.with_columns((pl.col("otb_vs_act_Forecast_percent")/100).alias("sales_act_vs_forecast"))
            DATA = DATA.with_columns(((pl.col('otb_amount')/pl.col("net_sales_ly"))*100).alias("otb_vs_py_percent"))
            DATA = DATA.with_columns(((pl.col('otb_amount')/pl.col("net_sales_lly"))*100).alias("otb_vs_ppy_percent"))
            DATA = DATA.with_columns(((pl.col('otb_amount')/pl.col("total_sku_count"))).alias("otb_vs_sku"))
# 
        return DATA  

    def otb_destribution_(self, DATA : pl.DataFrame, ):
        
        
        return DATA
    
    def apply_group_by(self, DATA:pl.DataFrame, data_filter : dict, group : list, sub_filter_state : bool, filter_condition : [Optional] = None):
        """
            Apply group-by operation on the given DataFrame based on specified parameters. 
            Checks contains the main filter flag and sub filter flag


            Args:
                DATA (pl.DataFrame): The main DataFrame.
                data_filter (Dict): input filter response.
                sub_filter_state (bool): Sub-filter state/ subfilter selected or not.
                group (List): List of columns used for grouping/subfilter columns.
                filters (Filters): Filters object.
                filter_condition: filter condition for main data /series of bool.

            Returns:
                Union[None, pl.Series, pl.DataFrame]: The result of the group-by operation filter_condtion and dataframe.


        """
        secondary = data_filter['secondary_filter']
        scores_m = secondary['article_score'] 
        

        


        art_cols = [f"pl.col('{col}').mean()" for a, col in art.items() if col in scores_m]

        print(f"group before ['group_by']['cols'] {group}") # one ingeminate 2 SFS's

        if filter_condition is not None:
            DATA = DATA.with_columns(filter_index = filter_condition)
            DATA = DATA.with_columns(filter_index = pl.col('filter_index').cast(pl.Int8))

        # decipher the stipulated dominant distill
        if not len(data_filter["group_by"]["columns"]) == 0: 
            print("P_F ENABLED")
            for i in data_filter["group_by"]["columns"]:
                print(i, "checking", end =" ")
                if i in gloabal_vars.filter_details:
                    print("is in filter_details")
                    group.append(gloabal_vars.filter_details[i])
                else:
                    print("is not in filter_details")
            print("group selected in main filters", group)
            limit = max_col.index(group[-1])
            print("limit of main filter-store grouping:", limit,)
            
            
            # The one has to find DATA based on the ITEM-ID, hence wielded rank-col
            agg_dict = [eval(expr) for expr in 
                        [f"pl.col('{col}').mean()" for col in avg_col] + 
                        [f"pl.col('{col}').sum()" for col in sum_col]+
                        [f"pl.col('{col}').max()" for col in kpi_col]+ 
                        [f"pl.col('{col}').sum()" for col in ['new_budget_mix', 'revised_budget_amount'] if col in DATA.columns]+
                        [f"pl.col('{col}').mean()" for a, col in art.items() if col in scores_m]+
                        [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]+
                        [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
                        ]
            if data_filter['sku'] != []:
                agg_dict = agg_dict + [eval(expr) for expr in [f"pl.col('{col}').max()" for col in rank_col]]
            # Have to prink the salient filter
            if filter_condition is not None: # MFS and filter_condition
                print("Have the salient filter columns")
                try:
                    data = DATA.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)
                    Otb.SUB_FILTER = Otb.call_filter(DATA.filter(list(filter_condition)), Otb.SUB_FILTER, group, DATA, Otb.filter_store)
                except Exception as e:
                    print("['group_by']['columns'] error with filter_store is : ", e)
                    pass
            else: # MFS 
                print("void state")
                try:
                    data = DATA.group_by(list(set(group))).agg(agg_dict)
                except Exception as e:
                    print(f"The error in ['group_by']['columns'] without filter store is :", e)
        else:
            print("We dont have the main filter-store does the main filter have the sub-filter state")
            agg_dict = [eval(expr) for expr in 
                        [f"pl.col('{col}').mean()" for col in avg_col]+
                        [f"pl.col('{col}').sum()" for col in sum_col]+
                        [f"pl.col('{col}').max()" for col in kpi_col]+
                        
                        [f"pl.col('{col}').sum()" for col in ['new_otb_mix', 'revised_otb_amount'] if col in DATA.columns]+
                        [f"pl.col('{col}').mean()" for a, col in art.items() if col in scores_m]+
                        [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]+
                        [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
                        ]
            if not sub_filter_state:
                print("wonted main group, no sub filter state")
                try:
                    datas = Otb.calculate_initial_frame(DATA, ['ITEMID'])
                    

                    #coefficient score mix percent calculation
                    
                    data = datas.select(agg_dict)
                    if 'renewed_otb_percent' in DATA.columns:
                        print(data.select(pl.col(['revised_otb_amount', 'new_otb_mix'])), 'DATA check revised mix')
                    
                    Otb.SUB_FILTER = Otb.call_filter(datas, Otb.SUB_FILTER, group, DATA, Otb.filter_store)
                
                except AttributeError as e:
                    print(f"Error is : {e}, in general grouping")
            # play with sub_filter_state
            else:
                print("main group subfilterstate cavort")
                try:
                    item_group = group + ['ITEMID']
                    item_group = list(set(item_group))
                    datas = Otb.calculate_initial_frame(DATA.filter(list(filter_condition)), item_group)

                    filter_condition = datas["filter_index"]==1

                    data = datas.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)

                    Otb.SUB_FILTER = Otb.call_filter(datas.filter(list(filter_condition)), Otb.SUB_FILTER, group, DATA, Otb.filter_store)

                except Exception as e:
                    print("[group_by] sub_filter state has problem with us: ", e, "Also...")
                    print(traceback.format_exc())
                    pass
                    
        # group before ['group_by']['cols']
        # P_F ENABLED
        # checking
        # group selected in main filters
        # limit of main filter-store grouping:
        # Have the salient filter columns
        # void state
        # We dont have the main filter-store does the main filter have the sub-filter state
        # wonted main group, no sub filter state
        # main group subfilterstate cavort

        #errors

        # ['group_by']['columns'] error with filter_store is 
        # The error in ['group_by']['columns'] without filter store
        # in general grouping
        # [group_by] sub_filter state has problem with us:

        return data

    #This is little problematic when applied on init
    def get_expand_filter(self, DATA : pl.DataFrame, data_filter:dict, heirarchy:list, group :list, filter_condition :[Optional] = None):
        
        print('we are in get expand filter')
        print('group is', group)

        columns_to_filter = []
        values_to_filter = []
        # print(data_filter,"DATAFILTER")
        selected_row = data_filter['expand']["row"]
        print(selected_row)
        # filters for delving 
        print('checking... hier')
        for i in selected_row:
            if i in heirarchy:
                columns_to_filter.append(i)
                values_to_filter.append(selected_row[i])
                # last filter is the position of columns need to be filtered in the heirarchy list
        if columns_to_filter != []:
            print('columns to filter is not empty:', columns_to_filter)
            last_filter = heirarchy.index(columns_to_filter[0])
            print(last_filter, 'drill process last_filter')

        else:
            last_filter = 0 # to select first expanding
        group_to_filter = []
        group_value_to_filter = []
        
        "subfiltering"
        print('checking sfs filters')
        for i in selected_row:
            if i in group:
                group_to_filter.append(i)
                group_value_to_filter.append(selected_row[i])

        print(f"cols ; {columns_to_filter}, vals: {values_to_filter}, grp : {group_to_filter}, grp_vals : {group_value_to_filter}, of expand")

        # filter all the necessary values
        for col, val in zip(columns_to_filter+group_to_filter, values_to_filter+group_value_to_filter):
            if filter_condition is None:
                try:
                    filter_condition = (DATA[col] == val)
                except Exception as e:
                    
                    print(f"wrong heirarchy: {e}")
            else:
                filter_condition = filter_condition & (DATA[col] == val)

        if filter_condition is not None:
            DATA = DATA.with_columns(filter_index=filter_condition)
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))

        return columns_to_filter, filter_condition, group, last_filter


    
    def apply_expand_heirarchy(self, DATA:pl.DataFrame, data_filter : dict, sub_filter_state : bool, group : list, filters : Filters,
                                heirarchy : list, limit : int, filter_condition : [Optional] = None):
        
        secondary = data_filter['secondary_filter']
        scores_m = secondary['article_score']
        if scores_m != []:
            DATA = Otb.call_kpi(DATA, data_filter)
        art_cols = [f"pl.col('{col}').mean()" for a,col in art.items() if col in scores_m]
        agg_dict = [eval(expr) for expr in 
                    [f"pl.col('{col}').mean()" for col in avg_col]+
                    [f"pl.col('{col}').sum()" for col in sum_col]+
                    [f"pl.col('{col}').max()" for col in kpi_col]+
                    [f"pl.col('{col}').sum()" for col in ['new_otb_mix','revised_otb_amount'] if col in DATA.columns]+
                    [f"pl.col('{col}').mean()" for a,col in art.items() if col in scores_m]+
                    [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]+
                    [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]
                    ]
        
        columns_to_filter, filter_condition, \
                group, last_filter = self.get_expand_filter(DATA, data_filter, heirarchy, group, filter_condition)
        # SFS From apply_secondary_filter and apply_heirarchial_filter
        if filter_condition is not None:
            print('filter_condition not None in exp heirarchy')
            DATA = DATA.with_columns(filter_index=filter_condition)
            DATA = DATA.with_columns(filter_index= pl.col("filter_index").cast(pl.Int8))
       
        # ascertain channel
        if columns_to_filter == []:
            print('havinf girst course')
            last_filter = 0
            group.append(heirarchy[last_filter])
            # Remove identical
            groups = list(set(group))
            item_group = groups + ['ITEMID']
            #remove duplicates
            item_group = list(set(item_group))
            if sub_filter_state: 
                print('sFs in 0th docket')
                try:
                    # calculate deficit coefficient score then grouping, the find sold_article_count_ly and total_sku_count_ly
                    datas = Otb.calculate_initial_frame(DATA.filter(list(filter_condition)), item_group)
                    filter_condition = datas['filter_index'] == 1
                    data = datas.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)

                    # can we add kpi_analysis?
                except Exception as e:
                    print('error at primary sFs', e)
            else:
                print('no sFs on nill indication')
                try:
                    datas = Otb.calculate_initial_frame(DATA, item_group)

                    data = datas.group_by(list(set(groups))).agg(agg_dict)

                except Exception as e:
                    print(traceback.format_exc())
                    print('no sFs channel err : ', e)
        else:
            print('piercing while there is sFs')
            # groups is used for aggregation of max cols combining the group
            try:
                groups = max_col[limit:max_col.index(columns_to_filter[0])+1]
            except Exception as e:
                print('err is while walking on expands no sFs')
                print(traceback.format_exc())
                groups = max_col[:max_col.index(columns_to_filter[0])+1]
            
            group.append(heirarchy[last_filter+1]) # last filter got from expandrow and last of group from drill down loop
            

            print(group, 'groups of multiple drilling')

            item_group = group+["ITEMID"]
            
            # To get distincts up to the slected drilldown
            item_group = list(set(item_group + heirarchy[:last_filter+1]))
            
            # filter_condition = DATA["filter_index"]==1

            datas = Otb.calculate_initial_frame(DATA.filter(list(filter_condition)),item_group)

            filter_condition = datas["filter_index"]==1
 
            agg_dict = [eval(expr) for expr in 
                        [f"pl.col('{col}').mean()" for col in avg_col]+
                        [f"pl.col('{col}').sum()" for col in sum_col]+

                        [f"pl.col('{col}').max()" for col in groups if col not in group]+ # Added up
                        [f"pl.col('{col}').max()" for col in kpi_col]+
                        [f"pl.col('{col}').sum()" for col in ['new_otb_mix','revised_otb_amount'] if col in DATA.columns]+
                        [f"pl.col('{col}').mean()" for a,col in art.items() if col in scores_m]+
                        [f"pl.col('{col}').mean()" for col in ['coefficient_score'] if len(art_cols)!=0]+
                        [f"pl.col('{col}').sum()" for col in ['coefficient_score_mix_percent'] if len(art_cols)!=0]]

            data = datas.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)                

            Otb.SUB_FILTER = Otb.call_filter(datas.filter(list(filter_condition)),Otb.SUB_FILTER,group,DATA,Otb.filter_store)
 
        return data
 
    def sort_and_clean(self, data_filter:Dict, data:pl.DataFrame, filters:Filters):
        """
        Prepare data for display in UI,do sorting/pagination/cleaning.

        Args:
            data_filter (Dict): input response dict.
            data (pl.DataFrame): DataFrame to be sorted and cleaned and paginated.

        Returns:
            pl.DataFrame: Sorted ,cleaned and paginated DataFrame.

        """
        print('we are finding corr ex5')
        try:
            print("we are trying in sorting")
            sort_columnid = data_filter["sort"]["id"]
            if data_filter["sort"]["desc"] == False:
                datas = data.sort(by =[sort_columnid], descending= False)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size]
            elif data_filter["sort"]["desc"] == True:
                datas =data.sort(by =[sort_columnid], descending= True)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size]
           
        except:
            print(traceback.format_exc(), 'the error sort')
            print("we are exceptioning in sorting")
            datas = data[(filters.page_number*filters.page_size):((filters.page_number+1)*filters.page_size)]
            column = datas.columns
            if "coefficient_score_mix_percent" in column:
                print(datas["coefficient_score"].mean(),"ckbok45")
                if 'Channel' in column:
                    print(datas.select(pl.col(["coefficient_score", 'Channel', 'coefficient_score_mix_percent'])),"ckbok4pol")
        return datas