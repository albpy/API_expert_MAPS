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
# HEIRARCHY = gloabal_vars.heirarchy
# print(HEIRARCHY, 'the heirarchy')
class kp_Operations:
    #
    def calculate_revised_budget(self, DATA : pl.DataFrame, child : pl.Series, DATA_KPI : pl.DataFrame, KPI_DICT : dict, row : dict =None):
  
        #--------------
        otb_amount_summation = DATA['otb_amount'].sum()
        #--------------
        print(otb_amount_summation, 'otb_amount_summation')
        print(child, 'child in calculate revised budget')
        print(type(child), 'child in calculate revised budget')

        Data_major = DATA.filter(list(child))

        Data_major = Data_major.with_columns(check_kpi = pl.lit(1).cast(pl.Int8))

        Data_major = Data_major.with_columns(renewed_otb_percent = pl.col('coefficient_score_mix_percent').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))

        value_to_find_net_mix = (Data_major['otb_percent']-Data_major['renewed_otb_percent']).sum()

        print(value_to_find_net_mix, 'value_to_find_net_mix')

        Data_major = Data_major.with_columns(re_assigned_mix = pl.lit(0).cast(pl.Float64))

        Data_major = Data_major.with_columns(new_otb_mix = pl.col('renewed_otb_percent')+pl.col('re_assigned_mix'))
        """##User unselected data##"""

        Data_minor = DATA.filter(list(child.not_()))

        Data_minor = Data_minor.with_columns(check_kpi = pl.lit(0).cast(pl.Int8))

        # Here renewed otb percent remains same as otb percent because user not get select the kpi metrics
        
        Data_minor = Data_minor.with_columns(renewed_otb_percent = pl.col('otb_percent').cast(pl.Float64))

        sum_of_remaining_otb_perc = Data_minor['otb_percent'].sum()

        print(sum_of_remaining_otb_perc, 'sum_of_remaining_otb_perc')        

        Data_minor = Data_minor.with_columns(re_assigned_mix =((value_to_find_net_mix) * (pl.col('renewed_otb_percent').cast(pl.Float64)/sum_of_remaining_otb_perc)).cast(pl.Float64).replace({np.inf:0, -np.inf:0}).fill_nan(0))

        Data_minor = Data_minor.with_columns(new_otb_mix = pl.col('otb_percent')+pl.col('re_assigned_mix'))

        DATA = pl.concat([Data_major, Data_minor])
       
        print((DATA['otb_percent']-DATA['renewed_otb_percent']).sum(), 'cdsdsdfsdfsdfds')        

        DATA = DATA.with_columns(revised_otb_amount = (pl.col('new_otb_mix')* (pl.col('otb_amount').sum()))/100)

        print(child.value_counts(), 'chuld')

        return DATA


    def user_uncheck_kpi_selection(self, DATA : pl.DataFrame, child : pl.Series, newValue: int = None):
       
        
        Data_minor = DATA.filter(list(child)) 
        
        Data_minor = Data_minor.with_columns(check_kpi = pl.lit(newValue).cast(pl.Int8))
        
        Data_minor = Data_minor.drop(['renewed_otb_percent', 'renewed_otb_amount', 'different_otb_perc', 're_assigned_mix', 'new_otb_mix', 'revised_otb_amount'])

        print(Data_minor['check_kpi'], 'uncheck_check kpi')

        Data_major = DATA.filter(list(child.not_()))
        
        Data_major = Data_major.with_columns(check_kpi = pl.lit(0).cast(pl.Int8))
        
        Data_major = Data_major.drop(['renewed_otb_percent', 'renewed_otb_amount', 'different_otb_perc', 're_assigned_mix', 'new_otb_mix', 'revised_otb_amount'])
        
        print(Data_major['check_kpi'], 'uncheck_check kpi')
        
        DATA = pl.concat([Data_minor, Data_major])

        return DATA

    def kpi_inner_child_selection(self, data : pl.DataFrame, data_filter : dict, group : list, heirarchy : list):
        print('We are in kpi inner child selection')
        if data_filter['table_changes'] != {}:
            row = data_filter["table_changes"]["row"]
            columnID = data_filter["table_changes"]["columnId"]
            newValue = data_filter["table_changes"]["newValue"]

            
            columns_to_filter = []
            values_to_filter = []
            print(group, 'group in kpi')

            # Calculate the values to filter and columns to filter from data
            for i in group+heirarchy:
                if i in row:
                    columns_to_filter.append(i)
                    values_to_filter.append(data_filter['table_changes']["row"][i])
            print(columns_to_filter, 'the_cols_to_filter_in_kpi_child')
            print(values_to_filter, 'the_values_to_filter_in_kpi_child')
            
            # child to destribute kpi selection 
            kpi_child =None
            
            parent = None

            # create the kpi_child filter using col name and value name from data
            for col, val in zip(columns_to_filter, values_to_filter):
                if kpi_child is None:
                    kpi_child = data[col] == val
                    parent = None
                else:
                    next_child = data[col]==val
                    kpi_child = kpi_child & next_child

            return kpi_child, group
        elif data_filter['select_all_kpi'] != '': #or data_filter['select_all_kpi'] == '':
            print(group, 'group in kpi')
            if group == []:
                kpi_child = pl.Series([True])
            else:
                kpi_child = pl.Series(True for elem in range(len(data[group[-1]]))) 
                print(kpi_child, 'kpi_child in the expansion')

            return kpi_child, group


    def expand_hierarchy():

        
        return DATA, data, filter_condition


    def destribute_otb_(self, the_value :int|pl.Series|float, DATA : pl.DataFrame, group : list, data_filter :dict, heirarchy : list, sub_filter_state : bool):
        
  
        # columns_to_filter, filter_condition, group, last_filter = filters_for_expand_agg(DATA, data_filter, heirarchy, group)
        row = data_filter['expand']["row"]
        child,other_filter_condition, filter_condition,parent,columns_to_filter, \
            values_to_filter, group, DATA = Otb.table_change_filter(group, heirarchy, data_filter, DATA, row)
        # child = kpi_inner_child_selection(data, data_filter, group, heirarchy)
        increase = the_value - float(row['otb_amount'])
        print(type(child), columns_to_filter, values_to_filter,  'we are in destribute otb')
        if type(the_value) != type(DATA['otb_amount']):
            if child is None:
                print('child destribute_otb_ is empty')
            else:
                if sub_filter_state == True:
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


                        DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'otb_amount')
                        DATA_siblings = DATA.filter(list(parent.not_()))
                        DATA = pl.concat([DATA_siblings,DATA_parent])
                        summation = DATA["otb_amount"].sum()
                        DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))

                else:
                    print('destribute_otb_no_ SFS')
                    if len(columns_to_filter) == 1:
                        print('len(columns_to_filter) == 1:')
                        print(columns_to_filter, 'columns_to filter')

                        ''' to manage 1 inner block,if 1 group'''
                        DATA = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'otb_amount')
                        summation = DATA["otb_amount"].sum()
                        DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))
                    else:
                        print('len(columns_to_filter) is other')
                        print(columns_to_filter, 'columns_to filter2')

                        ''' to manage more than 1 inner block,if more than 1 group'''
                        DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'otb_amount')
                        DATA_siblings = DATA.filter(list(parent.not_()))
                        
                        # types =zip(DATA_siblings.columns, DATA_siblings)
                        DATA = pl.concat([DATA_siblings,DATA_parent])
                        summation = DATA["otb_amount"].sum()
                        DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))
        return DATA

    def destribute_otb_total(self, values : pl.DataFrame, DATA : pl.DataFrame, group, data_filter, heirarchy, sub_filter_state):
        
        row = data_filter['expand']["row"]
        the_value = values['revised_otb_amount'].sum()
        
        print(group, 'destribute_otb_total')
        # if list(row)[0] in heirarchy:
        #     Level_to_destribute = heirarchy.index(list(row)[0]) #must defined here
        # else:
        #     matching_elements = [element for element in reversed(group) if element in heirarchy]
        #     Level_to_destribute = heirarchy.index(matching_elements[0])
        

        child,other_filter_condition, filter_condition,parent,columns_to_filter, \
            values_to_filter, group,DATA = Otb.table_change_filter(group, heirarchy, data_filter, DATA, row)
        
        stripling = DATA['check_kpi'] == 10
        print(group,'group in destribute_otb_total')
        # increase = the_value - float(row['otb_amount'])
        
        # print(stripling.value_counts(), 'val_cnt_strpling')
        # print(Level_to_destribute, 'level to destribute_otb_total')
        # print(heirarchy[Level_to_destribute], 'this level')
        # for neonate in DATA[heirarchy[Level_to_destribute]].unique():
        #     minor =  DATA[heirarchy[Level_to_destribute]] == neonate
            # stripling = stripling|minor
        
        if sub_filter_state == True:
            if len(columns_to_filter) == 1:
                ''' to manage 1 inner block,if 1 group'''
                print('len(columns_to_filter) == 1:')
                print(columns_to_filter, 'columns_to filter')
                DATA = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(sibling_condition)),increase= increase, colID= columnID)
                data = DATA.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)
                DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))
            else:
                # when main on stores
                print('len(columns_to_filter) is other')
                print(columns_to_filter, 'columns_to filter2')
                # Level_to_destribute = group[-1]
                print(group, 'group in main group stores')
                lst_whl = []
                # print(values, Level_to_destribute)
                if row != {}:
                    for neonate in values[group[-1]]:

                        minor =  DATA[group[-1]] == neonate
                        others = DATA[group[-1]] != neonate
                        print(DATA.filter(minor)['Channel'].max())
                        print(DATA.filter(minor)['otb_amount'].sum(), 'checking otb amount matches the data otb amount')
                        the_new_value = values.filter(values[group[-1]]==neonate)['revised_otb_amount'].item()
                        print(the_new_value, 'new_vals')
                        old_otb_amount = values.filter(values[group[-1]]==neonate)['otb_amount'].item()
                        print(old_otb_amount, 'old_vals')
                        
                        increase = the_new_value - float(old_otb_amount)
                        grouped_df = DATA.filter(list(minor))
                        summation = grouped_df['otb_amount'].fill_nan(0).sum()
                    
                        increase = the_new_value - float(old_otb_amount)
                        grouped_df = DATA.filter(list(minor))
                        summation = grouped_df['otb_amount'].fill_nan(0).sum()
                        grouped_df = grouped_df.with_columns((grouped_df['otb_amount'] + (grouped_df['otb_amount']*increase)/summation).alias('otb_amount'))
                        lst_whl.append(grouped_df)
                        other_grouped_df=DATA.filter(list(others))
            
                    DATA = pl.concat(lst_whl)
                    summation = DATA["otb_amount"].sum()
                    DATA = DATA.with_columns(otb_percent = (pl.col('otb_amount')/pl.col('otb_amount').sum()).fill_nan(0).fill_null(0).replace({np.inf:0, -np.inf:0}))
                # else:


                # DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'otb_amount')
                # DATA_siblings = DATA.filter(list(parent.not_()))
                # DATA = pl.concat([DATA_siblings,DATA_parent])
                # summation = DATA["otb_amount"].sum()
                # DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))

        else:
            print('sub_filter_state is false')
            if list(row)[0] in heirarchy:
                Level_to_destribute = heirarchy.index(list(row)[0]) #must defined here
            else:
                matching_elements = [element for element in reversed(group) if element in heirarchy]
                Level_to_destribute = heirarchy.index(matching_elements[0])
        
            print('destribute_otb_no_ SFS')
            if len(columns_to_filter) == 1:
                print('len(columns_to_filter) == 1:')
                print(columns_to_filter, 'columns_to filter')
                print('updating multiple columns 1 col to filter')
                ''' to manage 1 inner block,if 1 group'''
                count = len(values)
                lst_whl = []
                for neonate in values[heirarchy[Level_to_destribute]]:

                    minor =  DATA[heirarchy[Level_to_destribute]] == neonate
                    others = DATA[heirarchy[Level_to_destribute]] != neonate
                    print(DATA.filter(minor)['Channel'].max())
                    print(DATA.filter(minor)['otb_amount'].sum(), 'checking otb amount matches the data otb amount')
                    the_new_value = values.filter(values[heirarchy[Level_to_destribute]]==neonate)['revised_otb_amount'].item()
                    print(the_new_value, 'new_vals')
                    old_otb_amount = values.filter(values[heirarchy[Level_to_destribute]]==neonate)['otb_amount'].item()
                    print(old_otb_amount, 'old_vals')
                    
                    increase = the_new_value - float(old_otb_amount)
                    grouped_df = DATA.filter(list(minor))
                    summation = grouped_df['otb_amount'].fill_nan(0).sum()
                    grouped_df = grouped_df.with_columns((grouped_df['otb_amount'] + (grouped_df['otb_amount']*increase)/summation).alias('otb_amount'))
                    lst_whl.append(grouped_df)
                    other_grouped_df=DATA.filter(list(others))
        
                DATA = pl.concat(lst_whl)
                summation = DATA["otb_amount"].sum()
                DATA = DATA.with_columns(otb_percent = (pl.col('otb_amount')/pl.col('otb_amount').sum()).fill_nan(0).fill_null(0).replace({np.inf:0, -np.inf:0}))
                # DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))
            else:
                print('len(columns_to_filter) is other')
                print(columns_to_filter, 'columns_to filter2')

                ''' to manage more than 1 inner block,if more than 1 group'''
                for neonate in values[heirarchy[Level_to_destribute]]:

                    minor =  DATA[heirarchy[Level_to_destribute]] == neonate
                    others = DATA[heirarchy[Level_to_destribute]] != neonate
                    print(DATA.filter(minor)['Channel'].max())
                    print(DATA.filter(minor)['otb_amount'].sum(), 'checking otb amount matches the data otb amount')
                    the_new_value = values.filter(values[heirarchy[Level_to_destribute]]==neonate)['revised_otb_amount'].item()
                    print(the_new_value, 'new_vals')
                    old_otb_amount = values.filter(values[heirarchy[Level_to_destribute]]==neonate)['otb_amount'].item()
                    print(old_otb_amount, 'old_vals')
                    
                    increase = the_new_value - float(old_otb_amount)
                    grouped_df = DATA.filter(list(minor))
                    summation = grouped_df['otb_amount'].fill_nan(0).sum()
                    grouped_df = grouped_df.with_columns((grouped_df['otb_amount'] + (grouped_df['otb_amount']*increase)/summation).alias('otb_amount'))
                    lst_whl.append(grouped_df)
                    other_grouped_df=DATA.filter(list(others))
        
                DATA = pl.concat(lst_whl)
                summation = DATA["otb_amount"].sum()
                DATA = DATA.with_columns(otb_percent = (pl.col('otb_amount')/pl.col('otb_amount').sum()).fill_nan(0).fill_null(0).replace({np.inf:0, -np.inf:0}))           
                
                
                # DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'otb_amount')
                # DATA_siblings = DATA.filter(list(parent.not_()))
                
                # # types =zip(DATA_siblings.columns, DATA_siblings)
                # DATA = pl.concat([DATA_siblings,DATA_parent])
                # summation = DATA["otb_amount"].sum()
                # DATA = DATA.with_columns(((DATA["otb_amount"]/summation)*100).alias('otb_percent'))
        return DATA

    def apply_kpi_for_main_data(self, DATA : pl.DataFrame, newValue : int, row : dict):

        if newValue == 1:
            otb_amount_summation = DATA['otb_amount'].sum()
            DATA = DATA.with_columns(check_kpi = pl.lit(1).cast(pl.Int8))
            DATA = DATA.with_columns(renewed_otb_percent = pl.col('coefficient_score_mix_percent').cast(pl.Float64))
            sum_of_transferred_coeff_score_mix_only = DATA['renewed_otb_percent'].sum()
            DATA = DATA.with_columns(different_otb_perc = ((pl.col('renewed_otb_percent'))-(pl.col('otb_percent'))).cast(pl.Float64))
            DATA = DATA.with_columns(re_assigned_mix = pl.lit(0).cast(pl.Float64))
            DATA = DATA.with_columns(new_otb_mix = (pl.col('otb_percent') - pl.col('re_assigned_mix')).cast(pl.Float64))
            DATA = DATA.with_columns(revised_otb_amount = ((otb_amount_summation * pl.col('new_otb_mix')).cast(pl.Float64))/100)
        if newValue == 0:
            DATA = DATA.with_columns(check_kpi = pl.lit(newValue).cast(pl.Int8))
            DATA = DATA.drop(['renewed_otb_percent', 'renewed_otb_amount', 'different_otb_perc', 're_assigned_mix', 'new_otb_mix', 'revised_otb_amount'])
        
        return DATA
