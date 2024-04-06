import polars as pl
import numpy as np
import traceback
import subprocess

import time
from .parameters import OTB
from .schemas import Filters
from typing import Dict, List, Optional, Union, Tuple

Otb = OTB()
# Global variables are defined in one class

# HEIRARCHY = gloabal_vars.heirarchy
# print(HEIRARCHY, 'the heirarchy')
class kp_Operations:
    #
    def calculate_revised_budget(self, DATA : pl.DataFrame, child : pl.Series, other_filter_condition : pl.Series, DATA_KPI : pl.DataFrame, row : dict =None):
  
        print('we are in calculate revised budget')
        #--------------
        budget_amount_summation = DATA['budget_amount'].sum()
        #--------------
        # print(budget_amount_summation, 'budget_amount_summation')
        # print(child, 'child in calculate revised budget')
        # print(type(child), 'child in calculate revised budget')
        # print(other_filter_condition, 'kpi_other')
        # print(type(child), 'kpi_now')

        print(other_filter_condition.value_counts(), 'kpi_other')
        print(child.value_counts(), 'kpi_now')

        Data_major = DATA.filter(list(child))

        Data_siblings = DATA.filter(pl.col('Check_box') != 1)

        Data_major = Data_major.with_columns(Check_box = pl.lit(1).cast(pl.Int8))
        try:
            Data_armed = pl.concat([Data_major, Data_siblings])
        except Exception as e:
            print(e, 'calculate_revised_error')
            print(traceback.format_exc())
        Data_armed_1 = Data_armed.filter(pl.col('Check_box')==1)

        Data_armed_0 = Data_armed.filter(pl.col('Check_box')==0)

        Data_armed_1 = Data_armed_1.with_columns(renewed_budget_percent = pl.col('coefficient_score_mix_percent').replace({np.inf:0,-np.inf:0}).fill_nan(0).fill_null(0))

        value_to_find_net_mix = (Data_armed_1['budget_percent']-Data_armed_1['renewed_budget_percent']).sum()

        print(value_to_find_net_mix, 'value_to_find_net_mix')

        Data_armed_1 = Data_armed_1.with_columns(re_assigned_mix = pl.lit(0).cast(pl.Float64))

        Data_armed_1 = Data_armed_1.with_columns(new_budget_mix = pl.col('renewed_budget_percent')+pl.col('re_assigned_mix'))
        """##User unselected data##"""

        Data_minor = DATA.filter(list(child.not_()))

        Data_minor = Data_minor.with_columns(Check_box = pl.lit(0).cast(pl.Int8))

        # Here renewed otb percent remains same as otb percent because user not get select the kpi metrics
        
        Data_minor = Data_minor.with_columns(renewed_budget_percent = pl.col('budget_percent').cast(pl.Float64))

        sum_of_remaining_budget_perc = Data_minor['budget_percent'].sum()

        print(sum_of_remaining_budget_perc, 'sum_of_remaining_budget_perc')        

        Data_minor = Data_minor.with_columns(re_assigned_mix =((value_to_find_net_mix) * (pl.col('renewed_budget_percent').cast(pl.Float64)/sum_of_remaining_budget_perc)).cast(pl.Float64).replace({np.inf:0, -np.inf:0}).fill_nan(0))

        Data_minor = Data_minor.with_columns(new_budget_mix = pl.col('budget_percent')+pl.col('re_assigned_mix'))

        DATA = pl.concat([Data_armed_1, Data_minor])
    
        print((DATA['budget_percent']-DATA['renewed_budget_percent']).sum(), 'cdsdsdfsdfsdfds')        

        DATA = DATA.with_columns(revised_budget_amount = (pl.col('new_budget_mix')* (pl.col('budget_amount').sum()))/100)

    
        # print(child.value_counts(), 'chuld')

        return DATA


    def user_uncheck_kpi_selection(self, DATA : pl.DataFrame, child : pl.Series, newValue: int = None):
       
        
        Data_minor = DATA.filter(list(child)) 
        
        Data_minor = Data_minor.with_columns(Check_box = pl.lit(newValue).cast(pl.Int8))
        
        Data_minor = Data_minor.drop(['renewed_budget_percent', 'renewed_otb_amount', 'different_otb_perc', 're_assigned_mix', 'new_otb_mix', 'revised_budget_amount'])

        print(Data_minor['Check_box'], 'uncheck_check kpi')

        Data_major = DATA.filter(list(child.not_()))
        
        Data_major = Data_major.with_columns(Check_box = pl.lit(0).cast(pl.Int8))
        
        Data_major = Data_major.drop(['renewed_otb_percent', 'renewed_otb_amount', 'different_otb_perc', 're_assigned_mix', 'new_otb_mix', 'revised_budget_amount'])
        
        print(Data_major['Check_box'], 'uncheck_check kpi')
        
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
                # print(kpi_child, 'kpi_child in the expansion')

            return kpi_child, group



    def destribute_otb_(self, the_value :int|pl.Series|float, DATA : pl.DataFrame, group : list, data_filter :dict, heirarchy : list, sub_filter_state : bool):
        
        # columns_to_filter, filter_condition, group, last_filter = filters_for_expand_agg(DATA, data_filter, heirarchy, group)
        row = data_filter['expand']["row"]
        if row == {}:
            row['budget_amount'] = DATA['budget_amount'].sum()
            print(row, 'row of groupby_sts')
        print(row, 'destribute_otb_')
        child,other_filter_condition, filter_condition,parent,columns_to_filter, \
            values_to_filter, group, DATA = Otb.table_change_filter(group, heirarchy, data_filter, DATA, row)
        # child = kpi_inner_child_selection(data, data_filter, group, heirarchy)
        increase = the_value - float(row['budget_amount'])
        print(type(child), columns_to_filter, values_to_filter,  'we are in destribute otb')
        if type(the_value) != type(DATA['budget_amount']):
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
                        DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('otb_percent'))
                    else:
                        print('len(columns_to_filter) is other')
                        print(columns_to_filter, 'columns_to filter_1')
                        print(1)

                        DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'budget_amount')
                        DATA_siblings = DATA.filter(list(parent.not_()))
                        DATA = pl.concat([DATA_siblings,DATA_parent])
                        summation = DATA["budget_amount"].sum()
                        DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('otb_percent'))

                else:
                    print('destribute_otb_no_ SFS')
                    if len(columns_to_filter) == 1:
                        print('len(columns_to_filter) == 1:')
                        print(columns_to_filter, 'columns_to filter')

                        ''' to manage 1 inner block,if 1 group'''
                        DATA = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'budget_amount')
                        summation = DATA["budget_amount"].sum()
                        DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('otb_percent'))
                    else:
                        print('len(columns_to_filter) is other')
                        print(columns_to_filter, 'columns_to filter_2')
                        print(2)

                        ''' to manage more than 1 inner block,if more than 1 group'''
                        DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'budget_amount')
                        DATA_siblings = DATA.filter(list(parent.not_()))
                        
                        # types =zip(DATA_siblings.columns, DATA_siblings)
                        DATA = pl.concat([DATA_siblings,DATA_parent])
                        summation = DATA["budget_amount"].sum()
                        DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('otb_percent'))
        return DATA

    def destribute_otb_total(self, values : pl.DataFrame, DATA : pl.DataFrame, group, data_filter, heirarchy, sub_filter_state):
        
        row = data_filter['expand']["row"]
        the_value = values['revised_budget_amount'].sum()
        
        print(group, 'destribute_otb_total')        

        child,other_filter_condition, filter_condition,parent,columns_to_filter, \
            values_to_filter, group,DATA = Otb.table_change_filter(group, heirarchy, data_filter, DATA, row)
        
        stripling = DATA['Check_box'] == 10
        print(group,'group in destribute_otb_total')
        
        if sub_filter_state == True:
            if len(columns_to_filter) == 1:
                ''' to manage 1 inner block,if 1 group'''
                print('len(columns_to_filter) == 1:')
                print(columns_to_filter, 'columns_to filter')
                DATA = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(sibling_condition)),increase= increase, colID= columnID)
                data = DATA.filter(list(filter_condition)).group_by(list(set(group))).agg(agg_dict)
                DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('otb_percent'))
            else:
                #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                """Also set foot in here when select sub filter in expand sts"""
                #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                # when main on stores
                print('len(columns_to_filter) is other')
                print(columns_to_filter, 'columns_to filter_3')
                print(3)

                # Level_to_destribute = group[-1]
                print(group, 'group in main group stores')
                lst_whl = []
                # print(values, Level_to_destribute)
                print(values, 'sub_filter_error_Store_caught_here')
                print(DATA.columns, 'cols sub_filter_error_Store_caught_here')
                print(row, 'row to understand subfilter state')
                if row != {}:
                    if group[-1] not in values.columns:
                        print("Not In value columns")
                        for neonate in DATA[group[-1]].unique():
                            print(neonate)
                            minor =  DATA[group[-1]] == neonate
                            others = DATA[group[-1]] != neonate
                            # print(DATA.filter(minor)['Channel'].max())
                            # print(DATA.filter(minor)['budget_amount'].sum(), 'checking otb amount matches the data otb amount')
                            the_new_value = DATA.filter(DATA[group[-1]]==neonate)['revised_budget_amount'].sum()#.item()
                            # print(the_new_value, 'new_vals')
                            old_otb_amount = DATA.filter(DATA[group[-1]]==neonate)['budget_amount'].sum()#.item()
                            # print(old_otb_amount, 'old_vals')
                            
                            increase = the_new_value - float(old_otb_amount)
                            grouped_df = DATA.filter(list(minor))
                            summation = grouped_df['budget_amount'].fill_nan(0).sum()
                        
                            # increase = the_new_value - float(old_otb_amount)
                            grouped_df = DATA.filter(list(minor))
                            summation = grouped_df['budget_amount'].fill_nan(0).sum()
                            grouped_df = grouped_df.with_columns((grouped_df['budget_amount'] + (grouped_df['budget_amount']*increase)/summation).alias('budget_amount'))
                            lst_whl.append(grouped_df)
                            other_grouped_df=DATA.filter(list(others))
                    else:
                        print("In value columns")
                        for neonate in values[group[-1]]:
                            print(neonate)
                            minor =  values[group[-1]] == neonate
                            others = values[group[-1]] != neonate
                            # print(values.filter(minor)['Channel'].max())
                            print(values.filter(minor)['budget_amount'].sum(), 'checking otb amount matches the data otb amount')
                            the_new_value = values.filter(values[group[-1]]==neonate)['revised_budget_amount'].item()
                            print(the_new_value, 'new_vals')
                            old_otb_amount = values.filter(values[group[-1]]==neonate)['budget_amount'].item()
                            print(old_otb_amount, 'old_vals')
                            
                            increase = the_new_value - float(old_otb_amount)
                            grouped_df = values.filter(list(minor))
                            summation = grouped_df['budget_amount'].fill_nan(0).sum()
                        
                            # increase = the_new_value - float(old_otb_amount)
                            grouped_df = values.filter(list(minor))
                            summation = grouped_df['budget_amount'].fill_nan(0).sum()
                            grouped_df = grouped_df.with_columns((grouped_df['budget_amount'] + (grouped_df['budget_amount']*increase)/summation).alias('budget_amount'))
                            lst_whl.append(grouped_df)
                            other_grouped_df=values.filter(list(others))          
                    DATA = pl.concat(lst_whl)

                    summation = DATA["budget_amount"].sum()
                    DATA = DATA.with_columns(budget_percent = (pl.col('budget_amount')/pl.col('budget_amount').sum()).fill_nan(0).fill_null(0).replace({np.inf:0, -np.inf:0}))
                # else:


                # DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'budget_amount')
                # DATA_siblings = DATA.filter(list(parent.not_()))
                # DATA = pl.concat([DATA_siblings,DATA_parent])
                # summation = DATA["budget_amount"].sum()
                # DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('otb_percent'))
# Generally we falls here due to the absence of secondary filter
        else:
            print('sub_filter_state is false')
            if list(row)[0] in heirarchy:
                Level_to_destribute = heirarchy.index(list(row)[0]) #must defined here
            else:
                matching_elements = [element for element in reversed(group) if element in heirarchy]
                Level_to_destribute = heirarchy.index(matching_elements[0])
        
            print('destribute_otb_no_ SFS')
            # Generally we falls here... 
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
                    # print(DATA.filter(minor)['Channel'].max())
                    # print(DATA.filter(minor)['budget_amount'].sum(), 'checking otb amount matches the data otb amount')
                    the_new_value = values.filter(values[heirarchy[Level_to_destribute]]==neonate)['revised_budget_amount'].item()
                    # print(the_new_value, 'new_vals')
                    old_otb_amount = values.filter(values[heirarchy[Level_to_destribute]]==neonate)['budget_amount'].item()
                    # print(old_otb_amount, 'old_vals')
                    
                    increase = the_new_value - float(old_otb_amount)
                    grouped_df = DATA.filter(list(minor))
                    summation = grouped_df['budget_amount'].fill_nan(0).sum()
                    grouped_df = grouped_df.with_columns((grouped_df['budget_amount'] + (grouped_df['budget_amount']*increase)/summation).alias('budget_amount'))
                    lst_whl.append(grouped_df)
                    other_grouped_df=DATA.filter(list(others))
        
                DATA = pl.concat(lst_whl)
                summation = DATA["budget_amount"].sum()
                DATA = DATA.with_columns(otb_percent = (pl.col('budget_amount')/pl.col('budget_amount').sum()).fill_nan(0).fill_null(0).replace({np.inf:0, -np.inf:0}))
                # DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('otb_percent'))
            else:
                lst_whl = []
                print('len(columns_to_filter) is other')
                print(columns_to_filter, 'columns_to filter_4')
                print(4)

                ''' to manage more than 1 inner block,if more than 1 group'''
                for neonate in values[heirarchy[Level_to_destribute]]:

                    minor =  DATA[heirarchy[Level_to_destribute]] == neonate
                    others = DATA[heirarchy[Level_to_destribute]] != neonate
                    # print(DATA.filter(minor)['Channel'].max())
                    # print(DATA.filter(minor)['budget_amount'].sum(), 'checking otb amount matches the data otb amount')
                    the_new_value = values.filter(values[heirarchy[Level_to_destribute]]==neonate)['revised_budget_amount'].item()
                    # print(the_new_value, 'new_vals')
                    old_otb_amount = values.filter(values[heirarchy[Level_to_destribute]]==neonate)['budget_amount'].item()
                    # print(old_otb_amount, 'old_vals')
                    
                    increase = the_new_value - float(old_otb_amount)
                    grouped_df = DATA.filter(list(minor))
                    summation = grouped_df['budget_amount'].fill_nan(0).sum()
                    grouped_df = grouped_df.with_columns((grouped_df['budget_amount'] + (grouped_df['budget_amount']*increase)/summation).alias('budget_amount'))
                    lst_whl.append(grouped_df)
                    other_grouped_df=DATA.filter(list(others))
        
                DATA = pl.concat(lst_whl)
                summation = DATA["budget_amount"].sum()
                DATA = DATA.with_columns(otb_percent = (pl.col('budget_amount')/pl.col('budget_amount').sum()).fill_nan(0).fill_null(0).replace({np.inf:0, -np.inf:0}))           
                
                
                # DATA_parent = Otb.change_percent(grouped_df=DATA.filter(list(child)),other_grouped_df=DATA.filter(list(other_filter_condition)),increase= increase, colID= 'budget_amount')
                # DATA_siblings = DATA.filter(list(parent.not_()))
                
                # # types =zip(DATA_siblings.columns, DATA_siblings)
                # DATA = pl.concat([DATA_siblings,DATA_parent])
                # summation = DATA["budget_amount"].sum()
                # DATA = DATA.with_columns(((DATA["budget_amount"]/summation)*100).alias('otb_percent'))
        return DATA

    def apply_kpi_for_main_data(self, DATA : pl.DataFrame, newValue : int, row : dict):

        if newValue == 1:
            # print('new_value_1')
            budget_amount_summation = DATA['budget_amount'].sum()
            DATA = DATA.with_columns(Check_box = pl.lit(1).cast(pl.Int8))
            DATA = DATA.with_columns(renewed_budget_percent = pl.col('coefficient_score_mix_percent').cast(pl.Float64))
            sum_of_transferred_coeff_score_mix_only = DATA['renewed_budget_percent'].sum()
            DATA = DATA.with_columns(different_budget_perc = ((pl.col('renewed_budget_percent'))-(pl.col('budget_percent'))).cast(pl.Float64))
            DATA = DATA.with_columns(re_assigned_mix = pl.lit(0).cast(pl.Float64))
            DATA = DATA.with_columns(new_budget_mix = (pl.col('budget_percent') - pl.col('re_assigned_mix')).cast(pl.Float64))
            DATA = DATA.with_columns(revised_budget_amount = ((budget_amount_summation * pl.col('new_budget_mix')).cast(pl.Float64))/100)
        if newValue == 0:

            DATA = DATA.with_columns(Check_box = pl.lit(0).cast(pl.Int8))
            DATA = DATA.drop(['renewed_budget_percent', 'renewed_budget_amount', 'different_budget_perc', 're_assigned_mix', 'new_budget_mix', 'revised_budget_amount'])
        
        return DATA

    def bottom_kpi(self, data : pl.DataFrame, data_kpi : pl.dataframe, data_filter : dict, group : list, heirarchy : list):
        row = data_filter["table_changes"]["row"]
        columnID = data_filter["table_changes"]["columnId"]
        newValue = data_filter["table_changes"]["newValue"]
        print(row, 'checking row for passed revised')
        if columnID == 'Check_box':
            newValue_kpi = newValue
            child, group = self.kpi_inner_child_selection(data, data_filter, group, heirarchy)
            data = Otb.call_kpi(data, data_filter)
#                   Include main group
            if child is not None:
                if newValue_kpi == 1:                
                    # if scores_m != []:
                    data = self.calculate_revised_budget(data, child, data_kpi, row)
                if newValue_kpi == 0:
                    # if scores_m != []:
                    data = self.user_uncheck_kpi_selection(data, child, newValue_kpi)
            return data
        else:
            return data

    def kpi_revision_initialize(self, DATA: pl.DataFrame, data :pl.DataFrame, new_revised : pl.DataFrame|int|float, group : list, data_filter : dict,
                                        heirarchy : list, sub_filter_state : bool, do_revise_dict : dict,filter_condition : [Optional] =None):
        filters = Filters(**data_filter)
        if type(new_revised['revised_otb']) != type(pl.DataFrame({'check' : [1,2,3]})):
            ida_of_otb_mix = data.find_idx_by_name('budget_amount')
            data = data.with_columns(budget_amount = (data['otb_percent'] * new_revised['revised_otb'])/100)
            print(group, 'group at do revise 1')
            filter_condition,sub_filter_state,group = Otb.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)
            DATA = self.destribute_otb_(new_revised['revised_otb'], DATA, group, data_filter, heirarchy, sub_filter_state)
            do_revise_dict['do_revise'] = False
        elif type(new_revised['revised_otb']) == type(pl.DataFrame({'check' : [1,2,3]})):
            row = data_filter["expand"]['row']
            ida_of_otb_mix = data.find_idx_by_name('budget_amount')
            print(group, 'group at do revise 2')
            # print(data.columns, 'columns_of_data_to_transfer')
            # print(want_to_update_new_otb_mix['update'].columns, 'the saved rev otb data')
            # print(new_revised['revised_otb'].columns, 'the saved rev otb data cols')
            # print(new_revised['revised_otb'], 'the saved mix data')
            #------------------------need to 
            # user_selected_rows_ = new_revised['revised_otb'].filter(new_revised['revised_otb'][list(row)[0]] == row[list(row)[0]])['revised_budget_amount'].item()
            # data = data.with_columns(budget_amount = (data['otb_percent'] * user_selected_rows_)/100)
            #------------------------need to
            filter_condition,sub_filter_state,group = Otb.apply_secondary_filters(DATA,filters,sub_filter_state,group,filter_condition)
            DATA = self.destribute_otb_total(new_revised['revised_otb'], DATA, group, data_filter, heirarchy, sub_filter_state)
            do_revise_dict['do_revise'] = False
                                
        return DATA, data, do_revise_dict['do_revise']#, do_revise_dict

    def kpi_for_all(self, data : pl.DataFrame, data_filter : dict, group : list, heirarchy : list):

        if data_filter['select_all_kpi'] == True and 'coefficient_score_mix_percent' in data.columns:
            
            print('select all kpi true')
            # The child will be of the condition to select full and apply the integration totaly                
            row = None
            data_kpi = data.clone()
            child, group = self.kpi_inner_child_selection(data, data_filter, group, heirarchy)
            data = data.with_columns(Check_box = pl.lit(1).cast(pl.Int8))
            data = Otb.call_kpi(data, data_filter)
            data = self.calculate_revised_budget(data, child, data_kpi, row)
        
        elif data_filter['select_all_kpi'] == False and data_filter['table_changes']=={}:

            print('select all kpi false')
            child, group = self.kpi_inner_child_selection(data, data_filter, group, heirarchy)
            data = data.with_columns(Check_box = pl.lit(0).cast(pl.Int8))
            data = Otb.call_kpi(data, data_filter)
            newValue_kpi = 0
            data = self.user_uncheck_kpi_selection(data, child, newValue_kpi)

        return data

# calculate_revised_budget  -> # kpi will calculate the revised budget for selected column
# kpi_for_all               
# kpi_revision_initialize
# bottom_kpi
# apply_kpi_for_main_data
# destribute_otb_total      -> Destribute otb to all rows user selected
# destribute_otb_           -> Destribute otb in the selected row
# kpi_inner_child_selection -> Select the child for kpi
# user_uncheck_kpi_selection-> Unselect the kpi to previous state
