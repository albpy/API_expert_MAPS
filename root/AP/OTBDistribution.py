import polars as pl
from .parameters import Otb, Parameters

hier_ap = ['Channel', 'Family', 'SubFamily', 'Supplier', 'Category','DOM_COMM','SubCategory']

gloabal_vars = Parameters()
Otb = Otb()

max_col = gloabal_vars.max_col
sum_col = gloabal_vars.sum_col
avg_col = gloabal_vars.avg_col
int_cols = gloabal_vars.int_cols
float_cols = gloabal_vars.float_cols
rank_col = gloabal_vars.rank_col
kpi_col = gloabal_vars.kpi_col
art = gloabal_vars.SCORES


class OTBDistribute:

    def destribute_otb(self, DATA : pl.DataFrame, DATA_2 : pl.DataFrame, item_flag : bool, selected_row : dict, SELECTION_DICT : dict, newValue : int):
        if item_flag: 
            item_code = selected_row['ITEMID']
            current_amount = selected_row["otb_amount"]
            # Because the binary logic inversed
            selected_element  = DATA["ITEMID"] == item_code # binary list holds the selected itemid wise ==> true equals no of ticks because ITEMID is unique
            
            deleted_elements = DATA["ITEMID"].is_in(SELECTION_DICT["item_code"]) # get full false at first attempt. Already deleted elements

            print('SELECTION_DICT is :', SELECTION_DICT, current_amount)

            other_elements = (selected_element | deleted_elements).not_() # Shifting the counts of selected elements in bool            
            if newValue == 0:
                SELECTION_DICT["item_code"].append(item_code)
                print('selection dict after appending', SELECTION_DICT)
                
                DATA_others = DATA.filter(list(other_elements)) # list of data with unselected elements
                DATA_others = Otb.drill_down_otb(DATA_others, "otb_amount", current_amount) # Destribute otb amount to other elements
                
                DATA_selected = DATA.filter(list(selected_element))
                DATA_selected = DATA_selected.with_columns(Check_box = pl.lit(newValue))
                DATA_selected = DATA_selected.with_columns(otb_amount = pl.lit(0.0))
                DATA_selected  = DATA_selected.with_columns(order_index=pl.lit(1))
                
                DATA_deleted = DATA.filter(list(deleted_elements))                
                DATA_deleted  = DATA_deleted.with_columns(order_index=pl.lit(1))

                DATA_others   = DATA_others.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64))
                DATA_deleted  = DATA_deleted.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64))
                DATA_selected = DATA_selected.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64))

                DATA = pl.concat([DATA_others,DATA_deleted,DATA_selected])
            else:
                SELECTION_DICT["item_code"].remove(item_code)
                print(SELECTION_DICT,"selection dict after unchecking")
                current_amount = DATA_2.filter(list(DATA_2["ITEMID"] == item_code))["otb_amount"][0]
                print(current_amount,"current amount on removal")
                # restore selected item amount and reduce the same amount of value from others 
                # also set selected element checkbox with newvalue
                DATA_others = DATA.filter(list(other_elements))
                DATA_others = Otb.drill_down_otb(DATA_others, "otb_amount", -current_amount)
                DATA_selected = DATA.filter(list(selected_element))
                DATA_selected = DATA_selected.with_columns(Check_box = pl.lit(newValue))
                DATA_selected = DATA_selected.with_columns(otb_amount = pl.lit(current_amount))
                # set order index
                DATA_selected  = DATA_selected.with_columns(order_index=pl.lit(1))
                deleted_elements = DATA["ITEMID"].is_in(SELECTION_DICT["item_code"])
                DATA_deleted = DATA.filter(list(deleted_elements))
                DATA_deleted  = DATA_deleted.with_columns(order_index=pl.lit(1))

                DATA_others   = DATA_others.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64))
                DATA_deleted  = DATA_deleted.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64))
                DATA_selected = DATA_selected.with_columns(Check_box = pl.col('Check_box').cast(pl.Float64))


                DATA = pl.concat([DATA_others,DATA_deleted,DATA_selected])

        else:
            pass

        return DATA

    def iter_filter(self, group,heirarchy,row,hier_ap,DATA, data_filter : dict):
        print('we are in iter filter')
        print(group, 'group_iter_filter')
        columns_to_filter = []
        values_to_filter = []
        # print(row,"ROWS")
        # print(heirarchy, 'heirarch')
        for i in group+heirarchy:
            if i in (row and hier_ap):
                columns_to_filter.append(i)
                values_to_filter.append(data_filter['table_changes']["row"][i])
                
                # print(i,"cols in the iter filter")
                # print(data_filter['table_changes']["row"][i], "values in the iter filter")
                # print(columns_to_filter,"COLUMNS")
                # print(values_to_filter,"VALUESSS")
                # print(hier_ap,"HEIRAPP")

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
        # print(hier_ap[-1], 'finding last one')
        return child,hier_ap[-1]

    def asm_destribute(self, DATA : pl.DataFrame,):

        asm_code = selected_row["AssemblyCodeNickName"]
        item_code = selected_row["ITEMID"]
        current_amount = selected_row["otb_amount"]


        return DATA