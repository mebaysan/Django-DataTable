import pandas as pd

# from helpers.datatable.format import format_numeric_cols


class AWSDataTableFactory(object):
    def __init__(self, con):
        """Create an AWSDataTableFactory object

        AWS specific data table data generator
        """
        self.columns = []
        self.displayed_columns = []
        self.con = con
        self.export_query = ""
        self.none_replacer = ""

    def execute_query(self, query, *args, **kwargs):
        """Get (extract) data from the source by executing the query with the engine

        Args:
            query (str): The query will be executed
            *args: args
            **kwargs: kwargs

        Returns:
            table_data (pd.DataFrame): Returns the data that extracted from AWS
        """

        table_data = pd.read_sql(con=self.con, sql=query, *args, **kwargs)
        return table_data

    def set_columns(self, query, col_count=None):
        """Set factory's columns

        Args:
            col_count (int): If it's defined, columns will be displayed by using this col_count
            query (str): to get columns from query

        Returns:
            It doesn't return anything. It sets the factory's columns.
        """
        limited_query = query.split("limit")[0]
        limited_query = limited_query.split("where")[0]
        limited_query += " limit 1"
        res = self.execute_query(limited_query)
        self.columns = [col for col in res.columns]
        if col_count is None:
            self.set_displayed_columns(self.columns[:4])
        else:
            self.set_displayed_columns(self.columns[:col_count])

    def set_displayed_columns(self, column_list):
        """
        Set factory displayed columns
        """
        self.displayed_columns = column_list

    def get_displayed_columns_for_data_table(self):
        return [{"data": col} for col in self.displayed_columns]

    def get_ordered_query(self, query, order_column, order_direction):
        query += f' order by "{order_column}" {order_direction}'
        return query

    def get_row_numbered_query(self, query, order_col, order):
        """
        query (str): the query will be modified
        order_col (str): row number window function will be applied on the column
        order (str): order type: asc | desc
        """
        ordered_query = f"""
                            with source as (
                            {query}
                            ),
                            numbered as (
                            select *, ROW_NUMBER () OVER (ORDER BY "{order_col}" {order}) as "row_number" from source
                            )
                            select * from numbered    
                        """
        return ordered_query

    def set_export_query(self, query):
        self.export_query = query

    def get_export_query(self):
        return self.export_query

    def get_search_key_query(self, query, search_key):
        # convert Turkish characters to English
        search_key = search_key.replace("ı", "i")
        search_key = search_key.replace("ğ", "g")
        search_key = search_key.replace("ü", "u")
        search_key = search_key.replace("ş", "s")
        search_key = search_key.replace("ö", "o")
        search_key = search_key.replace("ç", "c")
        search_key = search_key.replace("İ", "I")
        search_key = search_key.replace("Ğ", "G")
        search_key = search_key.replace("Ü", "U")
        search_key = search_key.replace("Ş", "S")
        search_key = search_key.replace("Ö", "O")
        search_key = search_key.replace("Ç", "C")

        if "where" not in query:
            where_clause = " where "
        else:
            where_clause = " and "

        conditions = []

        for col in self.displayed_columns:
            # If you’re using SQLAlchemy:
            # Use “%%” instead of “%” in your queries, because
            # a single “%” is used in Python string formatting.
            conditions.append(f"\"{col}\" ilike '%%{search_key}%%'")

        where_clause += " or ".join(conditions)
        query += where_clause
        return query
    

    def get_advanced_search_query(self, query, search_dict):
        """
        query (str): the query will be modified
        search_dict (dict): keys are column names, values are search keys and conditions
        """
        if "where" not in query.lower():
            where_clause = " WHERE "
        else:
            where_clause = " AND "

        conditions = []

        for col, search_key in search_dict.items():
            col = col.split("-")[1]
            value, condition = search_key[0].split("baysansoftidentifier")
            value = value.strip("'")
            conditions.append(f"{col} {condition} '{value}'")

        where_clause += " AND ".join(conditions)
        query += where_clause
        return query


    def get_row_number_filtered_query(self, query, start, length):
        query = (
            f"{query} where row_number >= {start} and row_number <= {start + length}"
        )
        return query

    def get_total_query(self, query):
        """replace '*' in query to get total observation number in all table/view"""
        return "count(*)".join(query.rsplit("*", 1))

    def set_none_replacer(self, none_replacer=""):
        """
        set self.none_replacer to use in "fill_NONE_values" method
        """
        self.none_replacer = none_replacer

    def fill_NONE_values(self, queryset):
        """
        Replace "None" with "none_replacer"
        none_replacer default is '' comes from "self"
        """
        return queryset.fillna(self.none_replacer)

    def replace_EMPTY_values(self, queryset):
        """
        Replace "''" with "none_replacer"
        none_replacer default is '' comes from "self"
        """
        return queryset.replace("", self.none_replacer)

    def filter_by_request_args(self, query, **kwargs):
        """Filter and sort the data by using request.GET parameters

        Args:
            **kwargs: request.GET

        Returns:
            dict: to use in the frontend with ajax data table requests
        """
        draw = int(kwargs.get("draw", None)[0])
        length = int(kwargs.get("length", None)[0])
        start = int(kwargs.get("start", None)[0])
        search_value = kwargs.get("search[value]", None)[0]

        order_column_number = kwargs.get("order[0][column]", None)[0]
        # order_column = self.columns[int(order_column_number)] ## which col
        # which col (this is exact column number on the web table!)
        order_column = kwargs.get(f"columns[{order_column_number}][data]", None)[0]

        order = kwargs.get("order[0][dir]", None)[0]  # asc or desc

        factory_filters = [
            f for f in kwargs if f.startswith("factory_filter")
        ]  # if we want to filter out the raw query, we have(!) to pass filter keywords with 'factory_filter' prefix


        advanced_filter_parameters = {
            key: value
            for key, value in kwargs.items()
            if key.startswith("advanced_filter")
        }
        # ?ajax_factory_loader=true&advanced_filter-my_column='filter_value'baysansoftidentifier>= (condition)
        if advanced_filter_parameters:
            query = self.get_advanced_search_query(query, advanced_filter_parameters)


        ############ factory_filters applying ############
        if len(factory_filters) > 0:
            where_clause = "where "
            for ff in factory_filters:
                query_col = ff.split("factory_filter-")[1]
                query_val = kwargs.get(ff)[0]
                if query_val != "All":
                    if "where" not in query:
                        query += f" where {query_col} = '{query_val}'"
                    else:
                        query += f" and {query_col} = '{query_val}'"

        if search_value:
            query = self.get_search_key_query(query, search_value)

        query = self.get_ordered_query(query, order_column, order)

        query = self.get_row_numbered_query(query, order_column, order)

        export_query = query  # export query

        try:
            total_query = self.get_total_query(query)
            total = self.execute_query(
                total_query
            )  # total observation (all data, old query used)
            total = int(total.iloc[0, 0])
        except Exception as e:  # null / None (there is no data)
            total = 0

        query = self.get_row_number_filtered_query(query, start, length)

        queryset = self.execute_query(query)

        queryset = self.fill_NONE_values(queryset)  # "None" values break datatable.js

        queryset = self.replace_EMPTY_values(
            queryset
        )  # replace empty "''" values with none replacer

        queryset = queryset[self.displayed_columns]

        # format the values
        # queryset = format_numeric_cols(queryset)

        return {
            "data": queryset,
            "count": total,  # for page number => count / 10, total observation after filtering
            "total": total,
            "draw": draw,
            "start": start,
            "length": length,
            "export_query": export_query,
        }
