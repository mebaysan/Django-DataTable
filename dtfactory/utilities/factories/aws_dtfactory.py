import pandas as pd


class AWSDataTableFactory(object):
    def __init__(self, con):
        """Create an AWSDataTableFactory object

        AWS specific data table data generator
        """
        self.columns = []
        self.displayed_columns = []
        self.con = con

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
        limited_query = limited_query.split('where')[0]
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
        query += f" order by {order_column} {order_direction}"
        return query

    def get_row_numbered_query(self, query):
        ordered_query = f"""
                            with source as (
                            {query}
                            ),
                            numbered as (
                            select *, ROW_NUMBER () OVER () as "row_number" from source
                            )
                            select * from numbered    
                        """
        return ordered_query

    def get_row_number_filtered_query(self, query, start, length):
        query = f"{query} where row_number >= {start} and row_number <= {start + length}"
        return query

    def get_total_query(self, query):
        """ replace '*' in query to get total observation number in all table/view """
        return "count(*)".join(query.rsplit('*', 1))

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
        order_column = kwargs.get("order[0][column]", None)[0]
        order_column = self.columns[int(order_column)]  # which col
        order = kwargs.get("order[0][dir]", None)[0]  # asc or desc

        factory_filters = [f for f in kwargs if f.startswith(
            "factory_filter")]  # if we want to filter out the raw query, we have(!) to pass filter keywords with 'factory_filter' prefix

        ############ factory_filters applying ############

        if len(factory_filters) > 0:
            where_clause = "where "
            for ff in factory_filters:
                query_col = ff.split("factory_filter-")[1]
                query_val = kwargs.get(ff)[0]
                if query_val != "All":
                    if 'where' not in query:
                        query += f" where {query_col} = '{query_val}'"
                    else:
                        query += f" {query_col} = '{query_val}'"

        query = self.get_ordered_query(query, order_column, order)

        query = self.get_row_numbered_query(query)

        total = self.execute_query(self.get_total_query(query))  # total observation (all data, old query used)

        try:
            total = int(total.iloc[0, 0])
        except:  # null / None (there is no data)
            total = 0

        query = self.get_row_number_filtered_query(query, start, length)

        queryset = self.execute_query(query)

        if search_value:
            queryset = queryset[
                queryset.apply(
                    lambda row: row.astype(str).str.contains(search_value).any(), axis=1
                )
            ]

        count = total  # total observation after filtering

        queryset = queryset[self.displayed_columns]

        return {
            "data": queryset,
            "count": count,  # for page number => count / 10
            "total": total,
            "draw": draw,
            "start": start,
            "length": length,
        }
