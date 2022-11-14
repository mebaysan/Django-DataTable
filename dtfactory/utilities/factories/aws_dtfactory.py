from sqlalchemy import create_engine
import pandas as pd


def get_redshift_con():
    user = ""
    password = ""
    host = ""
    port = ""
    database = ""
    return create_engine(
        f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    )


class AWSDataTableFactory(object):
    def __init__(self, query_engine):
        """Create an AWSDataTableFactory object

        AWS specific data table data generator
        Args:
            query_engine (str): "aws"
        """
        self.query_engine = query_engine
        self.query = None
        self.columns = []
        self.displayed_columns = []

    def set_query(self, query):
        """Set factory's query

        Args:
            query (str): The query will be executed

        Returns:
            It doesn't return anything. It sets the factory's query.
        """
        self.query = query
        self.set_columns()

    def set_columns(self):
        """Set factory's columns

        Returns:
            It doesn't return anything. It sets the factory's columns.
        """
        limited_query = self.query.split('limit')[0]
        limited_query += " limit 1"
        res = self.get_data(limited_query)
        self.columns = [col for col in res.columns]
        self.displayed_columns = self.columns[:4]

    def set_displayed_columns(self, column_list):
        """Set factory's displayed columns

        Args:
            column_list (list): List of columns
        """
        self.displayed_columns = column_list

    def get_displayed_columns_for_data_table(self):
        """Get data table columns for AJAX request

        Returns:
            list: List of dictionaries
        """
        return [{'data': col} for col in self.displayed_columns]

    def get_data(self, query=None, *args, **kwargs):
        """Get (extract) data from the source by executing the query with the engine

        Args:
            query (str): The query will be executed
            *args:
            **kwargs:

        Returns:
            table_data (pd.DataFrame): Returns the data that extracted from AWS
        """
        if self.query is None:
            raise NotImplementedError(
                'Firstly you have to set a query for the engine')
        con = get_redshift_con()
        if query is None:
            table_data = pd.read_sql(con=con, sql=self.query, *args, **kwargs)
        else:
            table_data = pd.read_sql(con=con, sql=query, *args, **kwargs)
        return table_data

    def filter_by_request_args(self, **kwargs):
        """Filter and sort the data by using request.GET parameters

        Args:
            **kwargs: request.GET

        Returns:
            dict: to use in the frontend with ajax data table requests
        """
        draw = int(kwargs.get('draw', None)[0])
        length = int(kwargs.get('length', None)[0])
        start = int(kwargs.get('start', None)[0])
        search_value = kwargs.get('search[value]', None)[0]
        order_column = kwargs.get('order[0][column]', None)[0]
        order = kwargs.get('order[0][dir]', None)[0]  # asc or desc
        factory_filters = [f for f in kwargs if f.startswith('factory_filter')]
        order_column = self.columns[int(order_column)]
        queryset = self.get_data()

        ############ factory_filters applying ############
        if len(factory_filters) > 0:
            for ff in factory_filters:
                query_col = ff.split('factory_filter-')[1]
                query_val = kwargs.get(ff)[0]
                if query_val != 'All':
                    queryset = queryset[queryset[query_col] == query_val]

        total = len(queryset)
        if search_value:
            queryset = queryset[queryset.apply(lambda row: row.astype(
                str).str.contains(search_value).any(), axis=1)]

        count = len(queryset)

        queryset.sort_values(
            by=order_column, ascending=False if order == 'desc' else True, inplace=True)

        queryset = queryset[self.displayed_columns]

        return {
            'data': queryset,
            'count': count,
            'total': total,
            'draw': draw,
            'start': start,
            'length': length
        }
