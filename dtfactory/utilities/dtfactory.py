import pandas as pd
from .factories.aws_dtfactory import AWSDataTableFactory


class DataTableFactory(object):
    """Data Table Factory class to generate a related sub data table factory class

    """

    @staticmethod
    def get_factory(query_engine):
        """Get a query_engine related data table factory object

        Args:
            query_engine (str): [aws] To get related factory class
        Returns:
            an object that is created by using query_engine related data table factory class
        """
        if query_engine == "aws":
            return AWSDataTableFactory(query_engine)
        else:
            return pd.DataFrame()
