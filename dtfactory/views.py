from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from .utilities.dtfactory import DataTableFactory
from django.utils.decorators import method_decorator
from django.core.exceptions import PermissionDenied
from django.http import JsonResponse


@method_decorator(login_required, name="dispatch")
class DataTableView(View):
    """Data table class based view
    This view can get the parameters below in the as_view function  to use in the view
    Args:
        page_name (str): To display in page
        query_engine (str): [aws] To create a DataTableFactory for querying the data source
        query (str): To execute the query by using the connection that is created with query_engine
        query_parameters (dict): Additional parameters to pass into to exec_sql method of the factory
        permission (str): The needed permission to access the url
    """
    page_name = ""
    query_engine = ""
    query = ""
    query_parameters = {}
    permission = ""
    filters = []

    def get(self, request, *args, **kwargs):
        data_table_factory = DataTableFactory.get_factory(self.query_engine)
        data_table_factory.set_columns(self.query)

        factory_displayed_columns = [
            *request.GET.getlist("factory_columns"),
            *request.GET.getlist("amp;factory_columns"),
        ]
        if len(factory_displayed_columns) != 0:
            # set displayed columns
            data_table_factory.set_displayed_columns(factory_displayed_columns)

        if request.GET.get("ajax_factory_loader", None) is not None:
            filter_args = data_table_factory.filter_by_request_args(self.query, **request.GET)
            data = filter_args["data"].to_dict(orient="records")

            result = {
                "data": data,
                "draw": filter_args["draw"],
                "recordsTotal": filter_args["total"],
                "recordsFiltered": filter_args["count"],
            }
            return JsonResponse(result)

        return render(
            request,
            "datagateway/data_table.html",
            context={
                "page_title": self.page_name,
                "filters": self.filters,
                "factory": data_table_factory,
                "is_active": self.is_active,
            },
        )

    def dispatch(self, request, *args, **kwargs):
        if not request.user.has_perm(self.permission):
            raise PermissionDenied(
                "You do not have permission to view this page")
        return super().dispatch(request, *args, **kwargs)
