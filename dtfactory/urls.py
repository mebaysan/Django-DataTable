from django.urls import path
from . import views

app_name = "dtfactory"

urlpatterns = [
    path(
        "human-resources/",
        views.DataTableView.as_view(
            page_name="Data Table Demo",
            query_engine="aws",
            query=f"select * from QUERY",
            permission="management.can_view_data_table_page",
            filters=[
                {'filter_column': 'custom_region', 'display_name': 'Region', 'type': 'static',
                 'filter_values': ['All', 'TR', 'EN']}
            ]
        ),
        name="dt_demo",
    ),
]
