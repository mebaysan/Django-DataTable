from django.urls import path
from . import views

app_name = "dtfactory"

urlpatterns = [
    path(
        "",
        views.DataTableView.as_view(
            page_name="Data Table Demo",
            query_engine="aws",
            query=f"select * from QUERY",
            permission="custom_app.can_view_data_table_page",
            filters=[
                {'filter_column': 'custom_region', 'display_name': 'Region', 'type': 'static',
                 'filter_values': ['All', 'TR', 'EN']}
            ],
            is_active=True,
            default_displayed_columns=[
                "col_1",
                "col_2",
            ],
            none_replacer="",
        ),
        name="dt_demo",
    ),
]
