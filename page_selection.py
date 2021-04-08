  
# Code source: https://dash-bootstrap-components.opensource.faculty.ai/examples/simple-sidebar/
import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px
from dash.dependencies import Input, Output
import pandas as pd
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash_extensions import Download
import dash_table
# data source: https://www.kaggle.com/chubak/iranian-students-from-1968-to-2017
# data owner: Chubak Bidpaa
# df = pd.read_csv('https://raw.githubusercontent.com/Coding-with-Adam/Dash-by-Plotly/master/Bootstrap/Side-Bar/iranian_students.csv')
# print(df)
df = pd.read_csv("data.csv")
print(df)
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
                 meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                 )


# styling the sidebar
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

# padding for the page content
CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}

table = dash_table.DataTable(
    id="table",
    # columns=[{"name": i, "id": i} for i in df.columns],
    # data=df.to_dict("records"),
    export_format="xlsx", 
    # This will make an export button appear
    
)

sidebar = html.Div(
    [
        html.H2("Sidebar", className="display-4"),
        html.Hr(),
        html.P(
            "", className="lead"
        ),
        dbc.Nav(
            [
                dbc.NavLink("Home", href="/", active="exact"),
                dbc.NavLink("Page 1", href="/page-1", active="exact"),
                dbc.NavLink("Page 2", href="/page-2", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style=SIDEBAR_STYLE,
)

content = html.Div(id="page-content", children=[], style=CONTENT_STYLE)



app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    content,
    html.P("Here's my table:"),
    table
    # Download(id="download"),
    # html.Button("Save",
    # id="save-button"),
    # html.Div("Press button to save data at your desktop",
    # id="output-1"),
    # dash_table.DataTable(
    #     id='table',
    #     columns=[{"name": i, "id": i} for i in df.columns],
    #     data=df.to_dict('records'),
    #     )    
])



@app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname")]
)
def render_page_content(pathname):
    if pathname == "/":
        return [
                # html.Button('extract graph into excel',id='url'),
                html.H1('Indicator',
                        className="this-p-header",
                        style={'textAlign':'center'}),
                dcc.Graph(id='bargraph',
                         figure=px.bar(df, barmode='group', x='date',
                         y=['rate']))
                ]
    elif pathname == "/page-1":
        return [
                html.H1('Indicator',
                        className="this-p-header",
                        style={'textAlign':'center'}),
                dcc.Graph(id='bargraph',
                         figure=px.bar(df, barmode='group', x='date',
                         y=['rate']))
                ]
    elif pathname == "/page-2":
        return [
                html.H1('Indicator',
                        className="this-p-header",
                        style={'textAlign':'center'}),
                dcc.Graph(id='bargraph',
                         figure=px.bar(df, barmode='group', x='date',
                         y=['rate']))
                ]
    # If the user tries to reach a different page, return a 404 message
    return dbc.Jumbotron(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ]
    )



# @app.callback(
# Output("download", "data"),
# Input("save-button", "n_clicks"),
# State("table", "data"))
# def download_as_csv(n_clicks, table_data):
#     df = pd.DataFrame.from_dict(table_data)
#     if not n_clicks:
#       raise PreventUpdate
#     download_buffer = io.StringIO()
#     df.to_csv(download_buffer, index=False)
#     download_buffer.seek(0)
#     return dict(content=download_buffer.getvalue(), filename="some_filename.csv")

if __name__=='__main__':
    app.run_server(debug=True, port=3000)

    
