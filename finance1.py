# -*- coding: utf-8 -*-
"""
Created on Thu Apr  1 12:17:35 2021

@author: Anjum Khan
"""


import dash                                # pip install dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc    # pip install dash-bootstrap-components
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from alpha_vantage.timeseries import TimeSeries # pip install alpha-vantage
from flask import Flask, request

dff = pd.read_csv("data.csv")
diff = dff[dff.indicator.isin(['high'])]

#print(diff)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                )
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            dbc.Card(
                [
                    dbc.CardImg(
                        src="/assets/tata.png",
                        top=True,
                        style={"width": "8rem"},
                        className="ml-3"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.P("CHANGE (1D)", className="ml-3")
                            ],width={'size':5, 'offset':1}),

                            dbc.Col([
                                dcc.Graph(id='indicator-graph', figure={},
                                          config={'displayModeBar':False},
                                          )
                            ], width={'size':3,'offset':2})
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dcc.Graph(id='daily-line', figure={},
                                          config={'displayModeBar':False})
                            ], width=12)
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dbc.Button("SELL", className="ml-5"),
                            ], width=4),

                            dbc.Col([
                                dbc.Button("BUY")
                            ], width=4)
                        ], justify="between"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Label(id='low-price', children="12.237",
                                          className="mt-2 ml-5 bg-white p-1 border border-primary border-top-0"),
                            ],width=4),
                            dbc.Col([
                                dbc.Label(id='high-price',
                                          className="mt-2 bg-white p-1 border border-primary border-top-0"),
                            ], width=4)
                        ], justify="between")
                    ]),
                ],
                style={"width": "24rem"},
                className="mt-3"
            ),dcc.Interval(id='update', n_intervals=0, interval=1000*5),
        ], width=6),
         dbc.Col([
            dbc.Card(
                [
                    dbc.CardImg(
                        src="/assets/IBM.png",
                        top=True,
                        style={"width": "8rem"},
                        className="ml-3"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.P("CHANGE (2D)", className="ml-3")
                            ],width={'size':5, 'offset':1}),

                            dbc.Col([
                                dcc.Graph(id='indicator-graph2', figure={},
                                          config={'displayModeBar':False},
                                          )
                            ], width={'size':3,'offset':2})
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dcc.Graph(id='daily-line2', figure={},
                                          config={'displayModeBar':False})
                            ], width=12)
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dbc.Button("SELL", className="ml-5"),
                            ], width=4),

                            dbc.Col([
                                dbc.Button("BUY")
                            ], width=4)
                        ], justify="between"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Label(id='low-price2', children="12.237",
                                          className="mt-2 ml-5 bg-white p-1 border border-primary border-top-0"),
                            ],width=4),
                            dbc.Col([
                                dbc.Label(id='high-price2',
                                          className="mt-2 bg-white p-1 border border-primary border-top-0"),
                            ], width=4)
                        ], justify="between")
                    ]),
                ],
                style={"width": "24rem"},
                className="mt-3"
            ),dcc.Interval(id='update2', n_intervals=0, interval=1000*5),
        ], width=6)      
    ], justify="start"),
    dbc.Row([
        dbc.Col([
            dbc.Card(
                [
                    dbc.CardImg(
                        src="/assets/2.png",
                        top=True,
                        style={"width": "8rem"},
                        className="ml-3"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.P("CHANGE (3D)", className="ml-3")
                            ],width={'size':5, 'offset':1}),

                            dbc.Col([
                                dcc.Graph(id='indicator-graph3', figure={},
                                          config={'displayModeBar':False},
                                          )
                            ], width={'size':3,'offset':2})
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dcc.Graph(id='daily-line3', figure={},
                                          config={'displayModeBar':False})
                            ], width=12)
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dbc.Button("SELL", className="ml-5"),
                            ], width=4),

                            dbc.Col([
                                dbc.Button("BUY")
                            ], width=4)
                        ], justify="between"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Label(id='low-price3', children="12.237",
                                          className="mt-2 ml-5 bg-white p-1 border border-primary border-top-0"),
                            ],width=4),
                            dbc.Col([
                                dbc.Label(id='high-price3',
                                          className="mt-2 bg-white p-1 border border-primary border-top-0"),
                            ], width=4)
                        ], justify="between")
                    ]),
                ],
                style={"width": "24rem"},
                className="mt-3"
            ),dcc.Interval(id='update3', n_intervals=0, interval=1000*5),
        ], width=6),
        dbc.Col([
            dbc.Card(
                [
                    dbc.CardImg(
                        src="/assets/1.png",
                        top=True,
                        style={"width": "8rem"},
                        className="ml-3"),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.P("CHANGE (4D)", className="ml-3")
                            ],width={'size':5, 'offset':1}),

                            dbc.Col([
                                dcc.Graph(id='indicator-graph4', figure={},
                                          config={'displayModeBar':False},
                                          )
                            ], width={'size':3,'offset':2})
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dcc.Graph(id='daily-line4', figure={},
                                          config={'displayModeBar':False})
                            ], width=12)
                        ]),

                        dbc.Row([
                            dbc.Col([
                                dbc.Button("SELL", className="ml-5"),
                            ], width=4),

                            dbc.Col([
                                dbc.Button("BUY")
                            ], width=4)
                        ], justify="between"),

                        dbc.Row([
                            dbc.Col([
                                dbc.Label(id='low-price4', children="12.237",
                                          className="mt-2 ml-5 bg-white p-1 border border-primary border-top-0"),
                            ],width=4),
                            dbc.Col([
                                dbc.Label(id='high-price4',
                                          className="mt-2 bg-white p-1 border border-primary border-top-0"),
                            ], width=4)
                        ], justify="between")
                    ]),
                ],
                style={"width": "24rem"},
                className="mt-3"
            ),dcc.Interval(id='update4', n_intervals=0, interval=1000*5)
        ], width=6)
    ], justify="start")   
])

# Indicator Graph
@app.callback(
    Output('indicator-graph', 'figure'),
    Input('update', 'n_intervals')
)
def update_graph(timer):
    dff_rv = dff.iloc[::-1]
    day_start = dff_rv[dff_rv['date'] == dff_rv['date'].min()]['rate'].values[0]
    day_end = dff_rv[dff_rv['date'] == dff_rv['date'].max()]['rate'].values[0]

    fig = go.Figure(go.Indicator(
        mode="delta",
        value=day_end,
        delta={'reference': day_start, 'relative': True, 'valueformat':'.2%'}))
    fig.update_traces(delta_font={'size':12})
    fig.update_layout(height=30, width=70)

    if day_end >= day_start:
        fig.update_traces(delta_increasing_color='green')
    elif day_end < day_start:
        fig.update_traces(delta_decreasing_color='red')

    return fig


# Indicator Graph
@app.callback(
    Output('indicator-graph2', 'figure'),
    Input('update2', 'n_intervals')
)
def update_graph2(timer):
    dff_rv = dff.iloc[::-1]
    day_start = dff_rv[dff_rv['date'] == dff_rv['date'].min()]['rate'].values[0]
    day_end = dff_rv[dff_rv['date'] == dff_rv['date'].max()]['rate'].values[0]

    fig = go.Figure(go.Indicator(
        mode="delta",
        value=day_end,
        delta={'reference': day_start, 'relative': True, 'valueformat':'.2%'}))
    fig.update_traces(delta_font={'size':12})
    fig.update_layout(height=30, width=70)

    if day_end >= day_start:
        fig.update_traces(delta_increasing_color='green')
    elif day_end < day_start:
        fig.update_traces(delta_decreasing_color='red')

    return fig    


@app.callback(
    Output('indicator-graph3', 'figure'),
    Input('update3', 'n_intervals')
)
def update_graph(timer):
    dff_rv = dff.iloc[::-1]
    day_start = dff_rv[dff_rv['date'] == dff_rv['date'].min()]['rate'].values[0]
    day_end = dff_rv[dff_rv['date'] == dff_rv['date'].max()]['rate'].values[0]

    fig = go.Figure(go.Indicator(
        mode="delta",
        value=day_end,
        delta={'reference': day_start, 'relative': True, 'valueformat':'.2%'}))
    fig.update_traces(delta_font={'size':12})
    fig.update_layout(height=30, width=70)

    if day_end >= day_start:
        fig.update_traces(delta_increasing_color='green')
    elif day_end < day_start:
        fig.update_traces(delta_decreasing_color='red')

    return fig

@app.callback(
    Output('indicator-graph4', 'figure'),
    Input('update4', 'n_intervals')
)
def update_graph(timer):
    dff_rv = dff.iloc[::-1]
    day_start = dff_rv[dff_rv['date'] == dff_rv['date'].min()]['rate'].values[0]
    day_end = dff_rv[dff_rv['date'] == dff_rv['date'].max()]['rate'].values[0]

    fig = go.Figure(go.Indicator(
        mode="delta",
        value=day_end,
        delta={'reference': day_start, 'relative': True, 'valueformat':'.2%'}))
    fig.update_traces(delta_font={'size':12})
    fig.update_layout(height=30, width=70)

    if day_end >= day_start:
        fig.update_traces(delta_increasing_color='green')
    elif day_end < day_start:
        fig.update_traces(delta_decreasing_color='red')

    return fig

# Line Graph---------------------------------------------------------------
@app.callback(
    Output('daily-line', 'figure'),
    Input('update', 'n_intervals')
)
def update_graph(timer):
    dff_rv = dff.iloc[::-1]
    fig = px.line(dff_rv, x='date', y='rate',
                    range_y=[dff_rv['rate'].min(), dff_rv['rate'].max()],
                    height=120).update_layout(margin=dict(t=0, r=0, l=0, b=20),
                                              paper_bgcolor='rgba(0,0,0,0)',
                                              plot_bgcolor='rgba(0,0,0,0)',
                                              yaxis=dict(
                                              title=None,
                                              showgrid=False,
                                              showticklabels=False
                                              ),
                                              xaxis=dict(
                                              title=None,
                                              showgrid=False,
                                              showticklabels=False
                                              ))

    day_start = dff_rv[dff_rv['date'] == dff_rv['date'].min()]['rate'].values[0]
    day_end = dff_rv[dff_rv['date'] == dff_rv['date'].max()]['rate'].values[0]

    if day_end >= day_start:
        return fig.update_traces(fill='tozeroy',line={'color':'green'})
    elif day_end < day_start:
        return fig.update_traces(fill='tozeroy',
                              line={'color': 'red'})

@app.callback(
    Output('daily-line2', 'figure'),
    Input('update2', 'n_intervals')
)
def update_graph2(timer):
    dff_rv = dff.iloc[::-1]
    fig = px.line(dff_rv, x='date', y='rate',
                    range_y=[dff_rv['rate'].min(), dff_rv['rate'].max()],
                    height=120).update_layout(margin=dict(t=0, r=0, l=0, b=20),
                                              paper_bgcolor='rgba(0,0,0,0)',
                                              plot_bgcolor='rgba(0,0,0,0)',
                                              yaxis=dict(
                                              title=None,
                                              showgrid=False,
                                              showticklabels=False
                                              ),
                                              xaxis=dict(
                                              title=None,
                                              showgrid=False,
                                              showticklabels=False
                                              ))

    day_start = dff_rv[dff_rv['date'] == dff_rv['date'].min()]['rate'].values[0]
    day_end = dff_rv[dff_rv['date'] == dff_rv['date'].max()]['rate'].values[0]

    if day_end >= day_start:
        return fig.update_traces(fill='tozeroy',line={'color':'green'})
    elif day_end < day_start:
        return fig.update_traces(fill='tozeroy',
                              line={'color': 'red'})
@app.callback(
    Output('daily-line3', 'figure'),
    Input('update3', 'n_intervals')
)
def update_graph(timer):
    dff_rv = dff.iloc[::-1]
    fig = px.line(dff_rv, x='date', y='rate',
                    range_y=[dff_rv['rate'].min(), dff_rv['rate'].max()],
                    height=120).update_layout(margin=dict(t=0, r=0, l=0, b=20),
                                              paper_bgcolor='rgba(0,0,0,0)',
                                              plot_bgcolor='rgba(0,0,0,0)',
                                              yaxis=dict(
                                              title=None,
                                              showgrid=False,
                                              showticklabels=False
                                              ),
                                              xaxis=dict(
                                              title=None,
                                              showgrid=False,
                                              showticklabels=False
                                              ))

    day_start = dff_rv[dff_rv['date'] == dff_rv['date'].min()]['rate'].values[0]
    day_end = dff_rv[dff_rv['date'] == dff_rv['date'].max()]['rate'].values[0]

    if day_end >= day_start:
        return fig.update_traces(fill='tozeroy',line={'color':'green'})
    elif day_end < day_start:
        return fig.update_traces(fill='tozeroy',
                              line={'color': 'red'})
    
@app.callback(
    Output('daily-line4', 'figure'),
    Input('update4', 'n_intervals')
)
def update_graph(timer):
    dff_rv = dff.iloc[::-1]
    fig = px.line(dff_rv, x='date', y='rate',
                    range_y=[dff_rv['rate'].min(), dff_rv['rate'].max()],
                    height=120).update_layout(margin=dict(t=0, r=0, l=0, b=20),
                                              paper_bgcolor='rgba(0,0,0,0)',
                                              plot_bgcolor='rgba(0,0,0,0)',
                                              yaxis=dict(
                                              title=None,
                                              showgrid=False,
                                              showticklabels=False
                                              ),
                                              xaxis=dict(
                                              title=None,
                                              showgrid=False,
                                              showticklabels=False
                                              ))

    day_start = dff_rv[dff_rv['date'] == dff_rv['date'].min()]['rate'].values[0]
    day_end = dff_rv[dff_rv['date'] == dff_rv['date'].max()]['rate'].values[0]

    if day_end >= day_start:
        return fig.update_traces(fill='tozeroy',line={'color':'green'})
    elif day_end < day_start:
        return fig.update_traces(fill='tozeroy',
                              line={'color': 'red'})    


# Below the buttons--------------------------------------------------------

@app.callback(
    Output('high-price2', 'children'),
    Output('high-price2', 'className'),
    Input('update', 'n_intervals')
)
def update_graph(timer):
    if timer ==0:
        dff_filtered = dff.iloc[[21,22]]
        print(dff_filtered)
    elif timer == 1:
        dff_filtered = dff.iloc[[20,21]]
        print(dff_filtered)
    elif timer == 2:
        dff_filtered = dff.iloc[[19,20]]
        print(dff_filtered)
    elif timer == 3:
        dff_filtered = dff.iloc[[18,19]]
        print(dff_filtered)
    elif timer == 4:
        dff_filtered = dff.iloc[[17,18]]
        print(dff_filtered)
    elif timer == 5:
        dff_filtered = dff.iloc[[16,17]]
        print(dff_filtered)
    elif timer > 5:
        return dash.no_update

    recent_high = dff_filtered['rate'].iloc[0]
    older_high = dff_filtered['rate'].iloc[1]


    if recent_high > older_high:
        return recent_high, "mt-2 bg-success text-white p-1 border border-primary border-top-0"
    elif recent_high == older_high:
        return recent_high, "mt-2 bg-white p-1 border border-primary border-top-0"
    elif recent_high < older_high:
        return recent_high, "mt-2 bg-danger text-white p-1 border border-primary border-top-0"


@app.callback(
    Output('high-price', 'children'),
    Output('high-price', 'className'),
    Input('update', 'n_intervals')
)
def update_graph(timer):
    if timer ==0:
        dff_filtered = dff.iloc[[21,22]]
        print(dff_filtered)
    elif timer == 1:
        dff_filtered = dff.iloc[[20,21]]
        print(dff_filtered)
    elif timer == 2:
        dff_filtered = dff.iloc[[19,20]]
        print(dff_filtered)
    elif timer == 3:
        dff_filtered = dff.iloc[[18,19]]
        print(dff_filtered)
    elif timer == 4:
        dff_filtered = dff.iloc[[17,18]]
        print(dff_filtered)
    elif timer == 5:
        dff_filtered = dff.iloc[[16,17]]
        print(dff_filtered)
    elif timer > 5:
        return dash.no_update

    recent_high = dff_filtered['rate'].iloc[0]
    older_high = dff_filtered['rate'].iloc[1]


    if recent_high > older_high:
        return recent_high, "mt-2 bg-success text-white p-1 border border-primary border-top-0"
    elif recent_high == older_high:
        return recent_high, "mt-2 bg-white p-1 border border-primary border-top-0"
    elif recent_high < older_high:
        return recent_high, "mt-2 bg-danger text-white p-1 border border-primary border-top-0"
 
    
@app.callback(
    Output('high-price3', 'children'),
    Output('high-price3', 'className'),
    Input('update3', 'n_intervals')
)
def update_graph(timer):
    if timer ==0:
        dff_filtered = dff.iloc[[21,22]]
        print(dff_filtered)
    elif timer == 1:
        dff_filtered = dff.iloc[[20,21]]
        print(dff_filtered)
    elif timer == 2:
        dff_filtered = dff.iloc[[19,20]]
        print(dff_filtered)
    elif timer == 3:
        dff_filtered = dff.iloc[[18,19]]
        print(dff_filtered)
    elif timer == 4:
        dff_filtered = dff.iloc[[17,18]]
        print(dff_filtered)
    elif timer == 5:
        dff_filtered = dff.iloc[[16,17]]
        print(dff_filtered)
    elif timer > 5:
        return dash.no_update

    recent_high = dff_filtered['rate'].iloc[0]
    older_high = dff_filtered['rate'].iloc[1]


    if recent_high > older_high:
        return recent_high, "mt-2 bg-success text-white p-1 border border-primary border-top-0"
    elif recent_high == older_high:
        return recent_high, "mt-2 bg-white p-1 border border-primary border-top-0"
    elif recent_high < older_high:
        return recent_high, "mt-2 bg-danger text-white p-1 border border-primary border-top-0"


@app.callback(
    Output('high-price4', 'children'),
    Output('high-price4', 'className'),
    Input('update4', 'n_intervals')
)
def update_graph(timer):
    if timer ==0:
        dff_filtered = dff.iloc[[21,22]]
        print(dff_filtered)
    elif timer == 1:
        dff_filtered = dff.iloc[[20,21]]
        print(dff_filtered)
    elif timer == 2:
        dff_filtered = dff.iloc[[19,20]]
        print(dff_filtered)
    elif timer == 3:
        dff_filtered = dff.iloc[[18,19]]
        print(dff_filtered)
    elif timer == 4:
        dff_filtered = dff.iloc[[17,18]]
        print(dff_filtered)
    elif timer == 5:
        dff_filtered = dff.iloc[[16,17]]
        print(dff_filtered)
    elif timer > 5:
        return dash.no_update

    recent_high = dff_filtered['rate'].iloc[0]
    older_high = dff_filtered['rate'].iloc[1]


    if recent_high > older_high:
        return recent_high, "mt-2 bg-success text-white p-1 border border-primary border-top-0"
    elif recent_high == older_high:
        return recent_high, "mt-2 bg-white p-1 border border-primary border-top-0"
    elif recent_high < older_high:
        return recent_high, "mt-2 bg-danger text-white p-1 border border-primary border-top-0"

    
if __name__=='__main__':
    app.run_server(debug=True, port=3000)
   