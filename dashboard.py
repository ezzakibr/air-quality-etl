import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import pandas as pd

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    dbname="air_quality_db",
    user="ow_project",
    password="passedemot",
    host="localhost"
)
cursor = conn.cursor()

# Requête pour obtenir les dernières données AQI pour chaque ville
query = '''
SELECT city, timestamp, aqi_mean, temperature_mean, co_mean, no2_mean, o3_mean, so2_mean, pm2_5_mean, pm10_mean
FROM air_quality_stats
ORDER BY timestamp DESC
'''

cursor.execute(query)
records = cursor.fetchall()
data = pd.DataFrame(records, columns=['city', 'timestamp', 'aqi_mean', 'temperature_mean', 'co_mean', 'no2_mean', 'o3_mean', 'so2_mean', 'pm2_5_mean', 'pm10_mean'])

# Fermeture de la connexion
cursor.close()
conn.close()

# Dictionnaire des coordonnées des villes
coordinates = {
    "Rabat": [34.0209, -6.8416],
    "Casablanca": [33.5731, -7.5898],
    "Marrakech": [31.6295, -7.9811],
    "Fès": [34.0181, -5.0078],
    "Tanger": [35.7595, -5.8340],
    "Agadir": [30.4278, -9.5981]
}

# Créer une colonne pour les coordonnées
data['coords'] = data['city'].map(coordinates)

# Créer la carte avec les AQI
fig_map = px.scatter_mapbox(
    data,
    lat=data['coords'].apply(lambda x: x[0]),
    lon=data['coords'].apply(lambda x: x[1]),
    size='aqi_mean',
    color='aqi_mean',
    hover_name='city',
    hover_data={'coords': False, 'timestamp': True, 'aqi_mean': True},
    zoom=5,
    mapbox_style='open-street-map'
)

# Démarrer l'application Dash
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Qualité de l'air au Maroc", style={'text-align': 'center'}),
    dcc.Graph(id='map', figure=fig_map),
    html.Div([
        dcc.Graph(id='temperature'),
        dcc.Graph(id='pollutants')
    ], style={'display': 'flex', 'flex-direction': 'row'}),
    dcc.Interval(
        id='interval-component',
        interval=1*60*1000,  # Actualiser toutes les minutes
        n_intervals=0
    )
])

@app.callback(
    [Output('map', 'figure'),
     Output('temperature', 'figure'),
     Output('pollutants', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_graphs(n):
    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(
        dbname="air_quality_db",
        user="ow_project",
        password="passedemot",
        host="localhost"
    )
    cursor = conn.cursor()

    # Requête pour obtenir les dernières données AQI pour chaque ville
    query = '''
    SELECT city, timestamp, aqi_mean, temperature_mean, co_mean, no2_mean, o3_mean, so2_mean, pm2_5_mean, pm10_mean
    FROM air_quality_stats
    ORDER BY timestamp DESC
    '''
    cursor.execute(query)
    records = cursor.fetchall()
    data = pd.DataFrame(records, columns=['city', 'timestamp', 'aqi_mean', 'temperature_mean', 'co_mean', 'no2_mean', 'o3_mean', 'so2_mean', 'pm2_5_mean', 'pm10_mean'])

    cursor.close()
    conn.close()

    data['coords'] = data['city'].map(coordinates)

    fig_map = px.scatter_mapbox(
        data,
        lat=data['coords'].apply(lambda x: x[0]),
        lon=data['coords'].apply(lambda x: x[1]),
        size='aqi_mean',
        color='aqi_mean',
        hover_name='city',
        hover_data={'coords': False, 'timestamp': True, 'aqi_mean': True},
        zoom=5,
        mapbox_style='open-street-map'
    )

    fig_temp = px.line(data, x='timestamp', y='temperature_mean', color='city', title='Température moyenne par ville au fil du temps')

    pollutants = ['co_mean', 'no2_mean', 'o3_mean', 'so2_mean', 'pm2_5_mean', 'pm10_mean']
    fig_pollutants = make_subplots(rows=2, cols=3, subplot_titles=pollutants)

    for i, pollutant in enumerate(pollutants):
        row = i // 3 + 1
        col = i % 3 + 1
        fig = px.bar(data, x='city', y=pollutant, title=pollutant)
        for trace in fig.data:
            fig_pollutants.add_trace(trace, row=row, col=col)

    fig_pollutants.update_layout(height=800, title_text="Polluants moyens par ville")

    return fig_map, fig_temp, fig_pollutants

if __name__ == '__main__':
    app.run_server(debug=True)
