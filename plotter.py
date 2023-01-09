import os
import sys
import time
import plotly.graph_objects as go
import argparse
from pprint import pprint
from csvconcat import concat_csv, CSV_CONCAT_KEY

X = ["Attenuation"]
Y = ["TCP-TX", "TCP-RX", "UDP-TX", "UDP-RX"]
KEY = ["Band", "Security", "Channel"]
DEBUG_DIR = f"plotter_debug {int(time.time())}"


def create_hover_text(info=None):
    """
    Function to create format the plot hover-text based on a received dataframe row

    :param info: string to be formatted
    :return: Formatted string for plotly HoverText
    """
    info = info.to_dict()
    hover_text = ""
    for detail in info.keys():
        # Note: <br> creates a line break
        hover_text += str(detail) + ": " + str(info[detail]) + "<br>"
    return hover_text


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser(
        prog="Plotter",
        description="Creates 2D scatter plots"
    )
    parser.add_argument('out',
                        help='filepath to output html file')
    parser.add_argument('data',
                        help='filepath(s) to csv file(s) with data',
                        nargs='+')
    parser.add_argument('-x',
                        help='series (1 or more) to be plotted along x-axis',
                        nargs='+', default=X, dest='x')
    parser.add_argument('-y',
                        help='series (1 or more) to be plotted along y-axis',
                        nargs='+', default=Y, dest='y')
    parser.add_argument('-k', '--key',
                        help='series (0 or more) to be included to make unique graphs',
                        nargs='*', default=KEY, dest='key')
    parser.add_argument('-e', '--extra',
                        help='series (0 or more) to be included in hover text on markers in the graphs',
                        nargs='*', default=KEY, dest='extra')
    parser.add_argument('-d', '--debug',
                        help='debug mode - output all intermediate data',
                        action='store_true', dest='debug')

    args = parser.parse_args()

    files = args.data
    out_filepath = args.out
    X = args.x
    Y = args.y
    KEY = args.key
    if len(files) > 1:
        KEY.append(CSV_CONCAT_KEY)
    E = args.extra
    DEBUG = args.debug
    unitary = None

    # This program plots graphs using each series in X and its corresponding series in Y
    # For this, the number of series in X and Y must be equal, or one of them should have only 1 series
    if len(X) != len(Y):
        if len(X) != 1 and len(Y) != 1:
            sys.exit('Mismatch between number of series in X and Y arguments')
        elif len(X) == 1:
            X = [X[0] for _ in range(len(Y))]
            unitary = X
        else:
            Y = [Y[0] for _ in range(len(X))]
            unitary = Y
    if len(X) == len(Y) == 1:
        unitary = X + Y

    # Read dataframe from csv(s)
    out = None
    if DEBUG:
        os.mkdir(DEBUG_DIR)
        out = f'{DEBUG_DIR}\\joined.csv'
    df = concat_csv(in_files=files, out_file=out, drop_na=True, add_src=True)
    if isinstance(df, str):
        sys.exit(f"Unable to create plots: {df}")

    # Create figure to plot all graphs to
    fig = go.Figure()

    # Initialize empty dict
    data = {}

    # Iterate through all rows for each column to be graphed w.r.t unique key and append the x and y
    # values to the corresponding list in a dict of lists representing the dataframe
    # Note: df[i][j] is the value in ith column at the jth row
    for count, column in enumerate(Y):

        # Set x and y parameters of each scatter plot
        x_param = X[count]
        y_param = column

        for row in df.index:
            # Create primary key
            pk = y_param
            # Create secondary key
            sk = " ".join([str(df[i][row]) for i in KEY])
            # Create composite key from primary and secondary keys
            ck = f"{pk} ({sk})" if sk else pk

            if ck in data.keys():
                data[ck]['x'].append(df[x_param][row])
                data[ck]['y'].append(df[y_param][row])
                # Get row data excluding those columns in exclude_columns
                data[ck]['info'].append(create_hover_text(df.iloc[row, df.columns.isin(E)]))
            else:
                data[ck] = {
                    'x': [df[x_param][row]],
                    'y': [df[y_param][row]],
                    # Get row data excluding those columns in exclude_columns
                    'info': [create_hover_text(df.iloc[row, df.columns.isin(E)])]
                }

    if DEBUG:
        with open(f'{DEBUG_DIR}\\data.txt', 'w') as fp:
            pprint(data, stream=fp)

    # Plot all graphs to the figure
    for key in data.keys():
        fig.add_trace(go.Scatter(
            x=data[key]['x'],
            y=data[key]['y'],
            name=key,
            mode='lines+markers',
            hoverinfo="text",
            hovertext=data[key]['info']
        ))

    # Add metadata to figure
    fig.update_layout(
        xaxis_title=X[0] if unitary in [X, X + Y] else None,
        yaxis_title=Y[0] if unitary in [Y, X + Y] else None
    )

    # Write figure to html
    fig.write_html(out_filepath)
