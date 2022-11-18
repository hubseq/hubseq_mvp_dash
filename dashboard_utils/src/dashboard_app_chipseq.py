#
# dashboard_app_chipseq
#
# Main entry point for dashboard for chromatin-IP sequencing.
# Using Plotly Dash. Serves front-end that has callback listeners that respond to user inputs.
#
import sys, os, csv, uuid, socket
import dash
from dash import dash_table
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import pandas as pd
import flask
import dashboard_file_utils as dfu
import dashboard_plot_utils as dpu
import dashboard_server_utils as dsu
import dashboard_plots_main as dpm

import dashboard_constants_chipseq as dc
import dashboard_plots_chipseq as dp

sys.path.append('global_utils/src/')
import file_utils
import global_keys

# external_stylesheets = dsu.DASH_STYLESHEETS
SERVER_PORT = '5002'
# SESSION_ID = 'tempsession'

# log files for each sample - has info about each pipeline run
# dflogs = {}

############################################################
## APP OBJECT AND SERVE FRONTEND LAYOUT
############################################################
app = dash.Dash(__name__, suppress_callback_exceptions=True)

def serve_layout():
    global session_dfs
    print('in serve_layout()')
    sessionid = dsu.getSessionId( dc.TEAM_ID, dc.USER_ID, dc.PIPELINE_ID, True )
    session_dfs = dpm.initDataframe( sessionid, dc.DASHBOARD_CONFIG_JSON ) # user needs to define structure of data frame
    dp.initSessionDataFrame( session_dfs )
    return dpm.renderDashboard_main(dc.TEAM_ID, dc.USER_ID, dc.PIPELINE_ID, sessionid)

app.layout = serve_layout

############################################################
## CALLBACK FUNCTIONS
############################################################
dpm.defineCallbacks_mainDashboard(app)
dp.defineCallbacks_ChipSeqDashboardList(app)
dp.defineCallbacks_ChipSeqAnalysisList(app)
dp.defineCallbacks_fastqcAnalysisDashboard(app)
dp.defineCallbacks_alignmentAnalysisDashboard(app)
dp.defineCallbacks_peakAnalysisDashboard(app)
dp.defineCallbacks_downloadButtons(app)

@app.server.route(os.path.join(dsu.STATIC_PATH,'<resource>'))
def serve_static(resource):
    print('RESOURCE: '+str(resource))
    return flask.send_from_directory(dsu.STATIC_PATH, resource)

if __name__ == '__main__':
    local_ip = socket.gethostbyname(socket.gethostname())
    app.run_server(host=local_ip, port=SERVER_PORT, debug=False,dev_tools_ui=False,dev_tools_props_check=False)
