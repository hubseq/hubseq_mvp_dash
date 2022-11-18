import os, sys, socket, subprocess
import flask
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
sys.path.append('../../dashboard_utils/src/global_utils/src/')
import file_utils
sys.path.append('../../dashboard_utils/src/')
sys.path.append('../../dashboard_utils/src/global_utils/src/')
import dashboard_file_utils as dfu
import dashboard_server_utils as dsu

SERVER_PORT = '5000'

PIPELINE_DASHBOARD_URL = 'http://52.88.17.238:5001' # 'http://data.hubseq.com:8080'
DATA_DASHBOARD_URL_CHIPSEQ = 'http://52.88.17.238:5002' # 'http://data.hubseq.com:8081'
DATA_DASHBOARD_URL_DNASEQ = 'http://52.88.17.238:5003' # 'http://data.hubseq.com:8082'
GENOME_BROWSER_URL = 'http://52.88.17.238:5004' # 'http://data.hubseq.com:8083'
DATA_DASHBOARD_URL = DATA_DASHBOARD_URL_CHIPSEQ

app = dash.Dash(__name__, suppress_callback_exceptions=True)
# server = app.server

STATIC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
print('STATIC PATH: '+str(STATIC_PATH))

pipelines = ['chipseq', 'dnaseq_targeted']
teamid = 'hubseq-data' #'npipublicinternal'
userid = 'test'

pipelines_dropdown = [
    dcc.Dropdown(
        id="pipeline_dropdown",
        options=[{"label": x, "value": x} for x in pipelines],
        value=pipelines[0],
    )
]

runs_dropdown = [
    dcc.Dropdown(
        id="runs_dropdown",
        placeholder = "Choose Run",
        searchable=True
    )
]

app.layout = html.Div(
    [html.Div(children=[html.Button(html.A('Run New Pipeline', href=PIPELINE_DASHBOARD_URL), id='run_pipeline', style={'height':'35px', 'margin': '10px', 'color': 'green'}),
                        html.Button(html.A('Open Data Dashboard', href=DATA_DASHBOARD_URL, target="_blank", id='dashboard_link'), id='run_dashboard', style={'height':'35px'}),
                        html.Button(html.A('View Genome Browser', href=GENOME_BROWSER_URL), id='run_genome_browser', style={'height':'35px', 'margin': '10px', 'color': 'blue'})],
              style={'width': '100%'}),
     html.H1("HubSeq Run Browser"),
     html.Div(children=['Pipeline: ', html.Div(pipelines_dropdown)], style={'display': 'inline-block', 'width': '100%'}),
     html.P(''),
     html.Div(children=['Run:      ', html.Div(runs_dropdown)], style={'display': 'inline-block', 'width': '100%'}),
     html.Div(id="folder-files")]
)


def createLink( source_file, link_file ):
    if not os.path.exists(link_file):
        subprocess.call(['ln','-s',source_file, link_file])
    return link_file

def getRunIds( root_folder, teamid, userid, pipe ):
    # sub until we fix file_utils getRunIds()
    maindir = os.path.join(root_folder, teamid, userid, pipe)
    dirs = os.listdir(maindir)
    dirs_final = []
    for d in dirs:
        if d.rstrip('/') != 'fastq' and os.path.isdir(os.path.join(maindir,d)):
            dirs_final.append(d)
    return dirs_final

def getRunSamples(root_folder,teamid,userid,pipe,rid):
    # sub until we fix file_utils version
    maindir = os.path.join(root_folder, teamid, userid, pipe,rid)
    dirs = os.listdir(maindir)
    dirs_final = []
    for d in dirs:
        if os.path.isdir(os.path.join(maindir,d)):
            dirs_final.append(d)
    if 'bam' in dirs_final:
        dirs_final.remove('bam')
        dirs_final = ['bam'] + dirs_final            
    if 'fastq' in dirs_final:
        dirs_final.remove('fastq')
        dirs_final = ['fastq'] + dirs_final        
    return dirs_final

def getModuleFiles( module_dir ):
    files = os.listdir(module_dir)
    files_final = []
    for f in files:
        if not os.path.isdir(os.path.join(module_dir,f)) and not f.endswith('.log'):
            files_final.append(f)
    return files_final

def getModules( sdir ):
    files = os.listdir(sdir)
    files_final = []
    for f in files:
        if os.path.isdir(os.path.join(sdir,f)):
            files_final.append(f)
    return files_final

    
def makeDir( newdir ):
    if not os.path.exists(newdir):
        os.makedirs(newdir)
    return newdir


@app.callback(
    Output('dashboard_link', 'href'),
    Input('pipeline_dropdown', 'value'))
def choose_dashboard_link( p ):
    if p != [] and p != None:    
        print('SELECTED PIPELINE: '+str(p))
        if p == 'chipseq':
            DATA_DASHBOARD_URL = DATA_DASHBOARD_URL_CHIPSEQ
        else:
            DATA_DASHBOARD_URL = DATA_DASHBOARD_URL_DNASEQ
        return DATA_DASHBOARD_URL
    else:
        return DATA_DASHBOARD_URL
    
@app.callback(
    Output('runs_dropdown', 'options'),
    Input('pipeline_dropdown', 'value'))
def choose_run(selected_pipeline):
    global teamid, userid
    if selected_pipeline != [] and selected_pipeline != None:
        return dsu.list2optionslist(getRunIds('/s3/',teamid, userid, selected_pipeline))
    else:
        return []
        
@app.callback(Output("folder-files", "children"), Input("runs_dropdown", "value"), Input("pipeline_dropdown","value"))
def list_all_files(rid, pipeline_name):
    global teamid, userid
    file_list_all = []
    if rid in [[], None] or pipeline_name in [[], None]:
        return []
    file_list_all.append(html.P('To download a file, Right-Click and Save'))
    # first get sample directories
    output_samples = getRunSamples('/s3/',teamid,userid,pipeline_name,rid)
    rundir = os.path.join('/s3/',teamid,userid,pipeline_name,rid)
    print("OUTPUT SAMPLES: "+str(output_samples))
    for output_sample_iter in output_samples:
        output_sample = os.path.join(rundir, output_sample_iter)
        if output_sample_iter.lower() in ['fastq','bam']:
            file_list_all.append(html.H2('RAW FILES: '+str(output_sample_iter)))            
            output_files = getModuleFiles(output_sample)
            scratch_dir = STATIC_PATH # makeDir( os.path.join(STATIC_PATH,pipeline_name, rid, output_sample_iter) )
            file_list = []
            for f in output_files:
                f_link = output_sample_iter+'.'+f if output_sample_iter not in f else f
                print('F LINK: {}'.format(str(f_link)))
                createLink( os.path.join(output_sample, f), os.path.join(scratch_dir, f_link))
                file_list.append(html.Li(children=html.A(href=os.path.join(scratch_dir,f),children=f)))
            file_list_all.append(html.Ul(file_list))
        else:
            file_list_all.append(html.H2('SAMPLE: '+str(output_sample_iter)))
            # list all modules within each sample
            output_modules = getModules(output_sample)
            for output_module_iter in output_modules:
                output_module = os.path.join(output_sample, output_module_iter)
                file_list_all.append(html.H4(str(output_module_iter)))
                output_files = getModuleFiles(output_module)
                scratch_dir = STATIC_PATH
                # makeDir( os.path.join(STATIC_PATH, pipeline_name, rid, output_sample_iter, output_module_iter))
                file_list = []
                for f in output_files:
                    f_link = output_sample_iter+'.'+f if output_sample_iter not in f else f
                    print('F LINK: {}'.format(str(f_link)))                    
                    createLink( os.path.join(output_module, f), os.path.join(scratch_dir, f_link) )
                    file_list.append(html.Li(children=html.A(href=os.path.join(scratch_dir,f),children=f)))
                file_list_all.append(html.Ul(file_list))
        file_list_all.append(html.Hr())                
#    print('FILE LIST: '+str(file_list_all))
    return html.Div(children=file_list_all)


@app.server.route(os.path.join(STATIC_PATH,'<resource>'))
def serve_static(resource):
    print('RESOURCE: '+str(resource))
    return flask.send_from_directory(STATIC_PATH, resource)

#@app.server.route('/home/ec2-user/hubseq/mainpage/src/static/chipseq/run_test1/bam/homer-peaks.bed')
#def get_homer():
#    value = flask.request.args.get('value')
#    print('GET HOMER: {}'.format(str(value)))
#    return flask.send_from_directory('/home/ec2-user/hubseq/mainpage/src/static/chipseq/run_test1/bam/', 'homer-peaks.bed')

if __name__ == "__main__":
    local_ip = socket.gethostbyname(socket.gethostname())
    app.run_server(host=local_ip, port=SERVER_PORT, debug=False, dev_tools_ui=False, dev_tools_props_check=False)
