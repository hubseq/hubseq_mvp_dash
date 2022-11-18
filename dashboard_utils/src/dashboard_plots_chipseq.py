#
# dashboard_plots_chipseq
#
# Callbacks and plot functions for ChIP-Seq. These plots will be displayed on the web Dashboard upon user input (within callback functions).
#
import sys, os, csv, uuid
import dash
from dash import dash_table
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import pandas as pd
import numpy as np
import dashboard_file_utils as dfu
import dashboard_plot_utils as dpu
import dashboard_server_utils as dsu
import dashboard_constants_chipseq as dc
sys.path.append('global_utils/src/')
import file_utils
import global_keys

# main Pandas dataframe that contains all data to display on dashboard
session_dfs = {}
session_dfs = {}

def initSessionDataFrame( _session_dfs ):
    global session_dfs
    session_dfs = _session_dfs
    return

############################################################
## CALLBACK FUNCTIONS
############################################################
def defineCallbacks_ChipSeqDashboardList(app):
    # Once pipeline is chosen, define the list of possible QC dashboards in graphdiv.
    @app.callback(
        Output('graphdiv', 'children'),
        Input('choose-pipeline', 'value'),
        State('teamid', 'key'),
        State('userid', 'key'))
    def CB_chipseq_choose_analysisplots( selected_pipeline, teamid, userid ):
        print('in defineCallbacks_ChipSeqDashboardList callback')
        if selected_pipeline != [] and selected_pipeline != None and selected_pipeline == dc.PIPELINE_ID:
            dashboard_list = []
            for dboard in list(dc.DASHBOARD_CONFIG_JSON["dashboard_ids"].values()):
                dashboard_list.append(html.Div(id='{}-dbdiv'.format(dboard), style={'width': '100%'}, children=[]))
            return dashboard_list
        else:
            dash.no_update


def defineCallbacks_ChipSeqAnalysisList(app):
    # Once pipeline is chosen, the list of possible analysis dashboards will displayed as dropdown.
    @app.callback(
        Output('choose-analysis', 'options'),
        Input('choose-pipeline', 'value'),
        State('teamid', 'key'),
        State('userid', 'key'))
    def CB_chipseq_choose_analysisplots( selected_pipeline, teamid, userid ):
        print('in CB_chipseq_choose_analysisplots callback')
        if selected_pipeline != [] and selected_pipeline != None and selected_pipeline == dc.PIPELINE_ID:
            return dsu.list2optionslist(list(dc.DASHBOARD_CONFIG_JSON["dashboard_ids"].values()))
        else:
            dash.no_update


def defineCallbacks_fastqcAnalysisDashboard(app):
    """ Callbacks for FASTQC analysis dashboard.
    """
    @app.callback(
        Output('{}-dbdiv'.format(dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"]), 'children'),
        Input('choose-samples', 'value'),
        Input('choose-runs', 'value'),
        Input('choose-analysis', 'value'),
        State('sessionid', 'key'),
        State('teamid', 'key'),
        State('userid', 'key'),
        State('choose-pipeline', 'value'))
    def CB_fastqc_analysis_dashboard(selected_samples, selected_runs, selected_analysis, sessionid, teamid, userid, pipelineid):
        global session_dfs
        print('in CB_fastqc_analysis_dashboard callback: SELECTED ANALYSIS: {}'.format(str(selected_analysis)))
        if not dsu.selectionEmpty(selected_analysis) and not dsu.selectionEmpty(selected_samples) and dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"] in selected_analysis:
            # get remote sample file paths and IDs for currently chosen samples
            data_file_json_list = dfu.getSamples(dsu.ROOT_FOLDER, teamid, [userid], [pipelineid], selected_runs, selected_samples, ['fastqc'], ['^HTML'])
            data_files_remote = file_utils.getFromDictList(data_file_json_list, global_keys.KEY_FILE_NAME, '')
            data_sample_ids = file_utils.getFromDictList(data_file_json_list, global_keys.KEY_FILE_ID, '')
            # ONLY update IF we have grabbed new sample data files
            if data_files_remote != dfu.getSessionDataFiles( session_dfs, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"] ):
                # downloads the actual data files from remote
                data_files = file_utils.downloadFiles( data_files_remote, dsu.SCRATCH_DIR, file_utils.inferFileSystem(data_files_remote), False, True)
                # create dashboard plots
                graphs = []
                graphs.append(html.P(''))
                graphs.append(html.H2('Read FASTQC', id='fastqc-title'))
                graphs.append(html.P('Right-Click to open FASTQC HTML in new tab or window.', id='fastqc-desc'))
                list_elements = []
                for k in range(0,len(data_files)):
                    s_name = data_sample_ids[k]
                    f_name = data_files[k].split('/')[-1]
                    list_elements.append(html.Li(id=f_name+'_listitem', children=html.A(id=f_name,href=os.path.join(dsu.SCRATCH_DIR,f_name), children=s_name + ': '+f_name)))
                graphs.append(html.Ul(id='fastqc-files', children=list_elements))
                # save loaded data file paths in this session
                session_dfs = dfu.saveSessionDataFiles( session_dfs, data_files_remote, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"])
                session_dfs = dfu.saveSessionDataFiles( session_dfs, data_files, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"], 'local')
                return graphs
            else:
                raise dash.exceptions.PreventUpdate
                # dash.no_update
        else:
            # clear data files in session if we don't view this analysis
            session_dfs = dfu.saveSessionDataFiles( session_dfs, [], pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"])
            session_dfs = dfu.saveSessionDataFiles( session_dfs, [], pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"], 'local')
            return []


def defineCallbacks_alignmentAnalysisDashboard(app):
    """ Callbacks for genome-based alignment analysis dashboard.
    """
    @app.callback(
        Output('{}-dbdiv'.format(dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["alignment"]), 'children'),
        Input('choose-samples', 'value'),
        Input('choose-runs', 'value'),
        Input('choose-analysis', 'value'),
        State('sessionid', 'key'),
        State('teamid', 'key'),
        State('userid', 'key'),
        State('choose-pipeline', 'value'))
    def CB_alignment_panel_analysis_dashboard(selected_samples, selected_runs, selected_analysis, sessionid, teamid, userid, pipelineid):
        global session_dfs
        WHICH_DB = 'alignment'
        print('in CB_alignment_panel_analysis_dashboard callback')
        if not dsu.selectionEmpty(selected_analysis) and not dsu.selectionEmpty(selected_samples) and dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB] in selected_analysis:
            # get sample data file paths and IDs
            # NOTE: docker is hardcoded - this need to change
            data_file_json_list = dfu.getSamples(dsu.ROOT_FOLDER, teamid, [userid], [pipelineid], selected_runs, selected_samples, ['bowtie2'], ['^alignment_stats.csv'] )
            data_files_remote = file_utils.getFromDictList(data_file_json_list, global_keys.KEY_FILE_NAME, '')
            data_sample_ids = file_utils.getFromDictList(data_file_json_list, global_keys.KEY_FILE_ID, '')
            # ONLY update IF we have grabbed new data files
            if data_files_remote != dfu.getSessionDataFiles( session_dfs, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB] ):
                data_files = file_utils.downloadFiles( data_files_remote, dsu.SCRATCH_DIR, file_utils.inferFileSystem(data_files_remote), False, True)
                # create plot figures
                alignstats_figure_list = plotAlignStats( data_files, data_sample_ids )
                # create dashboard plots
                graphs = []
                graphs.append(html.H2('Genome Alignment Analysis', id='alignqc-title'))
                ## display figures
                for i in range(0,len(alignstats_figure_list)):
                    graphs.append(dcc.Graph(id='graphs_alignstats_'+str(i+1), figure=alignstats_figure_list[i]))
                    graphs.append(html.Hr())
                # save loaded remote and local data file paths in this session
                session_dfs = dfu.saveSessionDataFiles( session_dfs, data_files_remote, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB])
                session_dfs = dfu.saveSessionDataFiles( session_dfs, data_files, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB], 'local')
                return graphs
            else:
                raise dash.exceptions.PreventUpdate
                # dash.no_update
        else:
            # clear data files in session if we don't view this analysis
            session_dfs = dfu.saveSessionDataFiles( session_dfs, [], pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB])
            session_dfs = dfu.saveSessionDataFiles( session_dfs, [], pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB], 'local')
            return []

def defineCallbacks_downloadButtons(app):
    """ Callbacks for download buttons. A bit of a hack - we initialize for max 5 buttons.
    """
    @app.callback(
        Output('download_peak_table_1', 'data'),
        Input('download_peak_table_button_1', 'n_clicks'),
        State('sessionid', 'key'),
        State('choose-pipeline', 'value'),
        prevent_initial_call=True)
    def click_download_peak_table_button_1(n_clicks, sessionid, pipelineid):
        global session_dfs
        print('IN click_download_peak_table_button_1() NCLICKS: '+str(n_clicks))
        which_peak_file = 0
        analysis = dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]['peaks']
        if n_clicks != None and n_clicks > 0:
            session_peak_files = dfu.getSessionDataFiles( session_dfs, pipelineid, sessionid, analysis, 'local' )
            peaks_df, peaks_type = getPeaksTables( [session_peak_files[which_peak_file]], ['sample'] )
            return dcc.send_data_frame( peaks_df[0].to_csv, 'peaks_table_{}.txt'.format(str(peaks_type[0])))

    @app.callback(
        Output('download_peak_table_2', 'data'),
        Input('download_peak_table_button_2', 'n_clicks'),
        State('sessionid', 'key'),
        State('choose-pipeline', 'value'),
        prevent_initial_call=True)
    def click_download_peak_table_button_1(n_clicks, sessionid, pipelineid):
        global session_dfs
        print('IN click_download_peak_table_button_2() NCLICKS: '+str(n_clicks))
        which_peak_file = 1
        analysis = dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]['peaks']
        if n_clicks != None and n_clicks > 0:
            session_peak_files = dfu.getSessionDataFiles( session_dfs, pipelineid, sessionid, analysis, 'local' )
            peaks_df, peaks_type = getPeaksTables( [session_peak_files[which_peak_file]], ['sample'] )
            return dcc.send_data_frame( peaks_df[0].to_csv, 'peaks_table_{}.txt'.format(str(peaks_type[0])))

    @app.callback(
        Output('download_peak_table_3', 'data'),
        Input('download_peak_table_button_3', 'n_clicks'),
        State('sessionid', 'key'),
        State('choose-pipeline', 'value'),
        prevent_initial_call=True)
    def click_download_peak_table_button_1(n_clicks, sessionid, pipelineid):
        global session_dfs
        which_peak_file = 2
        analysis = dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]['peaks']
        if n_clicks != None and n_clicks > 0:
            session_peak_files = dfu.getSessionDataFiles( session_dfs, pipelineid, sessionid, analysis, 'local' )
            peaks_df, peaks_type = getPeaksTables( [session_peak_files[which_peak_file]], ['sample'] )
            return dcc.send_data_frame( peaks_df[0].to_csv, 'peaks_table_{}.txt'.format(str(peaks_type[0])))


def defineCallbacks_peakAnalysisDashboard(app):
    """ Callbacks for peak analysis dashboard.
    """
    @app.callback(
        Output('{}-dbdiv'.format(dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["peaks"]), 'children'),
        Input('choose-samples', 'value'),
        Input('choose-runs', 'value'),
        Input('choose-analysis', 'value'),
        State('sessionid', 'key'),
        State('teamid', 'key'),
        State('userid', 'key'),
        State('choose-pipeline', 'value'))
    def CB_peak_analysis_dashboard(selected_samples, selected_runs, selected_analysis, sessionid, teamid, userid, pipelineid):
        global session_dfs
        WHICH_DB = 'peaks'
        print('in CB_peaks_analysis_dashboard callback')
        if not dsu.selectionEmpty(selected_analysis) and not dsu.selectionEmpty(selected_samples) and dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB] in selected_analysis:
            # get sample data file paths and IDs
            # NOTE: docker is hardcoded - this need to change
            data_file_json_list = dfu.getSamples(dsu.ROOT_FOLDER, teamid, [userid], [pipelineid], selected_runs, selected_samples, ['macs2', 'homer'], ['^.broadPeak', '^.narrowPeak', '^.txt'] )
            print('DATA FILE JSON LIST: '+str(data_file_json_list))
            data_files_remote = file_utils.getFromDictList(data_file_json_list, global_keys.KEY_FILE_NAME, '')
            data_sample_ids = file_utils.getFromDictList(data_file_json_list, global_keys.KEY_FILE_ID, '')
            # ONLY update IF we have grabbed new data files
            if data_files_remote != dfu.getSessionDataFiles( session_dfs, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB] ):
                data_files = file_utils.downloadFiles( data_files_remote, dsu.SCRATCH_DIR, file_utils.inferFileSystem(data_files_remote), False, True)
                # create list of data frames that will be tables of peaks
                peaks_df_list, peaks_df_type = getPeaksTables( data_files, data_sample_ids )
                # create dashboard plots
                graphs = []
                graphs.append(html.H2('ChIP-Seq Peak Analysis', id='peaks-title'))
                ## display figures and tables
                graphs.append(html.Div(id='plotpeakdistr-div', style={'width': '100%', 'display': 'inline-block'}, \
                              children=[dpu.addGraph(plotPeakLengthHistograms(data_sample_ids[i], peaks_df_list[i], peaks_df_type[i]), options={'style': {'width': '45%', 'display': 'inline-block'}}) for i in range(0,len(peaks_df_list))]))

                # then tables of peak data with download button
                for i in range(0,len(peaks_df_list)):
                    if peaks_df_type[i] == 'MACS2':
                        graphs.append(html.H4('MACS2 peaks for {}'.format(str(data_sample_ids[i]))))
                    elif peaks_df_type[i] == 'homer':
                        graphs.append(html.H4('HOMER peaks for {}'.format(str(data_sample_ids[i]))))
                    graphs.append(dash_table.DataTable(id='tables_peaks_'+str(i+1), columns=[{"name": j, "id": j} for j in peaks_df_list[i]], data=peaks_df_list[i].to_dict(orient='records')))
                    graphs.append(html.Div(children=[html.Button('Download Peak Table', id='download_peak_table_button_{}'.format(str(i+1))), \
                                                     dcc.Download(id='download_peak_table_{}'.format(str(i+1)))]))
                    graphs.append(html.Hr())
                # save loaded remote and local data file paths in this session
                session_dfs = dfu.saveSessionDataFiles( session_dfs, data_files_remote, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB])
                session_dfs = dfu.saveSessionDataFiles( session_dfs, data_files, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB], 'local')
                return graphs
            else:
                raise dash.exceptions.PreventUpdate
                # dash.no_update
        else:
            # clear data files in session if we don't view this analysis
            session_dfs = dfu.saveSessionDataFiles( session_dfs, [], pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB])
            session_dfs = dfu.saveSessionDataFiles( session_dfs, [], pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"][WHICH_DB], 'local')
            return []

############################################################
## PLOT FUNCTIONS
############################################################
def plotAlignStats( alignstats_file_names, data_sample_ids ):
    """ plots percent mapped and other alignment plots derived from samtools view output from bwamem_bam module

    alignstats_file_names: LIST of alignment stats files (CSV format)
    data_sample_ids: LIST of sample IDs for these files (in same order as alignstats files list)
    return: LIST of figures
    """
    print('in plotAlignStats()')
    print('alignfiles: {}, samples: {}'.format(str(alignstats_file_names), str(data_sample_ids)))
    plots = []
    alignstats_dfs = []
    total, percent_mapped, mapped = [], [], []
    files_howmanyempty = 0

    # read and convert JSONs to pandas dataframes
    for i in range(len(alignstats_file_names)):
        alignstats_dfs.append(pd.read_csv(alignstats_file_names[i]) if alignstats_file_names[i] not in ['', []] else pd.DataFrame())

    # get flagstat alignment info for each sample
    for i in range(len(alignstats_dfs)):
        _df = alignstats_dfs[i]
        # we skip samples that don't have alignment stats output
        if (type(_df) == type(pd.DataFrame()) and _df.empty):
            files_howmanyempty += 1
            percent_mapped.append(0)
            mapped.append(0)
            total.append(0)
        else:
            _total = int(list(_df[_df.read_type.isin(['total'])]['count'])[0])
            _mapped = int(list(_df[_df.read_type.isin(['mapped'])]['count'])[0])
            _percent_mapped = (100.0*_mapped)/_total

            total.append(_total)
            mapped.append(_mapped)
            percent_mapped.append(_percent_mapped)

    _df = pd.DataFrame( list(zip(data_sample_ids, total, mapped, percent_mapped)), \
                       columns=['sample','total','mapped','percent_mapped'] ) \
                       if alignstats_dfs!= None and len(alignstats_dfs) > 0 and files_howmanyempty < len(alignstats_file_names) \
                       else pd.DataFrame({"sample": [], "total": [], "mapped": [], "percent_mapped": []})

    # total plot
    p1 = dpu.plotBar( list(_df["sample"]), list(_df["total"]), "sample", "total", "Total Reads Mapped to genome" )
    plots.append( p1.getFigureObject())

    # % mapped plot
    p2 = dpu.plotBar( list(_df["sample"]), list(_df["percent_mapped"]), "sample", "percent_mapped", "% Reads Mapped to genome" )
    plots.append( p2.getFigureObject())

    return plots

def getPeaksTables( peaks_file_names, data_sample_ids ):
    """ Given the list of peaks BED-like files, create data frames that will become tables listing those peaks.
    """
    def getHomerHeader(homerfile):
        # HOMER format changes depending on parameters. Pain-of-a-program to use.
        print('IN HOMER HEADER: {}'.format(str(homerfile)))
        rprev = ''
        with open(homerfile,'r') as f:
            while True:
                r = f.readline()
                if r=='' or r[0]!='#':
                    break
                rprev = r.rstrip(' \t\n').lstrip('# ')
        return rprev.split('\t')

    peaks_dfs_list, peaks_type_list = [], []
    print('in getPeaksTables()')
    print('peaks_files: {}, samples: {}'.format(str(peaks_file_names), str(data_sample_ids)))
    for i in range(0,len(peaks_file_names)):
        pfile = peaks_file_names[i]
        if pfile.lower().endswith('.homer.txt'):
            # homer file
            headers = getHomerHeader(pfile)
            peaks_dfs_list.append(pd.read_csv(pfile, sep='\t', comment='#', \
                    names=headers)) # ['peak_id','chr','start','end','strand','normalized_tag_count','region_size', 'findPeaks_score','total_tags','control_tags','fold_change_vs_control', 'pvalue_vs_control','clonal_fold_change']))
            peaks_type_list.append('homer')
        elif pfile.lower().endswith('broadpeak'):
            # macs2 file
            peaks_dfs_list.append(pd.read_csv(pfile, sep='\t', comment='#', names=['chr','start','end','peak_id','score', 'strand','fold_change_vs_control','-log10_pvalue','-log10_qvalue']))
            peaks_type_list.append('MACS2')
        elif pfile.lower().endswith('narrowpeak'):
            # macs2 file
            peaks_dfs_list.append(pd.read_csv(pfile, sep='\t', comment='#', names=['chr','start','end','peak_id','score', 'strand','fold_change_vs_control','-log10_pvalue','-log10_qvalue','peak_offset']))
            peaks_type_list.append('MACS2')
    return peaks_dfs_list, peaks_type_list

def plotPeakLengthHistograms( sample_id, peaks_df, peak_type = 'MACS2'):
    """ Given a peak data frame, plots histogram of peak lengths
    """
    print('SAMPLE ID: '+str(sample_id))    
    print('PEAKS_DF: '+str(peaks_df))
    print('PEAK TYPE: '+str(peak_type))
    print('PEAKS END: '+str(np.array(peaks_df['end'])))
    print('PEAKS START: '+str(np.array(peaks_df['start'])))
    p1 = dpu.plotHistogram( np.abs(np.array(peaks_df['end'].astype('int64') - peaks_df['start'].astype('int64'))), 'Peak Lengths, {}'.format(sample_id), 'Count', '{}: Distribution of Peak Lengths'.format(peak_type.upper()))
    return p1.getFigureObject()
