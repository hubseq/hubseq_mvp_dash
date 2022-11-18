#
# dashboard_plots_dnaseq_targeted
#
# Callbacks and plot functions for Targeted DNA-Seq. These plots will be displayed on the web Dashboard upon user input (within callback functions).
#
import sys, os, csv, uuid
import dash
from dash import dash_table
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State
import pandas as pd
import dashboard_file_utils as dfu
import dashboard_plot_utils as dpu
import dashboard_server_utils as dsu
import dashboard_constants_dnaseq_targeted as dc
sys.path.append('global_utils/src/')
import file_utils
import global_keys

# main Pandas dataframe that contains all data to display on dashboard
session_dfs = {}

def initSessionDataFrame( _session_dfs ):
    global session_dfs
    session_dfs = _session_dfs
    return

############################################################
## CALLBACK FUNCTIONS
############################################################
def defineCallbacks_DNASeqTargetedDashboardList(app):
    # Once pipeline is chosen, define the list of possible QC dashboards in graphdiv.
    @app.callback(
        Output('graphdiv', 'children'),
        Input('choose-pipeline', 'value'),
        State('teamid', 'key'),
        State('userid', 'key'))
    def CB_dnaseq_targeted_choose_analysisplots( selected_pipeline, teamid, userid ):
        print('in defineCallbacks_DNASeqTargetedDashboardList callback')
        if selected_pipeline != [] and selected_pipeline != None and selected_pipeline == dc.PIPELINE_ID:
            dashboard_list = []
            for dboard in list(dc.DASHBOARD_CONFIG_JSON["dashboard_ids"].values()):
                dashboard_list.append(html.Div(id='{}-dbdiv'.format(dboard), style={'width': '100%'}, children=[]))
            return dashboard_list
        else:
            dash.no_update


def defineCallbacks_DNASeqTargetedAnalysisList(app):
    # Once pipeline is chosen, the list of possible analysis dashboards will displayed as dropdown.
    @app.callback(
        Output('choose-analysis', 'options'),
        Input('choose-pipeline', 'value'),
        State('teamid', 'key'),
        State('userid', 'key'))
    def CB_dnaseq_targeted_choose_analysisplots( selected_pipeline, teamid, userid ):
        print('in CB_dnaseq_targeted_choose_analysisplots callback')
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
            print('DATA FILE JSON LIST: '+str(data_file_json_list))
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
                print('DATA SAMPLE IDS AND FILES: {} {}'.format(str(data_sample_ids), str(data_files)))
                for k in range(0,len(data_files)):
                    s_name = data_sample_ids[k]
                    f_name = data_files[k].split('/')[-1]
                    list_elements.append(html.Li(id=f_name+'_listitem', children=html.A(id=f_name,href=os.path.join(dsu.SCRATCH_DIR,f_name), children=s_name + ': '+f_name)))
                graphs.append(html.Ul(id='fastqc-files', children=list_elements))
                # save loaded data file paths in this session
                session_dfs = dfu.saveSessionDataFiles( session_dfs, data_files_remote, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"])
                return graphs
            else:
                raise dash.exceptions.PreventUpdate
                # dash.no_update
        else:
            # clear data files in session if we don't view this analysis
            session_dfs = dfu.saveSessionDataFiles( session_dfs, [], pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["fastqc"])
            return []


def defineCallbacks_alignmentPanelAnalysisDashboard(app):
    """ Callbacks for panel-based alignment analysis dashboard.
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
        print('in CB_alignment_panel_analysis_dashboard callback')
        if not dsu.selectionEmpty(selected_analysis) and not dsu.selectionEmpty(selected_samples) and dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["alignment"] in selected_analysis:
            # get sample data file paths and IDs
            data_file_json_list = dfu.getSamples(dsu.ROOT_FOLDER, teamid, [userid], [pipelineid], selected_runs, selected_samples, ['bwamem_bam'], ['^alignment_stats.csv'] )
            data_files_remote = file_utils.getFromDictList(data_file_json_list, global_keys.KEY_FILE_NAME, '')
            data_sample_ids = file_utils.getFromDictList(data_file_json_list, global_keys.KEY_FILE_ID, '')
            # ONLY update IF we have grabbed new data files
            if data_files_remote != dfu.getSessionDataFiles( session_dfs, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["alignment"] ):
                data_files = file_utils.downloadFiles( data_files_remote, dsu.SCRATCH_DIR, file_utils.inferFileSystem(data_files_remote), False, True)
                # hsmetrics_file_names, data_sample_ids = dfu.getSamples(userid, pipelineid, selected_runs, selected_sample, ['alignmentqc'], ['^hsmetrics.json'], 'JSON')
                # create plot figures
                alignstats_figure_list = plotAlignStats( data_files, data_sample_ids )
                # hsmetrics_figures_list = plotHsMetrics( hsmetrics_file_names, data_sample_ids )
                # create dashboard plots
                graphs = []
                graphs.append(html.H2('Targeted Alignment Analysis', id='alignqc-title'))
                ## display figures
                for i in range(0,len(alignstats_figure_list)):
                    graphs.append(dcc.Graph(id='graphs_alignstats_'+str(i+1), figure=alignstats_figure_list[i]))
                    graphs.append(html.Hr())
                # for i in range(0,len(hsmetrics_figure_list)):
                #    graphs.append(dcc.Graph(id='graphs_hsmetrics_'+str(i+1), figure=hsmetrics_figure_list[i]))
                #    graphs.append(html.Hr())
                # return final graph elements (list) - rendered by Dash

                # save loaded data file paths in this session
                session_dfs = dfu.saveSessionDataFiles( session_dfs, data_files_remote, pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["alignment"])
                return graphs
            else:
                raise dash.exceptions.PreventUpdate
                # dash.no_update
        else:
            # clear data files in session if we don't view this analysis
            session_dfs = dfu.saveSessionDataFiles( session_dfs, [], pipelineid, sessionid, dc.DASHBOARD_CONFIG_JSON["dashboard_ids"]["alignment"])
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
    p1 = dpu.plotBar( list(_df["sample"]), list(_df["total"]), "sample", "total", "Total Reads Mapped to hg38" )
    plots.append( p1.getFigureObject())

    # % mapped plot
    p2 = dpu.plotBar( list(_df["sample"]), list(_df["percent_mapped"]), "sample", "percent_mapped", "% Reads Mapped to hg38" )
    plots.append( p2.getFigureObject())

    return plots

"""
def plotFlagstat( flagstat_file_names, data_sample_ids ):
    # plots percent mapped and other alignment plots derived from samtools flagstat output

    # flagstat_file_names: LIST of JSON files
    # data_sample_ids: LIST of sample IDs for these files (ordered)
    # return: LIST of figures
    plots = []
    samtools_flagstat_dfs = []
    percent_mapped, mapped, properly_paired = [], [], []
    flagstat_howmanyempty = 0

    # read and convert JSONs to pandas dataframes
    for i in range(len(flagstat_file_names)):
        samtools_flagstat_dfs.append(pd.read_json( flagstat_file_names[i], orient='index' ) if flagstat_file_names[i] not in ['', []] else pd.DataFrame())

    # get flagstat alignment info for each sample
    for i in range(len(samtools_flagstat_dfs)):
        # we skip samples that don't have samtools flagstat output
        if type(samtools_flagstat_dfs[i]) == list and samtools_flagstat_dfs[i] == []:
            flagstat_howmanyempty += 1
            percent_mapped.append(0)
            properly_paired.append(0)
            mapped.append(0)
        else:
            _total_reads = samtools_flagstat_dfs[i]['qcpass']['total']
            percent_mapped.append(100.0*samtools_flagstat_dfs[i]['qcpass']['mapped']/_total_reads if _total_reads > 0 else 0)
            properly_paired.append(100.0*samtools_flagstat_dfs[i]['qcpass']['properly_paired']/_total_reads if _total_reads > 0 else 0)
            mapped.append(samtools_flagstat_dfs[i]['qcpass']['mapped'])

    df = pd.DataFrame( list(zip(data_sample_ids, mapped, percent_mapped, properly_paired)), columns=['sample','mapped','percent_mapped', 'properly_paired'] ) if samtools_flagstat_dfs != None and len(samtools_flagstat_dfs) > 0 and flagstat_howmanyempty < len(samtools_flagstat_dfs) else pd.DataFrame({"sample": [], "percent_mapped": [], "properly_paired": []})

    # % mapped plot
    p1 = plotBar( list(df["sample"]), list(df["percent_mapped"]), "sample", "percent_mapped", "% Reads Mapped to hg38" )
    plots.append( p1.getFigureObject())
    # % properly paired plot
    p2 = plotBar( list(df["sample"]), list(df["properly_paired"]), "sample", "properly_paired", "% Mapped Reads that are Properly Paired" )
    plots.append( p2.getFigureObject())

    return plots


def plotHsMetrics( hsmetrics_file_names, data_sample_ids ):
    # plots % on-target, % off-target and FOLD-80 from picard tools HS metrics.
    # Note that on-target includes "near-target" from HS metrics.

    # hsmetrics_file_names: LIST of JSON files
    # data_sample_ids: LIST of sample IDs for these files (ordered)
    # return: LIST of figures
    #
    plots = []
    picard_hsmetrics_dfs = []
    on_targets, off_targets, fold_80 = [], [], []
    hsmetrics_howmanyempty = 0

    # read and convert JSONs to pandas dataframes
    for i in range(len(hsmetrics_file_names)):
        picard_hsmetrics_dfs.append(pd.read_json( hsmetrics_file_names[i], orient='records' ))

    # get all HS metrics information
    for sindex in range(0,len(data_sample_ids)):
        # we skip samples that don't have picardtools HSmetrics output
        if type(picard_hsmetrics_dfs[sindex]) == list and picard_hsmetrics_dfs[sindex] == []:
            hsmetrics_howmanyempty += 1
        else:
            # ON/OFF TARGET
            on_target = int(picard_hsmetrics_dfs[sindex]['ON_BAIT_BASES'])
            near_target = int(picard_hsmetrics_dfs[sindex]['NEAR_BAIT_BASES'])
            off_target = int(picard_hsmetrics_dfs[sindex]['OFF_BAIT_BASES'])
            total_bases = on_target + near_target + off_target

            on_targets.append(100.0*(on_target+near_target)/total_bases if total_bases > 0 else 0)
            off_targets.append(100.0*off_target/total_bases if total_bases > 0 else 0)

            # FOLD-80
            fold_80_penalty = picard_hsmetrics_dfs[sindex]['FOLD_80_BASE_PENALTY']
            fold_80 = (fold_80 + [float(picard_hsmetrics_dfs[sindex]['FOLD_80_BASE_PENALTY'])]) if isfloat(fold_80_penalty) else (fold_80 + [0])

    # create combined data frame
    df = pd.DataFrame( list(zip(shorten(data_sample_ids), on_targets, off_targets, fold_80)), columns=['sample','percent_on_target','percent_off_target','fold-80']) if picard_hsmetrics_dfs != None and len(picard_hsmetrics_dfs) > 0 and hsmetrics_howmanyempty < len(samplenames) else pd.DataFrame({"sample": [], "percent_on_target": [], "percent_off_target": [], "fold-80": []})

    # create plots
    p1 = plotBar(list(df["sample"]), list(df["percent_on_target"]), "sample", "percent_on_target", "% Reads on-target")
    plots.append(p1.getFigureObject())

    p2 = plotBar(list(df["sample"]), list(df["percent_off_target"]), "sample", "percent_off_target", "% Reads off-target")
    plots.append(p2.getFigureObject())

    p3 = plotBar(list(df["sample"]), list(df["fold-80"]), "sample", "fold-80", "FOLD-80 Penalty Per Sample")
    plots.append(p3.getFigureObject())

    return plots
"""
