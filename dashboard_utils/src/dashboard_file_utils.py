#
# dashboard_file_utils
#
# Utility functions for retrieving and writing files given Dashboard inputs.
# Makes heavy use of the general file_utils script within the global utils repository.
#
import sys, os
sys.path.append('global_utils/src/')
import global_keys
import file_utils

DASHBOARD_CONFIG_DIR = './'

def getSamples(team_root_folder, teamid, userids, pipelineids, selected_runs, selected_samples, moduleids, extensions = [], extensions2exclude = []):
    """ Get all sample files and IDs of a particular file type, given the list of choices on the dashboard.
    Assumes the standard folder structure for pipeline runs.

    Example: dfu.getSamples(dsu.ROOT_FOLDER, teamid, [userid], [pipelineid], selected_runs, selected_samples, [], ['fastqc'], ['^HTML'])

    team_root_folder: STRING - 's3://' or '/'

    Return LIST of filenames, LIST of sample IDs (ordered)
    """
    # print('GET SAMPLES PARAMS: {}'.format(str(dict(team_root_folder=team_root_folder, teamid=teamid, userids=userids, pipelineids=pipelineids, selected_runs=selected_runs, selected_samples=selected_samples, moduleids=moduleids, extensions=extensions, extension2exclude=extensions2exclude))))
    # get sample folders
    data_file_folders = file_utils.getRunSampleOutputFolders(team_root_folder, teamid, userids, pipelineids, selected_runs, selected_samples, moduleids)
    # get data files matching extension patterns in these sample folders
    data_file_json_list = file_utils.getDataFiles(data_file_folders, extensions, extensions2exclude )
    return data_file_json_list


def getDashboardConfigJSON( pipeline_id ):
    """ Given a pipeline ID, loads and returns a JSON containing info for loading a dashboard for this pipeline.
    Config file must be named 'dashboard_config.<PIPELINE_ID>.json'
    """
    config_file = os.path.join(DASHBOARD_CONFIG_DIR, 'dashboard_config.{}.json'.format(pipeline_id))
    return file_utils.loadJSON( config_file )


def getSessionDataFiles( session_dataframe, pipelineid, sessionid, analysis, which_files = 'remote' ):
    """ Returns the list of data files currently loaded in this session of the pipeline dashboard.
    Follows the JSON specification for dashboard sessions.
    """
    if which_files.lower() == 'local':
        return session_dataframe[pipelineid][sessionid][analysis]["local_data_files"]
    else:
        print('HERE...')
        return session_dataframe[pipelineid][sessionid][analysis]["remote_data_files"]

def saveSessionDataFiles( session_dataframe, data_files_list, pipelineid, sessionid, analysis, which_files = 'remote'):
    """ Save or update data files list in the session data frame.
    This will prevent re-rendering of a dashboard if the files list (and hence the graphs) haven't changed.
    Follows the JSON specification for dashboard sessions.
    """
    if which_files == 'local':
        session_dataframe[pipelineid][sessionid][analysis]["local_data_files"] = data_files_list
    else:
        session_dataframe[pipelineid][sessionid][analysis]["remote_data_files"] = data_files_list
    print('NEW SESSION DATA FRAME AFTER SAVE: '+str(session_dataframe))
    return session_dataframe
