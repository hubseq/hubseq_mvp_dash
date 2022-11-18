import dashboard_file_utils as dfu

TEAM_ID = 'hubseq-data' # 'hubtenants' # 'npipublicinternal' # constant for now. FUTURE: get ID from client.
USER_ID = 'test' # constant for now. FUTURE: get ID from client.
PIPELINE_ID = 'chipseq'
DASHBOARD_CONFIG_JSON = dfu.getDashboardConfigJSON( PIPELINE_ID )
