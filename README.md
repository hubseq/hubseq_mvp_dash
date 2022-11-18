# hubseq_mvp_dash
MVP used for Y Combinator application. Used Plotly Dash.

### installation
You'll need to install the following Python libraries

- flask
- plotly
- dash
- boto3
- pandas

### run
There are several individual prototype dashboards that can be run:

Mainpage:

`$ cd dash_mainpage/src/`

`$ python app_mainpage.py`

ChIP-Seq:

`$ cd dashboard_utils/src/`

`$ python dashboard_app_chipseq.py`

DNA-Seq:

`$ cd dashboard_utils/src/`

`$ python dashboard_app_dnaseq_targeted.py`

Pipeline:

`$ cd pipeline_dashboard/src/`

`$ python dashboard_app_run_pipeline.py`

