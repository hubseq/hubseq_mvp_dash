#
# run_pipeline
#
# Runs a pipeline of docker modules.
#
# Input arguments:
# --pipeline <WHICH_PIPELINE_TO_RUN> - e.g., dnaseq_targeted.human,rnaseq.mouse
# --modules <WHICH_MODULES_TO_RUN> - e.g., fastqc,bwamem_bam,mpileup
# --input <INITIAL_LIST_OF_INPUT_FILES> - e.g., s3://fastq/R1.fastq,s3://fastq/R2.fastq. Can also specify a directory and file type like, s3://fastq/^fastq
# --output <BASE_OUTPUT_DIRECTORY> - e.g., s3://bam/
# --userid <USER_ID>
# --teamid <TEAM_ID>
# --runid <RUN_ID>
# --moduleargs <LIST_OF_ARGS_FOR_EACH_MODULE> - e.g., '','-t S','','',... - LIST SAME SIZE as --modules. Cannot contain file paths
# --altinputs <LIST_OF_ALT_INPUTS_FOR_EACH_MODULE> - e.g., '','s3://fasta/hg38.fasta','',... LIST SAME SIZE AS --modules.
# --altoutputs <LIST_OF_ALT_OUTPUTS_FOR_EACH_MODULE> - e.g., '','s3://bed/out1.bed,s3://bed/out2.bed','',... LIST SAME SIZE AS --modules.
# --mock : for mock run
# --dryrun : for dry run
#
# Deprecated:
# --samples <LIST_OF_RUN:SAMPLES> - e.g., run1:sample1,run1:sample2,run2:sample1,run2:sample2
#
# [TO-DO]: Need to figure out specifying I/O for each module in a DAG list, in a more flexible way. Right now its rigid (fixed output names)
#
import os, sys, uuid, json, boto3, yaml
from pathlib import Path
sys.path.append('global_utils/src/')
import module_utils
import file_utils
import aws_s3_utils
from argparse import ArgumentParser
from datetime import datetime
from run_batchjob import run_batchjob

CLIENT_BASE_DIR = 'hubtenants'
SCRIPT_DIR = str(os.path.dirname(os.path.realpath(__file__)))

def getDateAsString():
    """ move this to utils eventually
    """
    return datetime.now().strftime("%Y%m%d-%H%M")

def cleanList( mylist, remove_chars ):
    """ Cleans up list - removes chars before and after elements.
        Move this to utils eventually
    """
    return list(map(lambda x: x.lstrip(remove_chars).rstrip(remove_chars), mylist))

def parseStringList( strlist ):
    listout = []
    curr_str = ''
    prev_char = ""
    for e in strlist:
        if e == ',' and prev_char in ["'", '"']:
            listout.append(curr_str)
            curr_str = ''
        elif e in ["'", '"']:
            pass
        else:
            curr_str += e
        prev_char = e
    listout.append(curr_str)
    return listout

def replaceInString( s, replace_dict ):
    """ Replace all keys found in s with their values. This should go in utils at some point.
    >>> replaceInString( 's3://foo/<run_id>/<sample_id>.out', {'<run_id>': 'RUN1', '<sample_id>': 'sample1'})
    's3://foo/RUN1/sample1.out'
    """
    s_out = s
    for k, v in replace_dict.items():
        s_out = s_out.replace(k, v)
    return s_out

def run_pipeline( args_json ):
    global SCRIPT_DIR

    def moduleIndex(current_module, module_list):
        """ Module List may contain special characters.
            Example module list: ['bcl2fastq', '*fastqc', 'rnastar, bwamem', 'expressionqc', 'deseq2']

        >>> moduleIndex( 'expressionqc', ['bcl2fastq', '*fastqc', 'rnastar, bwamem', 'expressionqc', 'deseq2'])
        3
        """
        i = -1
        for j in range(0,len(module_list)):
            # a given step in DAG may allow multiple modules
            modules = cleanList(module_list[j].split(','), ' ^*~')
            for m in modules:
                if current_module == m:
                    i = j
                    break
        return i

    def getPreviousModule( current_module, initial_module, module_list, pipeline_dict ):
        """ Gets the previous module(s) in DAG workflow.
            Example DAG: ['bcl2fastq', '*fastqc', 'rnastar, bwamem', 'expressionqc', 'deseq2']
            Example submitted workflow: ['fastqc', 'rnastar', 'expressionqc']

            Returns list of previous modules
    
            [TO-DO] Needs some error checking
        """
        dag_modules = pipeline_dict['order']
        i = moduleIndex(current_module, dag_modules)
        start = moduleIndex(initial_module, dag_modules)
        if i != -1 and start != -1:
            if 'previous_module' not in pipeline_dict[current_module]:
                for j in range(start,i)[::-1]:
                    # current modules in search
                    loop_modules = cleanList(dag_modules[j].split(','), ' ')
                    for m in loop_modules:
                        if m[0] != '*' and moduleIndex(m, module_list) != -1:
                            return [m]
            else:
                return pipeline_dict[current_module]['previous_module'].split(',')
        return []

    def createFilePath( output_base_dir, file_pattern_list, module_type, prev_module_type, sid, sids):
        """ Creates a list of file paths for given module
            output_base_dir: output base directory
            file_pattern_list: file pattern to create - e.g., ['<sample_id>.bam', '<sample_id>.sam'], ['deqc.out.txt']
            module_type: linear or merge
            prev_module_type: module type of previous module, linear or merge
            sid: current sample id
            sids: all sample ids
        """
        outfiles = []
        if module_type.lower() == 'merge' and prev_module_type.lower() == 'linear':
            for f in file_pattern_list:
                for s in sids:
                    outfiles.append(os.path.join(output_base_dir, f.replace('<sample_id>', s).replace('<folder>', '')))
        else:  # module_type = linear
            for f in file_pattern_list:
                outfiles.append(os.path.join(output_base_dir, f.replace('<sample_id>', sid).replace('<folder>', '')))
        return outfiles
    
    def getPreviousOutput( base_output_dir, curr_module, prev_modules, curr_sid, all_sids, pipeline_dict ):
        """ Gets the previous output files as input files for current module.
            If files not found, then empty list is returned.
        """
        input_files = []
        for prev_module in prev_modules:
            if prev_module != '':                
                prev_module_output_dir = os.path.join(base_output_dir, prev_module)
                # lstrip rstrip are to remove spaces in a comma-separated list
                prev_module_output_file_extensions = list(map(lambda x: x.lstrip(' ').rstrip(' '), \
                                                              pipeline_dict[curr_module]['input_file'].split(',')))
                prev_module_ignore_file_patterns = list(map(lambda x: x.lstrip(' ').rstrip(' '), \
                                                            pipeline_dict[curr_module]['ignore'].split(','))) \
                                                            if 'ignore' in pipeline_dict[curr_module] \
                                                               else []
                print('PREV MODULE FILE EXTENSIONS: '+str(prev_module_output_file_extensions))
                for e in prev_module_output_file_extensions:
                    if e != '' and '<folder>' not in e:
                        print('listsubfiles args: {} {} {}'.format(str(prev_module_output_dir), str([curr_sid,'^'+e]), str(prev_module_ignore_file_patterns)))
                        input_files = file_utils.mergeLists( input_files, createFilePath( prev_module_output_dir, cleanList(pipeline_dict[prev_module]['output'].split(','), ' '), pipeline_dict[curr_module]['module_type'], pipeline_dict[prev_module]['module_type'], curr_sid, all_sids) )
                        # input_files = aws_s3_utils.listSubFiles(prev_module_output_dir, [sid,'^'+e], prev_module_ignore_file_patterns)
                    else:
                        input_files.append( replaceInString(e, {'<sample_id>': curr_sid, '<folder>': prev_module_output_dir}).rstrip('/')+'/' )
                        # input_files.append(prev_module_output_dir.rstrip('/')+'/')
        return input_files

    def getCurrentOutput( base_output_dir, module, pipeline_dict ):
        """ Gets current output files or directory
        """
        module_output_dir = os.path.join( base_output_dir, module ).rstrip('/')+'/'
        return module_output_dir

    def getDependentIDs( curr_module, prev_modules, sid, dependency_dict, pipeline_dict):
        """ Gets the job IDs that this current job depend on, as a list
            dependency_dict looks like:
            {<module>: {<sample_id>: {'job_id': <job_id>,...}}}
           [TO-DO] need more error checking with this
        """
        module_type = pipeline_dict[curr_module]['module_type']
        dep_ids = []
        for prev_module in prev_modules:
            if prev_module != '':
                prev_module_type = pipeline_dict[prev_module]['module_type']
                print('CURRENT  MODULE TYPE: '+str(module_type))
                print('PREVIOUS MODULE TYPE: '+str(prev_module_type))
                if module_type == 'merge' and prev_module_type == 'linear':
                    for s in dependency_dict[prev_module].keys():
                        if 'job_id' in dependency_dict[prev_module][s] and dependency_dict[prev_module][s]['job_id'] != '':
                            dep_ids.append(dependency_dict[prev_module][s]['job_id'])
                else: # module_type = linear
                    if 'job_id' in dependency_dict[prev_module][sid] and dependency_dict[prev_module][sid]['job_id'] != '':
                        dep_ids.append(dependency_dict[prev_module][sid]['job_id'])
        return dep_ids

    def getSubModule( module, pipeline_dict ):
        """ Returns subprogram name if specified in the workflow
        """
        if module in pipeline_dict and 'submodule' in pipeline_dict[module]:
            return pipeline_dict[module]['submodule']
        else:
            return ''
        
    def getModuleSampleId( dependency_dict, module ):
        """ Gets sample IDs for the input module
           [TO-DO] Make this more efficient. Currently looping through lists
        """
        samples_out = []
        if module != '' and module in dependency_dict:
            samples_out = list(dependency_dict[module].keys())
        return samples_out
    
    def createInputJSON( module, sampleid, input_files, output_files, alt_input_files, alt_output_files, \
                         module_args, dependent_ids, jobqueue, isdryrun, scratch_dir, submodule = '' ):
        """ Given sample, I/O, module and job dependency information, create JSON to submit to run batch job
        """
        input_json = {}
        input_json['module'] = module
        input_json['program_subname'] = submodule if submodule not in [[], None] else ''
        input_json["sampleid"] = sampleid
        input_json["input"] = ','.join(input_files) if type(input_files)==type([]) else input_files
        input_json["output"] = ','.join(output_files) if type(output_files)==type([]) else output_files
        input_json["scratchdir"] = scratch_dir
        if alt_input_files != '' and alt_input_files != []:
            input_json["alternate_inputs"] = alt_input_files
        if alt_output_files != '' and alt_output_files != []:
            input_json["alternate_outputs"] = alt_output_files
        if module_args != '' and module_args != []:
            input_json["pargs"] = module_args
        if dependent_ids != '' and dependent_ids != []:
            input_json["dependentid"] = dependent_ids
        if jobqueue != '':
            input_json["jobqueue"] = jobqueue
        if isdryrun:
            input_json["dryrun"] = True
        return input_json

    # read pipeline YAML
    pipeline = args_json['pipeline'].split('.')[0]
    genome = args_json['pipeline'].split('.')[1] if len(args_json['pipeline'].split('.')) > 1 else 'human'
    pipeline_dict = yaml.safe_load(Path('{}.pipeline.yaml'.format(pipeline)).read_text())

    # teamid, userid, runid
    teamid = args_json['teamid']
    userid = args_json['userid']
    runid = args_json['runid'] if ('runid' in args_json and args_json['runid']!='') else '{}-{}'.format(userid, getDateAsString())
    # initialize: get list of modules and module arguments the user wants to run, and initialize job dependencies
    module_list = args_json['modules'].split(',')
    module_args_list = parseStringList(args_json['moduleargs']) if ('moduleargs' in args_json and args_json['moduleargs'] not in ['', []]) else ['']*len(module_list)
    alt_input_list = parseStringList(args_json['altinputs']) if ('altinputs' in args_json and args_json['altinputs'] not in ['', []]) else ['']*len(module_list)
    alt_output_list = parseStringList(args_json['altoutputs']) if ('altoutputs' in args_json and args_json['altoutputs'] not in ['', []]) else ['']*len(module_list)
    sampleids_list = args_json['sampleids'].split(',') if ('sampleids' in args_json and args_json['sampleids'] not in ['', []]) else []
    jobQueue = args_json['jobqueue'] if 'jobqueue' in args_json else ''
    isDryRun = True if ('dryrun' in args_json and (args_json['dryrun'] == True or str(args_json['dryrun']).upper()[0]=='T')) else False
    scratch_dir = args_json['scratchdir'] if 'scratchdir' in args_json and args_json['scratchdir'] != '' else '/home/'
    
    # initial input files REQUIRED - these will feed into first module. Has format {'sampleid': [files],...}
    datafiles_list_by_group = file_utils.groupInputFilesBySample(str(args_json['input']).split(','), sampleids_list)
    print('DATAFILES LIST BY GROUP: '+str(datafiles_list_by_group))
    
    # base_output dir
    # base_output_dir = args_json['output'].rstrip('/')+'/' if ('output' in args_json and args_json['output'] not in ['',[]]) else 's3://{}/{}/{}/runs/{}/'.format(CLIENT_BASE_DIR, teamid, userid, runid)
    base_output_dir = args_json['output'].rstrip('/')+'/' if ('output' in args_json and args_json['output'] not in ['',[]]) else 's3://{}/{}/runs/{}/'.format(CLIENT_BASE_DIR, teamid, runid)

    print('BASE OUTPUT DIR '+str(base_output_dir))
    print('PIPELINE DICT: '+str(pipeline_dict))
    print('INITIAL INPUT FILES: '+str(datafiles_list_by_group))
    print('MODULE_list: '+str(module_list))
    print('module args list: '+str(module_args_list))
    print('alt input list: '+str(alt_input_list))
    print('alt output list: '+str(alt_output_list))
    print('job queue: '+str(jobQueue))

    # if this is a mock run, output parameters with a mock dependencies list, and return
    if 'mock' in args_json and (args_json['mock'] == True or str(args_json['mock']).upper()[0] == 'T'):
        print('MOCK RUN')
        mock_return_dict = {'fastqc': {'rnaseq_mouse_test_tiny1': {'job_id': '86126ddd-7ccf-403c-a1fe-633b5b99adad'}, 'rnaseq_mouse_test_tiny2': {'job_id': '188bc937-fa0d-4b5d-af9c-9f80c6310104'}, 'rnaseq_mouse_test_tiny4': {'job_id': '36244988-bca9-4a0e-af23-240f4ea4b320'}, 'rnaseq_mouse_test_tiny5': {'job_id': '418e2b6c-ab55-41fb-8674-7b71e26a6433'}}, 'rnastar': {'rnaseq_mouse_test_tiny1': {'job_id': '5c7edea8-69d1-4c65-9d33-57e01b2e79d8'}, 'rnaseq_mouse_test_tiny2': {'job_id': '164496e7-9269-4921-ba88-ded8faa27531'}, 'rnaseq_mouse_test_tiny4': {'job_id': '8579109c-41c6-46fc-b6c2-a0df7f7e0db2'}, 'rnaseq_mouse_test_tiny5': {'job_id': '1ac19773-46aa-465d-9c1e-076b20de2ca4'}}, 'expressionqc': {'test-20220714-1722_combined': {'job_id': 'e9c61818-bdec-4dc0-809b-95559396b515'}}, 'deseq2': {'test-20220714-1722_combined': {'job_id': '6a0b6d7f-d346-4fb5-aeaf-049a3e9c56cd'}}, 'deqc': {'test-20220714-1722_combined': {'job_id': '652d8bc8-8794-4aca-a1a6-cb1fd291a4fe'}}, 'david_go': {'test-20220714-1722_combined': {'job_id': 'ff8c849f-fdd8-4965-bd99-62be112a02bb'}}, 'goqc': {'test-20220714-1722_combined': {'job_id': '3c8621bb-2676-4282-b94d-3a7c51d3ccfd'}}}
        return mock_return_dict

    # initialize job IDs dictionary (for managing dependencies and for monitoring)
    dependency_dict = {}
    # initial module
    initial_module = module_list[0]
    # list of sample ids
    sids_all = list(datafiles_list_by_group.keys())
    sids_previous_initial = sids_all # if sampleids_list not in [[],''] else sampleids_list
    
    # now step through and run any modules that appear in the module input list, for each sample
    for i in range(0,len(module_list)):
        print('ON MODULE....'+str(module_list[i]))
        module = module_list[i]
        submodule = getSubModule( module, pipeline_dict )
        prev_modules = getPreviousModule( module, initial_module, module_list, pipeline_dict )  # returns a list of previous modules
        moduleargs = module_args_list[i]
        dependency_dict[module] = {}
        sids = []
        print('PRVEV MODULES...'+str(prev_modules))
        if prev_modules != []:
            for prev_module in prev_modules:
                # if we merge multiple samples, then the sample ID changes to become a merged ID
                if pipeline_dict[module]['module_type'] == 'merge' and \
                   (prev_module in pipeline_dict and pipeline_dict[prev_module]['module_type'] == 'linear'):
                    sids = file_utils.mergeLists( sids, [runid+'_combined'] )  # [sids_all[0]]  # analysis ID is just the first sample ID
                elif prev_module != '':
                    sids_previous = getModuleSampleId( dependency_dict, prev_module )
                    sids = file_utils.mergeLists( sids, sids_previous ) # otherwise the SID is the same as the previous module
                else:
                    sids = sids_previous_initial
        else:
            sids = sids_previous_initial
        print('SIDS... '+str(sids))
        # step through each sample and run current module
        for sid in sids:
            print('ON SAMPLE....'+str(sid))
            # alternate input and output files
            alti = replaceInString(alt_input_list[i], {'<run_id>': runid, '<sample_id>': sid, '<team_id>': teamid, '<user_id>': userid}) if len(alt_input_list) > i else ''
            alto = replaceInString(alt_output_list[i], {'<run_id>': runid, '<sample_id>': sid, '<team_id>': teamid, '<user_id>': userid}) if len(alt_output_list) > i else ''
            # get module template file
            module_template_file = module_utils.downloadModuleTemplate( module, scratch_dir, submodule, 'local' ) # os.path.join( os.getcwd(), module+'.template.json' ) # module_utils.downloadModuleTemplate( module, scratch_dir )
            # input_files of this docker are the output files of the previous docker
            # NEEDS TO HANDLE MULTIPLE PREV MODULES
            input_files = getPreviousOutput( base_output_dir, module, prev_modules, sid, sids_all, pipeline_dict )
            if input_files == []:
                input_files = datafiles_list_by_group[sid]
            print('CURR MODULE: '+str(module))
            print('SUBMODULE: '+str(submodule))
            print('PREV MODULES: '+str(prev_modules))
            print('INPUT FILES: '+str(input_files))
            print('ALT INPUT FILES: '+str(alti))
            print('ALT OUTPUT FILES: '+str(alto))
            
            # set output directory for this module
            module_output = getCurrentOutput( base_output_dir, module, pipeline_dict )

            # create JSON for inputs
            if submodule in ['', [], None]:
                job_input_json = createInputJSON( module, sid, input_files, module_output, \
                                                  alti, alto, moduleargs, \
                                                  getDependentIDs( module, prev_modules, sid, dependency_dict, pipeline_dict), \
                                                  jobQueue, isDryRun, scratch_dir )
            else:
                job_input_json = createInputJSON( module, sid, input_files, module_output, \
                                                  alti, alto, moduleargs, \
                                                  getDependentIDs( module, prev_modules, sid, dependency_dict, pipeline_dict), \
                                                  jobQueue, isDryRun, scratch_dir, submodule )                
            print('JOB_INPUT_JSON: '+str(job_input_json))

            # call runbatchjob()
            job_output_json = run_batchjob( job_input_json )
            print('JOB OUTPUT JSON: '+str(job_output_json))

            # add this job to dependencies dictionary
            if sid not in dependency_dict[module]:
                dependency_dict[module][sid] = {}
            dependency_dict[module][sid]['job_id'] = job_output_json['jobid']
        # keep track of sids of previous DAG module
        sids_previous = sids
    return dependency_dict


if __name__ == '__main__':
    def error(self, message):
        sys.stderr.write('Usage: %s\n' % message)
        self.print_help()
        sys.exit(2)
    argparser = ArgumentParser()
    file_path_group = argparser.add_argument_group(title='Run batch pipeline arguments')
    file_path_group.add_argument('--pipeline', '-p', help='<WHICH_PIPELINE_TO_RUN> - e.g., dnaseq_targeted,rnaseq', required=True)
    file_path_group.add_argument('--teamid', help='team ID for batch runs', required=True)
    file_path_group.add_argument('--userid', help='user ID for batch runs', required=True)
    file_path_group.add_argument('--runid', help='run ID for batch runs', required=False, default='')
    file_path_group.add_argument('--modules', '-m', help='<WHICH_MODULES_TO_RUN> - e.g., fastqc,bwamem_bam,mpileup', required=True)
    file_path_group.add_argument('--input', '-i', help='full path of initial INPUT_FILE(S) list - e.g., s3://fastq/R1.fastq,s3://fastq/R2.fastq. Can also specify a directory and file type to get all files of a file type in a dir, e.g. s3://fastq/^fastq or s3://fastq/* for all files.', required=True)
    file_path_group.add_argument('--output', '-o', help='full path of output directory - e.g., s3://bam/.', required=False, default='')
    file_path_group.add_argument('--moduleargs', '-ma', type=list, help='list of program args for each module, in quotes - e.g., "","-t S","",... - LIST SAME SIZE as --modules. Cannot contain file paths', required=False, default='')
    file_path_group.add_argument('--altinputs', '-alti', type=list, help='alternate input file(s) for each module, e.g., "","s3://fasta/hg38.fasta","",... LIST SAME SIZE AS --modules. ', required=False, default='')
    file_path_group.add_argument('--altoutputs', '-alto', type=list, help='alterate output file(s) for each module', required=False, default='')
    file_path_group.add_argument('--sampleids', help='sample IDs for input files', required=False, default=[])
    file_path_group.add_argument('--dryrun', help='dry run only', required=False, action='store_true')
    file_path_group.add_argument('--jobqueue', help='queue to submit batch job', required=False, default='')
    file_path_group.add_argument('--mock', help='mock run only', required=False, action='store_true')
    file_path_group.add_argument('--scratchdir', help='scratch directory for storing temp files', required=False, default='/home/')
    runpipeline_args = argparser.parse_args()
    p_out = run_pipeline( vars(runpipeline_args) )
    print('JOB IDS and DEPENDENCIES out: ')
    print(p_out)
