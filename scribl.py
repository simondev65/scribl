#!/usr/bin/python3

# Written by Jim Apger, Cribl
#
# This is to be run on a Splunk Indexer for the purpose of exporting buckets and streaming their contents to Cribl Stream
#
# Example:  scribl.py -d /opt/splunk/var/lib/splunk/bots/db/ -r 34.220.39.122 -p 20000 -t -n4 -l /tmp/scribl.log -et 1564819155 -lt 1566429310#
#
# Make sure nc (netcat) is in the path or hard code it below to fit your needs

import argparse,os,subprocess,sys,time,logging,shlex
from multiprocessing import Pool

logger=logging.getLogger("scribl")
logger.propagate=False

LOG_LEVELS=("CRITICAL","ERROR","WARNING","INFO","DEBUG")

def getArgs(argv=None):
    parser = argparse.ArgumentParser(description="This is to be run on a Splunk Indexer for the purpose\
            of exporting buckets and streaming their contents to Cribl Stream")
    parser.add_argument("-t","--TLS", help="Send with TLS enabled", action='store_true')
    parser.add_argument("-n","--numstreams", default="1", type=int, help="the number of parallel stream to utilize")
    parser.add_argument("-l","--logfile", default="/tmp/SplunkToCribl.log", help="specify the location to write/append the logging")
    parser.add_argument("--log-level", default="INFO", choices=LOG_LEVELS, help="set the logging verbosity")
    parser.add_argument("-et","--earliest", default=0, type=int, help="specify the earliest epoch time for bucket selection")
    parser.add_argument("-lt","--latest", default=9999999999, type=int, help="specify the latest epoch time for bucket selection")
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument("-d","--directory", help="Source directory containing the buckets", required=True)
    requiredNamed.add_argument("-r","--remoteIP", help="Remote address to send the exported data to", required=True)
    requiredNamed.add_argument("-p","--remotePort", help="Remote TCP port to be used", required=True)
    return parser.parse_args(argv)

def list_full_paths(directory,earliest,latest):
    logger.debug("Inspecting directory %s for buckets between %s and %s",directory,earliest,latest)
    dirs=os.listdir(directory)
    dirs=[x for x in dirs if x.startswith("db_")]   #keep only the dirs that we know contains buckets
    for dir in dirs:
        dirParsed=dir.split('_')
        maxEpoch=dirParsed[1]
        minEpoch=dirParsed[2]
        if not earliest < int(minEpoch) and latest > int(maxEpoch):
            dirs.remove(dir)
    return [os.path.join(directory, file) for file in dirs]

TEMP_DIR="/tmp/scribl"

def ensure_temp_dir():
    os.makedirs(TEMP_DIR, exist_ok=True)
    logger.debug("Ensured temp dir exists at %s",TEMP_DIR)

def buildCmdList(buckets,args):
    ensure_temp_dir()
    cliCommands=[]
    timestamp=int(time.time())
    for idx,bucket in enumerate(buckets):
        quoted_bucket=shlex.quote(bucket)
        temp_file=os.path.join(TEMP_DIR,f"{os.path.basename(bucket)}_{idx}_{timestamp}.csv")
        quoted_temp=shlex.quote(temp_file)
        exporttoolCmd="/opt/splunk/bin/splunk cmd exporttool "+quoted_bucket+" "+quoted_temp+" -csv && (cat "+quoted_temp+" | nc "
        if args.TLS:
            exporttoolCmd+="--ssl "
        exporttoolCmd+=args.remoteIP
        exporttoolCmd+=" "+args.remotePort
        exporttoolCmd+=") && rm -f "+quoted_temp
        cliCommands.append(exporttoolCmd)
        logger.debug("Queued command for bucket %s -> %s",bucket,temp_file)
    return cliCommands

def runCmd(cmd):
        startTime=time.time()
        logger.info("Starting command: %s",cmd)
        try:
            p = subprocess.Popen(cmd,  shell=True, encoding='utf-8',stderr=subprocess.PIPE,stdout=subprocess.PIPE)
            stdout_data, stderr_data = p.communicate()
            if stdout_data:
                logger.debug("STDOUT for command %s:\n%s",cmd,stdout_data)
            if stderr_data:
                logger.warning("STDERR for command %s:\n%s",cmd,stderr_data)
            rc=p.returncode
            duration=time.time()-startTime
            if rc==0:
                logger.info("Finished in %s seconds: %s ",duration,cmd)
            else:
                logger.error("Command failed with exit code %s after %s seconds: %s",rc,duration,cmd)
        except Exception as exc:
            logger.exception("Exception while running command %s: %s",cmd,exc)
            raise

def setup_logging(logfile,level_name):
    level=getattr(logging,level_name.upper(),logging.INFO)
    logger.setLevel(level)
    logger.handlers.clear()
    formatter=logging.Formatter("%(asctime)s %(levelname)s [%(processName)s] %(message)s")
    file_handler=logging.FileHandler(logfile,'a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler=logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.debug("Logger initialized at level %s writing to %s",level_name,logfile)

def init_worker(logfile,level_name):
    setup_logging(logfile,level_name)

def main():
    argvals = None
    args = getArgs(argvals)
    setup_logging(args.logfile,args.log_level)
    logger.info('\n------------\nStarting a new export using %i streams', args.numstreams)
    logger.info('Beginning Script with these args: %s',' '.join(f'{k}={v}' for k, v in vars(args).items()))
    startTime=time.time()
    buckets=(list_full_paths(args.directory,args.earliest,args.latest))
    if args.earliest < args.latest:
        logger.info('Search Min epoch = %i and Max epoch = %i',args.earliest,args.latest)
    else:
        logger.error('ERROR:  The specified Min epoch time (%i) must be less than the specified Max epoch time(%i)',args.earliest,args.latest)
        exit(1)
    logger.info('There are %s buckets in this directory that match the search criteria',len(buckets))
    logger.debug('Exporting these buckets: %s',buckets)
    cliCommands=buildCmdList(buckets,args)
    with Pool(args.numstreams,initializer=init_worker,initargs=(args.logfile,args.log_level)) as p:
        p.map(runCmd,cliCommands)
    logger.info('Done with script in %s seconds',time.time()-startTime)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        if not logger.handlers:
            logging.basicConfig(stream=sys.stderr, level=logging.ERROR)
        logger.exception("Fatal error encountered. See traceback for details.")
        raise

