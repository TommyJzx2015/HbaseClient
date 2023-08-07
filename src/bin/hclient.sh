#/bin/bash
# author: tommyjiang
# date: 2023-07-31

set -e

# only for ambari metrics collector
#export HBASE_HOME=/usr/lib/ams-hbase
#export HBASE_CONF=/etc/ambari-metrics-collector/conf

# for hbase prd cluster
#export HBASE_CONF=/etc/hbase/conf/

# default for hbase ats-hbase
# export HBASE_CONF=/usr/hdp/3.1.0.0-78/hadoop/conf/embedded-yarn-ats-hbase

# for hadoop components env
export JAVA_HOME=/usr/jdk64/jdk1.8.0_112/
export HADOOP_CONF=${HADOOP_HOME:-/etc/hadoop/conf}
export HBASE_CONF=${HBASE_CONF:-/usr/hdp/3.1.0.0-78/hadoop/conf/embedded-yarn-ats-hbase}
export HADOOP_HOME=${HADOOP_HOME:-/usr/hdp/3.1.0.0-78/hadoop}
export HBASE_HOME=${HBASE_HOME:-/usr/hdp/3.1.0.0-78/hbase}
export HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME:-/usr/hdp/3.1.0.0-78/hadoop-mapreduce}
export HADOOP_YARN_HOME=${HADOOP_YARN_HOME:-/usr/hdp/3.1.0.0-78/hadoop-yarn}
export HADOOP_CLASSPATH=$HADOOP_CONF:$HBASE_CONF
export libpath=$libpath

for i in `ls $HADOOP_HOME/lib/*jar`;
do
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
done

#for i in `ls $HBASE_HOME/lib/hbase-common-2.0.2.3.1.0.0-78.jar $HBASE_HOME/lib/hbase-client-2.0.2.3.1.0.0-78.jar`;
for i in `ls $HBASE_HOME/lib/*.jar`;
do
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
done

for i in `ls $HADOOP_YARN_HOME/*.jar`;
do
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
done

for i in `ls /usr/hdp/3.1.0.0-78/hadoop/client/*jar`;
do
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
done

for i in `ls ./lib/*jar`;
do
export libpath=$libpath:$i
done


dtstr=`date +"%Y%m%d"`
dts=`date +"%s"`
stamp=`echo ${dts:0-6}`
log(){
dt=`date +"%Y-%m-%d %H:%M:%S"`
echo -e "$dt $@"
}
show_usage="args:
-a --action=[list/flush/put/get/scan/Tmerge/merge/alter/major_compact/truncate/truncate_preserve],
-n --namespace=]
-t --tablename=]
-s --startrow=row], get action must have startrow, scan action have default value __allRows__
                  when action=alter,  startrow will be CONFIGURATION,
                  when action=put, startrow can be one rowkey
-e --endrow=row], scan action have default value __allRows__
                  when action=alter, and with columnfamily value, endrow will be columnfamily attributes name,for example TTL,COMPRESSION,DATA_BLOCK_ENCODING
-c --columnfamily=cf,
-f --column=,
-i --limit=, when action=scan, the num rows of scan action
            when action=alter, limit will be attributes value
            when action=put, outfile is null, limit will be cell value
-o --outfile, when action=get/scan/list, the result action will output to this file, default is ./logs/hclient.data
            when action=put, outfile is not null,will read from outfile data put into hbase
-l --lowsizeperregion, the max hfile size of per region joined in merge group
-u --upperregionsize, the max hfile size of new region after merge
-m --maxregiongroupsize, when action=merge, the num regions of merge group
                         when action=put, maxregiongroupsize can be puts group size
-h --help"


GETOPT_ARGS=`getopt -o a:n:t:s:e:c:f:i:o:l:u:m:h -al action:,namespace:,tablename:,startrow:,endrow:,columnfamily:,column:,limit:,outfile:,lowsizeperregion:,upperregionsize:,maxregiongroupsize:,,help -- "$@"`

#echo "$GETOPT_ARGS"
eval set -- "$GETOPT_ARGS"

while [ -n "$1" ]
do
        case "$1" in
                -a|--action) action=$2; shift 2;;
                -n|--namespace) namespace=$2; shift 2;;
                -t|--tablename) tablename=$2; shift 2;;
                -s|--startrow) startrow=$2; shift 2;;
                -e|--endrow) endrow=$2; shift 2;;
                -c|--columnfamily) columnfamily=$2; shift 2;;
                -f|--column) column=$2; shift 2;;
                -i|--limit) limit=$2; shift 2;;
                -o|--outfile) outfile=$2; shift 2;;
                -l|--lowsizeperregion) lowsizeperregion=$2; shift 2;;
                -u|--upperregionsize) upperregionsize=$2; shift 2;;
                -m|--maxregiongroupsize) maxregiongroupsize=$2; shift 2;;
                --) break ;;
                -h|--help) echo -e $show_usage; break ;;
        esac
done

if [[ -z $namespace ]];then
    namespace="default"
fi
if [[ -z $tablename ]] && [[ $action != "list" ]];then
    echo "-t is null, will exit"
    echo "$show_usage"
    exit 1
fi

if [[ $action == "scan" ]];then
    if [[ -z $outfile ]];then
    outfile="./logs/hclient.data"
    fi
elif [[ $action == "get" ]];then
    if [[ -z $startrow ]];then
        echo "-a is get, -s cannot null, will exit"
        echo "$show_usage"
        exit 1
    fi

    endrow=startrow
    limit=0

elif [[ $action = "list" ]];then
    tablename="__table__"
fi

if [[ -z $startrow ]];then
    startrow="__allRows__"
fi
if [[ -z $endrow ]];then
    endrow="__allRows__"
fi
if [[ -z $limit ]];then
    limit=5
fi

if [[ -z $outfile ]];then
    outfile="./logs/hclient.data"
fi


if [[ -z $columnfamily ]];then
    columnfamily="__allCfs__"
fi

if [[ -z $column ]];then
    column="__allColumns__"
fi

if [[ -z $outfile ]];then
    outfile="|"
fi

if [[ -z $lowsizeperregion || -z $upperregionsize ]];then
    lowsizeperregion=5
    upperregionsize=10
fi
if [[ -z $maxregiongroupsize ]];then
    maxregiongroupsize=2
fi

log "[INFO] action:$action,namespace:$namespace,tablename:$tablename"
log "[INFO] startrow:$startrow,endrow:$endrow,columnfamily:$columnfamily,column:$column,limit:$limit,outfile:$outfile"
log "[INFO] lowsizeperregion:$lowsizeperregion,upperregionsize:$upperregionsize,maxregiongroupsize:$maxregiongroupsize"

#鉴权认证
kinit -kt ~/hdtest.keytab hdtest

$JAVA_HOME/bin/java -cp $HADOOP_CLASSPATH:$libpath -Djava.util.logging.config.file=./log4j2.xml com.mytools.hbaseclient.HBaseShellClient $action $namespace $tablename $startrow $endrow $columnfamily $column $limit $outfile $lowsizeperregion $upperregionsize $maxregiongroupsize