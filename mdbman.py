import time
import os
import subprocess
import logging
from elasticsearch import Elasticsearch


class Node:
    def __init__(self, name, cpu, mem):
        self.name = name
        self.cpu = cpu
        self.mem = mem

class Stmt:
    def __init__(self, id, runtime, sqltext):
        self.id = id
        self.runtime = runtime
        self.sqltext = sqltext

logging.basicConfig(filename='am.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

f = open("sysconfig.txt", "r")
nodes = []
global mode
global sessions
global statements
mode = f.read()
es = Elasticsearch(['192.168.0.32:9200'])




def esquery(nodename, metric):
    estracer = logging.getLogger('elasticsearch')
    estracer.setLevel(logging.ERROR)
    if metric == "cpu":
        try:
            return es.search(index="metricbeat-*", body={"query": {"bool": {
                "must": [{"match_all": {}}, {"exists": {"field": "system.cpu.user.pct"}},
                         {"match_phrase": {"beat.hostname": {"query": nodename}}},
                         {"range": {"@timestamp": {"gte": query_ts2, "lte": query_ts1, "format": "epoch_millis"}}}],
                "filter": [], "should": [], "must_not": []}}, "size": 1, "sort": [{"@timestamp": {"order": "desc"}}]})
        except:
            print("the elasticsearch query fails")
            estracer.error("The Elasticsearch query failed")
    elif metric == "mem":
        try:
            return es.search(index="metricbeat-*", body={"query": {"bool": {
                "must": [{"match_all": {}}, {"exists": {"field": "system.memory.used.pct"}},
                         {"match_phrase": {"beat.hostname": {"query": nodename}}},
                         {"range": {"@timestamp": {"gte": query_ts2, "lte": query_ts1, "format": "epoch_millis"}}}],
                "filter": [], "should": [], "must_not": []}}, "size": 1, "sort": [{"@timestamp": {"order": "desc"}}]})
        except:
            print("the elasticsearch query fails")
            estracer.error("The Elasticsearch query failed")
    else:
        print("Wrong metric or unknown node")


def console_out():
    for node in nodes:
        print("Node: " + str(node.name))
        print("CPU: %d" % float(node.cpu * 100) + "%")
        print("Memory: %d" % float(node.mem * 100) + "%")


# check the system resource utilization against the system goal defined
def check_system_goals():
    global mode
    print("Checking system...")
    f_n = open("sysconfig.txt", "r")
    modenew = f_n.read()
    if mode != modenew:
        print("mode has changed...")
        logging.warning("The System Goal has changed to %s", modenew)
        mode = modenew
    if "res" not in mode:
        # the performance case
        analyze_workload(mode)
    else:
        # the resource case
        analyze_workload(mode)


def kill_statements(n):
    global sessions
    print("CPU utilization on node " + n.name + " is " + str(
        float(n.cpu * 100)) + ". Killing statements...")

    active_sessions = get_sessionids()
    sessions = str(active_sessions).splitlines()
    for line in sessions:
        logging.warning("Killed Query with query_id " + line)
        output = os.popen(
            "mysqlslap --user=cs1user --password=password --host=192.168.0.25 --create-schema=t50 --query='KILL CONNECTION " + line + "' --verbose").read()
        logging.warning(output)

def analyze_workload(goal):
    avg_cpu = getAvgMetrics("cpu")
    avg_mem = getAvgMetrics("mem")

    if "res" not in goal:
        logging.warning(goal + " not implemented yet...")
        print("system mode: " + goal + ", Average CPU: " + str(avg_cpu) + "%, Average MEM: " + str(avg_mem) + "%")
        print(goal + " not implemented yet...")  # performance
    else:
        print("system mode: " + goal + ", Average CPU: " + str(avg_cpu) + "%, Average MEM: " + str(avg_mem) + "%")
        if avg_cpu > 40:
            active_statements = get_statements()
            analyze_statements(active_statements, goal)


def get_sessionids():
    active_sessions = os.popen(
        "/usr/local/mariadb/columnstore/bin/mcsadmin getActiveSQLStatements | awk '/([Feb]{3}).*[\d+]/ {print $5}'").read()
    return active_sessions

def get_

def get_statements():
    active_statements = os.popen(
        "/usr/local/mariadb/columnstore/bin/mcsadmin getActiveSQLStatements").read()
    return active_statements

def analyze_statements(statements, goal):
    print(statements)
    print(goal)

def getAvgMetrics(metr):
    node_am = 0
    avg_mem = 0
    avg_cpu = 0
    for node in nodes:
        node_am += 1
        avg_cpu += int(node.cpu * 100)
        avg_mem += int(node.mem * 100)
    avg_cpu = avg_cpu / node_am
    avg_mem = avg_mem / node_am
    if (metr == "cpu"):
        return avg_cpu
    else:
        return avg_mem


while True:
    logging.info("Checking system...")
    query_ts1 = long(round(time.time() * 1000))
    query_ts2 = query_ts1 - 20000

    res_cpu_wal = esquery("walhalla", "cpu")
    res_cpu_lake = esquery("lakeville", "cpu")
    res_cpu_red = esquery("redshire", "cpu")

    res_mem_wal = esquery("walhalla", "mem")
    res_mem_lake = esquery("lakeville", "mem")
    res_mem_red = esquery("redshire", "mem")

    wal = Node(res_cpu_wal['hits']['hits'][0]["_source"]['beat']['hostname'],
               res_cpu_wal['hits']['hits'][0]["_source"]['system']['cpu']['user']['pct'],
               res_mem_wal['hits']['hits'][0]["_source"]['system']['memory']['used']['pct'])
    lake = Node(res_cpu_lake['hits']['hits'][0]["_source"]['beat']['hostname'],
                res_cpu_lake['hits']['hits'][0]["_source"]['system']['cpu']['user']['pct'],
                res_mem_lake['hits']['hits'][0]["_source"]['system']['memory']['used']['pct'])
    red = Node(res_cpu_red['hits']['hits'][0]["_source"]['beat']['hostname'],
               res_cpu_red['hits']['hits'][0]["_source"]['system']['cpu']['user']['pct'],
               res_mem_red['hits']['hits'][0]["_source"]['system']['memory']['used']['pct'])

    nodes.append(wal)
    nodes.append(lake)
    nodes.append(red)
    console_out()
    check_system_goals()
    nodes = []
    time.sleep(10)
