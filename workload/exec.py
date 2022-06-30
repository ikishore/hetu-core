import itertools
import os

#input the filename which contains the batch of queries and the installationDir
#installationDir = '/scratch/venkates/openlookeng-working/'
#workloadPath = '/home/venkates/BQO/final-T2-evaluation/varyingJoin.sql'
workload = 'varyingJoin.sql'
#workload = 'inputBatch.sql'
#queries required for memory connector #need to change the scale factor accordingly
warmupQueries = 'tiny_warmup.sql'

cmd_warmup_path = 'java -jar '+'../presto-cli/target/hetu-cli-1.2.0-SNAPSHOT-executable.jar -f '+warmupQueries

cmd_values_path = 'java -jar ../presto-cli/target/hetu-cli-1.2.0-SNAPSHOT-executable.jar -f values.sql'

cmd_workload_path = 'java -jar ../presto-cli/target/hetu-cli-1.2.0-SNAPSHOT-executable.jar -f  ' + workload

#cmd_client_workload = 'java -jar ../presto-cli/target/hetu-cli-1.2.0-SNAPSHOT-executable.jar -f  /scratch/venkates/openlookeng-working/scratch-space/topOrderTest.sql'

cmd_top_order_workload = 'java -jar ../presto-cli/target/hetu-cli-1.2.0-SNAPSHOT-executable.jar -f  top_order.sql'

#query_order_path = '../hetu-server/target/hetu-server-1.2.0-SNAPSHOT/data/topOrder.txt'
query_order_path = '../presto-main/topOrder.txt'

pipe = os.popen(cmd_warmup_path)
print (pipe.read())

pipe = os.popen(cmd_workload_path)
print (pipe.read())

pipe = os.popen(cmd_values_path)
print (pipe.read())


queries = ''
with open(workload, 'r') as qfile:
    queries = qfile.read()
    print (queries)

individual_queries = queries.split(';')

query_dict = {}
cnt = 1
session_str = ''
for q in individual_queries:
    q = q.strip() 
    if not "session" in q and   q != ''  :
        query_dict.update({cnt:q+';\n'})
        cnt += 1
    elif "session" in q and not 'batch_size' in q:
        session_str += (q + ';\n')
    
with open(query_order_path, 'r') as file:
    order  = file.read()


#print ('before pop topological order '+str(topological_order))
topological_order = order.split(',')
topological_order.pop()
topological_order = list(map(int, topological_order))
print (' topological order '+str(topological_order))

#topological_order = [6,5,4,3,2,1]

new_queries = session_str
for i in topological_order:
    new_queries += query_dict[i]

print (new_queries)

#with open('top_order_workload_varyJoins.sql', 'w') as text_file:
with open('top_order.sql', 'w') as text_file:
    text_file.write(new_queries)

pipe = os.popen(cmd_top_order_workload)
print (pipe.read())
