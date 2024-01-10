import sys
import random
import string
import random
import string
import concurrent.futures
from rediscluster import RedisCluster

MAX_THREADS = 1000

NUMBER_OF_KEY_VALUES = 1000

VALUE_LEN_IN_BYTES = 10
startup_nodes = []


def setTask( all_rcs, key, value):
    rc = random.choice(list(all_rcs.values()))
    return rc.set(key, value)

def main():
#    print('Number of arguments:', len(sys.argv), 'arguments.')
#    print('Argument List:', str(sys.argv))
    new_argv = list(sys.argv)
    new_argv = new_argv[1:]
    for arg in new_argv:
        try:
            ip = arg.split(':')[0]
            port = arg.split(':')[1]
            startup_nodes.append({"host": ip, "port": port})
        except NameError:
            print("Arguments should be in format ip:port")
        except IndexError:
            print("Arguments should be in format ip:port")
    
            
#    print(startup_nodes)
    all_rcs = {}
    for i in range(0, 5):
        all_rcs[i] = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

    rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    
    keysToInsert = [ 'key'+str(i) for i in range(0,NUMBER_OF_KEY_VALUES) ] 
    strLambda = lambda x: (str(x) + '_') + ''.join(random.choices(string.ascii_letters, k=VALUE_LEN_IN_BYTES-3))
    valuesToInsert = [strLambda(i) for i in range(0, NUMBER_OF_KEY_VALUES)]
    total_acks = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers = MAX_THREADS) as executor:
        redisSets = {executor.submit(setTask, all_rcs, keysToInsert[i], valuesToInsert[i]): i for i in range(0, NUMBER_OF_KEY_VALUES)}
        for future in concurrent.futures.as_completed(redisSets):
            redis_SET_return = redisSets[future]
            try:
                data = future.result()
            except Exception as exc:
                print("Something went wrong on the executor")
            else:
#                print(data)
                if(data == True):
                    total_acks = total_acks + 1
        if(total_acks == NUMBER_OF_KEY_VALUES):
            print("KEYS ADDED, TOTAL_KEYS:" + str(total_acks))

if __name__ == '__main__':
          main()
