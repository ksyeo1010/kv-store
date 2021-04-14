import subprocess
import os
import signal
from threading import Thread
from time import sleep

# default commands
tracing = "./tracing-server"
frontend = "./frontend"
test = ('go', 'run', os.path.join(os.getcwd(), 'cmd', 'test', 'test.go'))

# storage nodes
start_port = 8010

# trace output
trace_output = 'trace_output.log'

def read_trace(id, stop):
    strings = (f'storage{id}', 'FrontEndStorageFailed')
    while True:
        if stop():
            break
        sleep(0.2) # sleep 200 milliseconds b4 retrying
        file = open(trace_output, 'r')
        for line in file:
            if all(s in line for s in strings):
                file.close()
                return
        file.close()

# id:               the id of storage, the port will be based on this id
# alive:            if the storage node will be alive forever, other args are ignored.
# kill_after:       kill storage after input milliseconds have passed
# restart_after:    restart after AFTER READING STORAGE FAIL ON TRACE and waiting for input milliseconds
# continuous:       will continuously kill and restart
def start_storage(id, alive, kill_after, restart_after, continuous, stop):
    command = ('./storage', '--id', f'storage{id}', '--listen', f'127.0.0.1:{start_port+id}', '--path', f'./data/s{id}')
    if alive:
        proc = subprocess.Popen(args=command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        while True:
            if stop():
                break
    else:
        while True:
            if stop():
                break
            proc = subprocess.Popen(args=command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            # wait before killing
            sleep(kill_after/1000)
            proc.terminate()
            # if this is continuous
            if not continuous:
                while True: # infinite block
                    if stop():
                        break
            t = Thread(target=read_trace, args=(id, stop))
            t.start()
            t.join()
            sleep(restart_after/1000)
    proc.terminate()

def main():
    # recompile
    subprocess.run('make')

    # tracing server
    ts = subprocess.Popen(tracing, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sleep(1)

    # frontend
    f = subprocess.Popen(frontend, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sleep(1)

    # static storages
    stop_storage = False
    s1 = Thread(target=start_storage, args=(1, False, 10000, 1000, False, lambda: stop_storage))
    s2 = Thread(target=start_storage, args=(2, False, 1000, 2000, True, lambda: stop_storage))
    s3 = Thread(target=start_storage, args=(3, True, 0, 0, False, lambda: stop_storage))
    s4 = Thread(target=start_storage, args=(4, False, 2000, 1000, True, lambda: stop_storage))

    # all storages
    storages = {
        'storage1': s1,
        'storage2': s2,
        'storage3': s3,
        'storage4': s4
    }

    for _, s in storages.items():
        s.start()

    # test command
    t = subprocess.run(args=test, capture_output=True, text=True)

    test_cmd = t.stdout

    for s_id in storages:
        test_cmd += f" -s {s_id}"
    test_cmd += ' > grade.txt'
    print(test_cmd)

    stop_storage = True
    for _, s in storages.items():
        s.join()
        
    f.terminate()
    ts.terminate()


if __name__ == '__main__':
    main()