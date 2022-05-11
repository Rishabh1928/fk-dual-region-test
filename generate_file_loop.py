import os
import subprocess
dest = "gs://masood-mumbai-2/auto/"
for x in range (1,1000):
    print(str(x)+".txt started..")
    fn = str(x) + ".txt"
    f = open(fn,"wb")
    f.seek(120000-1)
    f.write(b"\0")
    f.close()
    os.stat(fn).st_size
    proc = subprocess.Popen(["gsutil", "-m", "cp", "-r", "-p", fn, dest])
    outs, errs = proc.communicate()
    print("Done")