import subprocess
import os

pwd1 = "testdb/"
pwd2 =  "/Users/limeng/space/c/leveldb/build/testdb/"

def chk(name):
    cmd = "cmp {0} {1}".format(pwd1 + "/" + name, pwd2 + "/" + name)
    subprocess.call(cmd, shell=True)

if __name__ == "__main__":
    f1s = os.listdir(pwd1)
    f2s = os.listdir(pwd2)
    for f1, f2 in zip(f1s, f2s):
        if f1 != f2:
            print "error: not equal", f1, f2
    for f in f1s:
        chk(f)