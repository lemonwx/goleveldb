import os
import subprocess
import sys

def listdir(pwd, kw):
    if not os.path.isdir(pwd):
        return
    files = os.listdir(pwd)
    for f in files:
        if f == ".git":
            continue
        if os.path.isdir(pwd + "/" + f):
            listdir(pwd + "/"  + f, kw)
        else:
            cmd = "grep --color -l \"{0}\" {1}/{2}".format(kw, pwd, f)
            subprocess.call(cmd, shell=True)


def main():
    pwd = ".."
    listdir(pwd, sys.argv[1])


if __name__ == "__main__":
    main()