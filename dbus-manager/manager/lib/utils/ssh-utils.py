#-*- coding: utf-8 -*-
#!/usr/bin/python
import paramiko
import re
import pdb

#pdb.set_trace()

def execShellCmd(ip,username,passwd,callback):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip,2222,username,passwd,timeout=5)
        callback(client)
        client.close()
    except Exception,e:
        client.close()
        print e

def listFiles(sshClient, dir):
    cmd = "ls -lR " + dir
    stdin, stdout, stderr = sshClient.exec_command(cmd)
    out = stdout.readlines()
    list = []
    for p in out:
        fileName = p.replace("\n", "")
        fileName = fileName[fileName.rindex(" ")+1:len(p)]
        #pdb.set_trace()
        if p[0] != "d":
            print fileName

def listJarFiles(sshClient):
    listFiles(sshClient, "/home/dbusssh")



execShellCmd("localhost", "dbusssh", "123456", listJarFiles);
