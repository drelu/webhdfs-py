from webhdfs import WebHDFS
import os, tempfile
import time

webhdfs = WebHDFS("localhost", 50070, "luckow")

webhdfs.mkdir("/hello-world/")

# create a temporary file
f = tempfile.NamedTemporaryFile()
f.write(b'Hello world!\n')
f.flush() 

print "Upload file: " + f.name

webhdfs.copyFromLocal(f.name, 
                      "/hello-world/test.txt")
    
webhdfs.copyToLocal("/hello-world/test.txt",
                    "/tmp/test1.txt")
    
for i in webhdfs.listdir("/hello-world/"):
    print str(i)
    
f.close()
