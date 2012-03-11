import sys, os
import stat
import httplib
import urlparse
import json

import logging
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='webhdfs')

WEBHDFS_CONTEXT_ROOT="/webhdfs/v1"

class WebHDFS(object):       
    """ Class for accessing HDFS via WebHDFS 
    
        To enable WebHDFS in your Hadoop Installation add the following configuration
        to your hdfs_site.xml (requires Hadoop >0.20.205.0):
        
        <property>
             <name>dfs.webhdfs.enabled</name>
             <value>true</value>
        </property>  
    
        see: https://issues.apache.org/jira/secure/attachment/12500090/WebHdfsAPI20111020.pdf
    """
    
    def __init__(self, namenode_host, namenode_port, hdfs_username):
        self.namenode_host=namenode_host
        self.namenode_port = namenode_port
        self.username = hdfs_username
        
    
    def mkdir(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT + path +'?op=MKDIRS&user.name='+self.username
        logger.debug("Create directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('PUT', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        httpClient.close()
        
        
    def rmdir(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT + path +'?op=DELETE&recursive=true&user.name='+self.username
        logger.debug("Delete directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('DELETE', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        httpClient.close()
     
     
    def copyFromLocal(self, source_path, target_path, replication=1):
        url_path = WEBHDFS_CONTEXT_ROOT + "/" + target_path + '?op=CREATE&overwrite=true&user.name='+self.username
        
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('PUT', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        msg = response.msg
        redirect_location = msg["location"]
        logger.debug("HTTP Location: %s"%(redirect_location))
        result = urlparse.urlparse(redirect_location)
        redirect_host = result.netloc[:result.netloc.index(":")]
        redirect_port = result.netloc[(result.netloc.index(":")+1):]
        # Bug in WebHDFS 0.20.205 => requires param otherwise a NullPointerException is thrown
        redirect_path = result.path + "?" + result.query + "&replication="+str(replication) 
            
        logger.debug("Send redirect to: host: %s, port: %s, path: %s "%(redirect_host, redirect_port, redirect_path))
        fileUploadClient = httplib.HTTPConnection(redirect_host, 
                                                  redirect_port, timeout=600)
        # This requires currently Python 2.6 or higher
        fileUploadClient.request('PUT', redirect_path, open(source_path, "r").read(), headers={})
        response = fileUploadClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        httpClient.close()
        fileUploadClient.close()
        return response.status
        
        
    def copyToLocal(self, source_path, target_path):
        url_path = WEBHDFS_CONTEXT_ROOT + "/" + source_path+'?op=OPEN&overwrite=true&user.name='+self.username
        logger.debug("GET URL: %s"%url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        # if file is empty GET returns a response with length == NONE and
        # no msg["location"]
        if response.length!=None:
            msg = response.msg
            redirect_location = msg["location"]
            logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
            logger.debug("HTTP Location: %s"%(redirect_location))
            result = urlparse.urlparse(redirect_location)
            redirect_host = result.netloc[:result.netloc.index(":")]
            redirect_port = result.netloc[(result.netloc.index(":")+1):]
            
            redirect_path = result.path + "?" + result.query  
                
            logger.debug("Send redirect to: host: %s, port: %s, path: %s "%(redirect_host, redirect_port, redirect_path))
            fileDownloadClient = httplib.HTTPConnection(redirect_host, 
                                                      redirect_port, timeout=600)
            
            fileDownloadClient.request('GET', redirect_path, headers={})
            response = fileDownloadClient.getresponse()
            logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
            
            # Write data to file
            target_file = open(target_path, "w")
            target_file.write(response.read())
            target_file.close()
            fileDownloadClient.close()
        else:
            target_file = open(target_path, "w")
            target_file.close()
            
        httpClient.close()        
        return response.status
     
     
    def listdir(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT +path+'?op=LISTSTATUS&user.name='+self.username
        logger.debug("List directory: " + url_path)
        httpClient = self.__getNameNodeHTTPClient()
        httpClient.request('GET', url_path , headers={})
        response = httpClient.getresponse()
        logger.debug("HTTP Response: %d, %s"%(response.status, response.reason))
        data_dict = json.loads(response.read())
        logger.debug("Data: " + str(data_dict))
        files=[]        
        for i in data_dict["FileStatuses"]["FileStatus"]:
            logger.debug(i["type"] + ": " + i["pathSuffix"]) 
            files.append(i["pathSuffix"])        
        httpClient.close()
        return files
    
    def __getNameNodeHTTPClient(self):
        httpClient = httplib.HTTPConnection(self.namenode_host, 
                                            self.namenode_port, 
                                                       timeout=600)
        return httpClient
    
    
    
if __name__ == "__main__":      
    webhdfs = WebHDFS("localhost", 50070, "luckow")
    webhdfs.mkdir("/pilotstore-1/pd-9c2d42c4-30a3-11e1-bab1-00264a13ca4c/")
    webhdfs.copyFromLocal("/Users/luckow/workspace-saga/applications/pilot-store/test/data1/test1.txt", 
                              "/pilotstore-1/pd-9c2d42c4-30a3-11e1-bab1-00264a13ca4c/test1.txt")
    
    webhdfs.copyToLocal("/pilotstore-1/pd-9c2d42c4-30a3-11e1-bab1-00264a13ca4c/test1.txt",
                        "/tmp/test1.txt")
    
    webhdfs.listdir("/")
        
        
        
