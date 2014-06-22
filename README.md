# WebHDFS Python Client Implementation

WebHDFS is REST-API to HDFS. To facilitate access to WebHDFS from Python, webhdfs-py was developed. The library can easily be installed via easy_install or pip:

easy_install webhdfs

Webhdfs-py has no further dependencies and solely relies on the Python standard library. Similar to the Python os package, webhdfs-py provides basic capabilities like the creation/listing and deletion of directories on files. 

## Hadoop configuration

Supported Hadoop version: 2.x (including 2.4). Tested with HDP 2.1

Ensure that WebHDFS is enabled in the `hdfs-site.xml`:

Relevant properties:

	<property>
   		<name>dfs.webhdfs.enabled</name>
   		<value>true</value>
	 </property>
	
see <http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>


## Limitations

	* Kerberos security not supported
