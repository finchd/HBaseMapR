# HBaseMapR

Import from exisiting source - select pom.xml

make sure it will compile, if not, setup jdk path

File - Project Structure

--Artifacts

--- +

---- Select 'gs-spring-boot' compiled output

---- Select build on make.

--- Verify that unnamed.jar was created in out directory - should be 4.3k rougly as of now

scp file to server

$ source test.sh

$ hadoop -jar jarname.jar hello.QueryNumSpeedsOver100

$ /usr/local/HBase/bin/hbase shell
> scan 'results'

verify results are correct, if mistake is made with rowkey:

> truncate 'results'

otherwise if rowkey is same on next run, the value will be overwritten

