# pravega-flink-wordcount-sample

to run this application you have to be sure that you have already started pravega in docker or local machine .
if not then open a terminal and type below command;

docker run -it -e HOST_IP=<ip> -p 9090:9090 -p 12345:12345 pravega/pravega:latest standalone

after that open another terminal and type below command 

nc -lk 9999


now put the below argument in StreamingJOb and WordCountWriter program argument filed in intelij

--controller tcp://localhost:9090


and rest of the part is coverd by default values .


now start your StreamingJob.java from IDE.

and then start WordCountWriter.java

then go to the terminal where you have entered  nc -lk 9999 this command 
and then type some msg like 

i
 am 
very
bad
boy
you
are 
very
good
boy

etc 
and then you will see the word count in your output console.

i have followed the material from 

https://github.com/pravega/pravega-samples/

Thank You.


