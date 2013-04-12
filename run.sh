export CLASSPATH=$CLASSPATH:/etc/hadoop/conf:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/.//*:/usr/lib/hadoop-hdfs/./:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/hadoop-hdfs/.//*:/usr/lib/hadoop-yarn/lib/*:/usr/lib/hadoop-yarn/.//*:/usr/lib/hadoop-0.20-mapreduce/./:/usr/lib/hadoop-0.20-mapreduce/lib/*:/usr/lib/hadoop-0.20-mapreduce/.//*
export HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.log.dir=/usr/lib/hadoop/logs -Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=/usr/lib/hadoop -Dhadoop.id.str= -Dhadoop.root.logger=DEBUG,console -Djava.library.path=/usr/lib/hadoop/lib/native -Dhadoop.policy.file=hadoop-policy.xml -Djava.net.preferIPv4Stack=true -Dhadoop.security.logger=INFO,NullAppender"
export HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec
. $HADOOP_LIBEXEC_DIR/hadoop-config.sh
/usr/lib/jvm/jdk1.6.0_43/bin/java -Xmx1000m $HADOOP_OPTS -cp target/RecoEngine-1.0-SNAPSHOT.jar:$CLASSPATH com.mongodb.hadoop.examples.wordcount.WordCount3 /user/ashok/input /user/ashok/output2 -D mapred.child.java.opts=-Xmx2048M
