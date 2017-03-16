# mysql-binlog-three
This is a sample implementation of shyiko's [mysql-binlog-connector-java](https://github.com/shyiko/mysql-binlog-connector-java) library.
- It demonstrates the ingestion part of incoming stream.
- The output should be properly formatted for your own use case.

## Run
- To run, first you need a valid MySQL Server which serves binary logs in ROW format.

- Then, you need to prepare a "properties" file to connect that MySQL Server. Sample file is [here](https://github.com/az3/mysql-binlog-three/blob/master/src/main/resources/test.properties).
```
$ cat <<EOF >/tmp/app.prop
jdbcHost = 10.96.5.33
jdbcPort = 3306
jdbcUser = binlog_user_three
jdbcPass = passw0rd
schemaName = schema_three
prometheusPort = 8091
binlogStartFile = mysql-bin.000181
binlogStartPosition = 1053247310
EOF
```

- Finally, you can run application with the following command.
```
$ git clone --depth 1 https://github.com/az3/mysql-binlog-three.git
$ cd mysql-binlog-three
$ mvn clean install
$ java -DCONFIG_FILE=/tmp/app.prop -jar target/mysql-binlog-three.jar
```

- It will parse incoming stream to console (stdout) starting from the given position.
