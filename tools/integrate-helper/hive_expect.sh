#!/usr/bin/expect -f

spawn hive
expect ">"
send "SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.atlas.hive.hook.HiveHook;\n"
interact