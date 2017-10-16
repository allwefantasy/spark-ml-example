connect jdbc where driver="com.mysql.jdbc.Driver"
    and url="jdbc:mysql://127.0.0.1/alarm_test?characterEncoding=utf8"
    and `user`="root"
    and password="csdn.net"
    as db1;
load jdbc.`db1.t_report` as tr;
load json.`/tmp/json` as a1;

select * from tr  union select * from a1 as new_tr;
save overwrite new_tr as json.`/tmp/todd`;
