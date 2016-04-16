KCL Heka Connectors
---

This is an Amazon Kinesis Consumer Library application written in python. It provides simple unbuffered consumers that output records as-is.


TCP Connector
---

This connector writes records to a TCP socket described by `ADDRESS` such as `127.0.0.1:4567`.


File Buffer Connector
---

This connector writes records to a file described by `FILE_BUFFER`. `FILE_BUFFER` cannot be `/dev/stdout`, because the KCL uses stdin and stdout of the python process to send and receive control messages for its Java MultiLangDaemon.
