<?php

namespace xiaosongshu\test;

require_once __DIR__ . '/sever.php';

for($i=0;$i<100;$i++){
    echo $i."\r\n";
    /** 投递普通消息 */
    \xiaosongshu\test\Demo::publish(['name' => 'tom','time'=>time(),'id'=>$i]);
    sleep(1);
}
echo "投递完成\r\n";

