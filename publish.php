<?php

namespace xiaosongshu\test;

require_once __DIR__ . '/sever.php';
require_once __DIR__ . '/sever2.php';

for ($i = 0; $i < 100; $i++) {
    echo $i . "\r\n";
    /** 投递普通消息 */
    \xiaosongshu\test\Demo::publish(['name' => 'tom1', 'time' => time(), 'id' => $i]);
    \xiaosongshu\test\Demo4::publish(['name' => 'tom4', 'time' => time(), 'id' => $i]);
    sleep(1);
}
echo "投递完成\r\n";

