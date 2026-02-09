<?php

namespace xiaosongshu\test;

require_once __DIR__ . '/Demo.php';

/** 开启消费，本函数为阻塞，后面的代码不会执行 */
\xiaosongshu\test\Demo::consume();