<?php
//用多个进程来跑行情的任务
ini_set('date.timezone', 'Asia/Shanghai');

for ($j = 0; $j < 15; $j++) {//15个进程
    $pid = pcntl_fork();
    switch ($pid) {
        case -1:
            die("Fork failed\n");
        case 0:
            $id = posix_getpid();
            $_s_time =  microtime(true);
            $redis = new Redis();
            $redis->connect('127.0.0.1', 6379);
            //$dir_path = "/data/kzks/code/txt/";
            $dir_path = "/mnt/";
            $files = scandir($dir_path);
            $push_arr = [];
            $fcode_last = [];
            $fcode_list = [];
            foreach ($files as $key => $value) {
                if (!strstr($value, 'mktdt00')) {
                    continue;
                }
                $handler = fopen($dir_path . $value, "r");

                $fork_start = $j * 100;//每个进程处理一百个
                if ($fork_start > 0) {
                    $fork_start = $fork_start - 1;
                }
                $fork_end = ($j + 1) * 100;
                if ($j == 14) {
                    $fork_end = 1501;
                }

                $i = 0;
                while (!feof($handler)) {
                    $m = fgets($handler, 1024); //fgets逐行读取，4096最大长度，默认为1024
                    if (substr_count($m, 'MD003') > 0) {
                        break; //以后要是数据有问题，注意这块的影响
                    }
                    if (substr_count($m, 'MD002|6') > 0) {
                        $i++;
                    }
                    if ($i > $fork_start && $i < $fork_end && substr_count($m, 'MD002|6')) {
                        $lm = str_replace(' ', '', $m);
                        $_str = explode('|', $lm);
                        /***********************last list start****************************/
                        $NPRI = $_str['9']; //最新价格
                        $PPRI = $_str['5']; //昨天收盘价格
                        $SCOD = $_str['1']; //产品代码
                        $HPRI = $_str['7'];; //最高价
                        $LPRI = $_str['8']; //最低价
                        $SNAM = $_str['2']; //股票名称
                        $TVAL = $_str['4']; //成交额
                        $TVOL = $_str['3']; //成交量
                        $OPRI = $_str['6']; //开盘价
                        $CRAT = ($NPRI - $PPRI) / $PPRI * 100; //时间区间涨跌幅->（现价-昨收）/ 昨收 * 100
                        $CRAT = floor($CRAT * 1000) / 1000; //不四舍五入
                        $CVAL = $NPRI - $PPRI; //涨跌  现价 - 昨收
                        $FCOD = 'sh' . $SCOD; //产品代码  sh
                        $ZF = (($HPRI - $LPRI) / $PPRI) * 100; //震幅
                        $SellPRI1 = $_str['13']; //申卖价一
                        $SellPRI2 = $_str['17']; //申卖价二
                        $SellPRI3 = $_str['21']; //申卖价三
                        $SellPRI4 = $_str['25']; //申卖价四
                        $SellPRI5 = $_str['29']; //申卖价5

                        $SellHAND1 = $_str['14']; //申卖量1
                        $SellHAND2 = $_str['18']; //申卖量2
                        $SellHAND3 = $_str['22']; //申卖量3
                        $SellHAND4 = $_str['26']; //申卖量4
                        $SellHAND5 = $_str['30']; //申卖量5


                        $BuyPRI1 = $_str['11']; //申买价1
                        $BuyPRI2 = $_str['15']; //申买价2
                        $BuyPRI3 = $_str['19']; //申买价3
                        $BuyPRI4 = $_str['23']; //申买价4
                        $BuyPRI5 = $_str['27']; //申买价4

                        $BuyHAND1 = $_str['12']; //申买价1
                        $BuyHAND2 = $_str['16']; //申买价2
                        $BuyHAND3 = $_str['20']; //申买价3
                        $BuyHAND4 = $_str['24']; //申买价4
                        $BuyHAND5 = $_str['28']; //申买价5
                        $h_str = $NPRI . ',' . $PPRI . ',' . $SCOD . ',' . $FCOD . ',' . $HPRI . ',' . $LPRI . ',' . $SNAM . ',' . $TVAL . ',' . $TVOL . ',' . $OPRI . ',' . $CRAT . ',' . $CVAL . ',' . $ZF . ',' . $SellPRI1 . ',' . $SellPRI2 . ',' . $SellPRI3 . ',' . $SellPRI4 . ',' . $SellPRI5 . ',' . $SellHAND1 . ',' . $SellHAND2 . ',' . $SellHAND3 . ',' . $SellHAND4 . ',' . $SellHAND5 . ',' . $BuyPRI1 . ',' . $BuyPRI2 . ',' . $BuyPRI3 . ',' . $BuyPRI4 . ',' . $BuyPRI5 . ',' . $BuyHAND1 . ',' . $BuyHAND2 . ',' . $BuyHAND3 . ',' . $BuyHAND4 . ',' . $BuyHAND5;
                        $fcode_last[$FCOD] = $h_str;
                        /**********************last list end*****************************/


                        //分时
                        $f_time = date('Hi', strtotime($_str['32']));
                        $f_time_key = $f_time . $_str[1];
                        /****************最高价 start ****************/
                        if (!isset($push_arr[$f_time_key]['HPRI'])) {
                            $push_arr[$f_time_key]['HPRI'] = 0;
                        }
                        if ($push_arr[$f_time_key]['HPRI'] < $_str['9']) {
                            $push_arr[$f_time_key]['HPRI'] = $_str['9'];
                        }

                        if ($f_time == '0930') {
                            $push_arr[$f_time_key]['HPRI'] = $_str['6'];
                        }
                        /****************最高价 end ****************/


                        /****************最低价 start ****************/
                        if (!isset($push_arr[$f_time_key]['LPRI'])) {
                            $push_arr[$f_time_key]['LPRI'] = 2000;
                        }
                        if ($push_arr[$f_time_key]['LPRI'] > $_str['9']) {
                            $push_arr[$f_time_key]['LPRI'] = $_str['9'];
                        }

                        /****************最低价 end ****************/


                        /****************成交量 start ****************/
                        $push_arr[$f_time_key]['TVOL'] = $_str['3'];
                        /****************成交量 end ****************/

                        /****************成交量 start ****************/
                        $push_arr[$f_time_key]['TVOL'] = $_str['3'];
                        /****************成交量 end ****************/

                        /****************成交金额 start ****************/
                        $push_arr[$f_time_key]['TVAL'] = $_str['4'];
                        /****************成交金额 end ****************/

                        /****************开盘价格 start ****************/
                        if (!isset($push_arr[$f_time_key]['OPRI'])) {
                            $push_arr[$f_time_key]['OPRI'] = $_str['9'];
                        }
                        if ($f_time == '0930') {
                            $push_arr[$f_time_key]['OPRI'] = $_str['6'];
                        }
                        /****************开盘价格 end ****************/


                        /****************现价 start ****************/
                        $push_arr[$f_time_key]['NPRI'] = $_str['9'];
                        /****************现价 end ****************/

                        /****************上收价 start ****************/
                        if ($f_time == '0930') {
                            $push_arr[$f_time_key]['PPRI'] = $_str['5'];
                        } else {
                            $kk = '0' . (string) ($f_time - 1);
                            $kk .= $_str['1'];
                            if (isset($push_arr[$kk])) {
                                $push_arr[$f_time_key]['PPRI'] = $push_arr[$kk]['NPRI'];
                            }
                        }
                        /****************上收价 end ****************/


                        /****************K线区间 涨跌 end ****************/
                        if ($f_time == '0930') {
                            $push_arr[$f_time_key]['KCVAL'] = $_str['9'] - $_str['5'];
                        } else {
                            isset($push_arr[$kk]['NPRI']) && $push_arr[$f_time_key]['KCVAL'] = $_str['9'] - $push_arr[$kk]['NPRI'];
                        }
                        /****************K线区间 涨跌 end ****************/


                        /****************K线区间 涨跌幅 end ****************/
                        if ($f_time == '0930') {
                            $push_arr[$f_time_key]['KCRAT'] = ($_str['9'] - $_str['5']) / $_str['5'] * 100;
                        } else {
                            isset($push_arr[$kk]['NPRI']) && $push_arr[$f_time_key]['KCRAT'] = ($_str['9'] - $push_arr[$kk]['NPRI']) / $push_arr[$kk]['NPRI'] * 100;
                        }
                        /****************K线区间 涨跌幅 end ****************/


                        /****************分时成交数量 start ****************/
                        if ($f_time == '0930') {
                            $_this_key = '0925' . $SCOD;
                            if (!isset($push_arr[$_this_key])) {
                                $_this_key = '0000' . $SCOD;
                            }
                            $push_arr[$f_time_key]['MTVOL'] = $_str['3'] - $push_arr[$_this_key]['TVOL'];
                        } else {
                            $kk = '0' . (string) ($f_time - 1);
                            $kk .= $_str['1'];
                            if (isset($push_arr[$kk])) {
                                $push_arr[$f_time_key]['MTVOL'] = $_str['3'] - $push_arr[$kk]['TVOL'];
                            }
                        }
                        /****************分时成交数量 end ****************/


                        /****************分时成交金额 start ****************/
                        if ($f_time == '0930') {
                            $push_arr[$f_time_key]['MTVAL'] = $_str['4'] - $push_arr[$_this_key]['TVAL'];
                        } else {
                            $kk = '0' . (string) ($f_time - 1);
                            $kk .= $_str['1'];
                            if (isset($push_arr[$kk])) {
                                $push_arr[$f_time_key]['MTVAL'] = $_str['4'] - $push_arr[$kk]['TVAL'];
                            }
                        }
                        /****************分时成交金额 end ****************/


                        /****************涨跌 start ****************/
                        $_cc = $_str['9'] - $_str['5'];
                        $push_arr[$f_time_key]['CVAL'] = $_cc;
                        /****************涨跌 end ****************/

                        /****************涨跌幅 start ****************/
                        $fzd = $_cc / $_str['5'] * 100;
                        $push_arr[$f_time_key]['CRAT'] = $fzd;
                        /****************涨跌幅 end ****************/


                        /****************均价 start ****************/
                        if ($_str['3'] > 0) {
                            $_AVPRI = $_str['4'] / $_str['3'];
                            $push_arr[$f_time_key]['AVPRI'] = $_AVPRI;
                        } else {
                            $_AVPRI = 0;
                        }
                        /****************均价 end ****************/


                        ////////////////////// list /////////////////////
                        $Dtime = date('YmdHis', strtotime($_str['32']));
                        if (!isset($fcode_list[$FCOD])) {
                            $SSCJJE = $TVAL;
                            $SSCJSL    = $TVOL / 100;
                            $fcode_list[$FCOD][$Dtime] =  $_str['2'] . ',' . $_str['8'] . ',' . $_str['7'] . ',' . $_str['9'] . ',' . $TVAL . ',' . $TVOL . ',' . $_str['5'] . ',' . $_str['6'] . ',' . $_cc . ',' . $fzd . ',' . $_AVPRI . ',' . $SSCJJE . ',' . $SSCJSL;
                        }
                        if (!array_key_exists($Dtime, $fcode_list[$FCOD])) {
                            $_l_arr = end($fcode_list[$FCOD]);
                            $_lo = explode(',', $_l_arr);
                            if (is_numeric($_lo['4'])) {
                                $SSCJJE = $TVAL - $_lo['4'];
                            }
                            $SSCJSL    = ($TVOL - $_lo['5']) / 100;
                            $fcode_list[$FCOD][$Dtime] =  $_str['2'] . ',' . $_str['8'] . ',' . $_str['7'] . ',' . $_str['9'] . ',' . $TVAL . ',' . $TVOL . ',' . $_str['5'] . ',' . $_str['6'] . ',' . $_cc . ',' . $fzd . ',' . $_AVPRI . ',' . $SSCJJE . ',' . $SSCJSL;
                        }
                        ////////////////////// list /////////////////////

                    }
                    if ($i > $fork_end || $i == 1549) {
                        break;
                    }
                }
            }

            $_pipe_list = $redis->multi(Redis::PIPELINE);
            foreach ($fcode_list as $k => $v) {
                foreach ($v as $key => $value) {
                    $value .= ',' . $key;
                    $_pipe_list->zadd('list:wsq:' . $k, $key, $value);
                }
            }

            foreach ($fcode_last as $k => $v) {
                $_arr = explode(',', $v);
                $_pipe_list->zadd('top_wsq:crat', $_arr['10'], $k);
                $_pipe_list->zadd('top_wsq:npri', $_arr['0'], $k);
                $_pipe_list->hset('last:code_wsq', $k, $v);
            }

            foreach ($push_arr as $k => $v) {
                $_k_t = substr($k, 0, 4);
                if ($_k_t == '0925') {
                    continue;
                }
                $fcode = substr($k, 4);
                $v['Time'] = $_k_t;
                $_pipe_list->zadd('fswsq:' . $fcode, $_k_t, json_encode($v));
            }
            $_pipe_list->exec();

            $_e_time =  microtime(true);
            echo $_e_time - $_s_time . PHP_EOL;
            exit;
            break;
        default:
            $pids[$pid] = $pid;
            break;
    }
}

//如果子进程还没结束，那么父进程就会一直等等等，如果子进程已经结束，那么父进程就会立刻得到子进程状态。这个函数返回退出的子进程的进程ID或者失败返回-1
//如果字进程在父进程之前退出，则会出现僵尸进程
while (count($pids)) {
    if (($id = pcntl_wait($status, WUNTRACED)) > 0) {
        unset($pids[$id]);
    }
}
