<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Utils;

class ArrayUtil
{
    /**
     * @param $needle
     * @param $array
     * @return mixed|null
     */
    public static function searchByRegexKey($needle, $array)
    {
        $targetValue = null;
        foreach ($array as $regexKey => $value) {
            if (preg_match('/' . $regexKey . '/', $needle)) {
                $targetValue = $value;
            }
        }

        return $targetValue;
    }
}
