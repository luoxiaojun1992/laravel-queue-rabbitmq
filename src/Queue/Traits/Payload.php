<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Traits;

use Illuminate\Queue\InvalidPayloadException;

trait Payload
{
    /**
     * Create a payload string from the given job and data.
     *
     * @param  string|object  $job
     * @param  mixed   $data
     * @return string
     *
     * @throws \Illuminate\Queue\InvalidPayloadException
     */
    protected function createPayload($job, $data = '')
    {
        if (!is_object($job)) {
            throw new InvalidPayloadException(
                'Job must be a object'
            );
        }

        $payload = $data ?: $job->data;

        //Format payload array
        if (is_array($payload)) {
            $payload = json_encode($payload);

            if (JSON_ERROR_NONE !== json_last_error()) {
                throw new InvalidPayloadException(
                    'Unable to JSON encode payload. Error code: ' . json_last_error()
                );
            }
        }

        return $payload;
    }
}
