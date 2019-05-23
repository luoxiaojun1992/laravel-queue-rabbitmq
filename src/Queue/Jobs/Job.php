<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class Job implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public $data;

    public $topic; //string, default is queue name
    public $exchange; //string, default is global name or queue name

    /**
     * Create a new job instance.
     *
     * @param $data
     * @param null $queue
     * @param null $topic
     * @param null $exchange
     */
    public function __construct($data, $queue = null, $topic = null, $exchange = null)
    {
        $this->data = $data;
        $this->queue = $queue ?: $this->queue;
        $this->topic = $topic ?: $this->topic;
        $this->exchange = $exchange ?: $this->exchange;
    }
}
