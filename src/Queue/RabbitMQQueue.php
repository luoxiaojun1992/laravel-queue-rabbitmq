<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use Carbon\Carbon;
use DateTimeInterface;
use DateInterval;
use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Database\DetectsLostConnections;
use Illuminate\Queue\InvalidPayloadException;
use Illuminate\Queue\Queue;
use Illuminate\Support\Str;
use Interop\Amqp\AmqpQueue;
use Interop\Amqp\AmqpTopic;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\AmqpMessage;
use Interop\Amqp\Impl\AmqpBind;
use Psr\Log\LoggerInterface;
use RuntimeException;
use Throwable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Connectors\RabbitMQConnector;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Traits\Payload;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Traits\QueueJobMapping;

class RabbitMQQueue extends Queue implements QueueContract
{
    use DetectsLostConnections;
    use Payload;
    use QueueJobMapping;

    protected $sleepOnError;

    protected $queueName;
    protected $queueOptions;
    protected $exchangeOptions;
    protected $producerOptions;
    protected $consumerOptions;
    protected $pQueueTopicExMapping;
    protected $cQueueTopicExMapping;

    protected $declaredExchanges = [];
    protected $declaredQueues = [];
    protected $binded = [];
    protected $exchanges = [];
    protected $queues = [];
    protected $consumers = [];
    protected $producer;

    /**
     * @var AmqpContext
     */
    protected $context;
    protected $correlationId;

    protected $config;

    public function __construct(AmqpContext $context, array $config)
    {
        $this->config = $config;

        $this->context = $context;

        $this->queueName = $config['queue'] ?? $config['options']['queue']['name'];
        $this->queueOptions = $config['options']['queue'];
        $this->queueOptions['arguments'] = isset($this->queueOptions['arguments']) ?
            json_decode($this->queueOptions['arguments'], true) : [];

        $this->exchangeOptions = $config['options']['exchange'];
        $this->exchangeOptions['arguments'] = isset($this->exchangeOptions['arguments']) ?
            json_decode($this->exchangeOptions['arguments'], true) : [];

        $this->producerOptions = $config['options']['producer'] ?? [];
        $this->consumerOptions = $config['options']['consumer'] ?? [];
        $this->pQueueTopicExMapping = $this->producerOptions['queueTopicExchangeMapping'] ?? [];
        $this->cQueueTopicExMapping = $this->consumerOptions['queueTopicExchangeMapping'] ?? [];

        $this->sleepOnError = $config['sleep_on_error'] ?? 5;
    }

    /** {@inheritdoc} */
    public function size($queueName = null): int
    {
        /** @var AmqpQueue $queue */
        $queue = $this->declareForPop($queueName);

        return $this->context->declareQueue($queue);
    }

    /**
     * {@inheritdoc}
     *
     * @param object|string $job
     * @param string $data
     * @param null $queue
     * @return mixed|string|null
     * @throws Throwable
     */
    public function push($job, $data = '', $queue = null)
    {
        if (!is_object($job)) {
            throw new InvalidPayloadException(
                'Job must be a object'
            );
        }

        $options = [
            'routing_key' => $job->topic ?? null,
            'exchange_name' => $job->exchange ?? null,
        ];

        return $this->pushRaw($this->createPayload($job, $data), $queue ?: $job->queue, $options);
    }

    /**
     * {@inheritdoc}
     *
     * @param string $payload
     * @param null $queueName
     * @param array $options
     * @return mixed|string|null
     * @throws Throwable
     */
    public function pushRaw($payload, $queueName = null, array $options = [])
    {
        //Wrapped with aggregation queue
        $queueName = $this->getQueueName($queueName);
        if (isset($this->producerOptions['aggQueue'][$queueName])) {
            $payload = json_encode(['queue' => $queueName, 'msg' => is_array($payload) ? json_encode($payload) : $payload]);
            $queueName = $this->producerOptions['aggQueue'][$queueName];
        }

        $pushRawFunc = function () use ($payload, $queueName, $options) {
            try {
                /** @var AmqpMessage $message */
                $message = $this->context->createMessage($payload);

                $message->setCorrelationId($this->getCorrelationId());
                $message->setContentType('application/json');
                $message->setDeliveryMode(AmqpMessage::DELIVERY_MODE_PERSISTENT);

                if (isset($options['routing_key'])) {
                    $message->setRoutingKey($this->getRoutingKeyWithPrefix($options['routing_key']));
                } else {
                    $routingKey = $this->formatDefaultRoutingKey($queueName);

                    if (isset($this->pQueueTopicExMapping[$queueName])) {
                        $topicExMapping = $this->pQueueTopicExMapping[$queueName];

                        if (isset($topicExMapping['topic'])) {
                            $routingKey = $topicExMapping['topic'];
                        }
                    }

                    $message->setRoutingKey($this->getRoutingKeyWithPrefix($routingKey));
                }

                if (isset($options['priority'])) {
                    $message->setPriority($options['priority']);
                }

                if (isset($options['expiration'])) {
                    $message->setExpiration($options['expiration']);
                }

                if (isset($options['delivery_tag'])) {
                    $message->setDeliveryTag($options['delivery_tag']);
                }

                if (isset($options['consumer_tag'])) {
                    $message->setConsumerTag($options['consumer_tag']);
                }

                if (isset($options['headers'])) {
                    $message->setHeaders($options['headers']);
                }

                if (isset($options['properties'])) {
                    $message->setProperties($options['properties']);
                }

                if (isset($options['attempts'])) {
                    $message->setProperty(RabbitMQJob::ATTEMPT_COUNT_HEADERS_KEY, $options['attempts']);
                }

                if (isset($options['exchange_name'])) {
                    $exchangeName = $options['exchange_name'];
                } else {
                    $exchangeName = null;

                    if (isset($this->pQueueTopicExMapping[$queueName])) {
                        $topicExMapping = $this->pQueueTopicExMapping[$queueName];

                        if (isset($topicExMapping['exchange'])) {
                            $exchangeName = $topicExMapping['exchange'];
                        }
                    }

                    $exchangeName = $exchangeName ?? ($this->exchangeOptions['name'] ?: $this->formatDefaultExchangeName($queueName));
                }

                if (isset($this->producer)) {
                    $producer = $this->producer;
                } else {
                    $producer = $this->producer = $this->context->createProducer();
                }

                if (isset($options['delay']) && $options['delay'] > 0) {
                    $message->setProperty('delay_to', Carbon::now()->addSeconds($options['delay'])->getTimestamp());

                    $delayMillisecond = $options['delay'] * 1000;

                    //For Aliyun
                    if (is_null($this->config['delay_strategy'])) {
                        $maxDelayMillisecond = $this->config['max_delay'];
                        if ($delayMillisecond > $maxDelayMillisecond) {
                            $delayMillisecond = $maxDelayMillisecond;
                            $exchangeName .= '-delay';
                        }

                        $message->setProperty('delay', $delayMillisecond);
                    } else {
                        $producer->setDeliveryDelay($delayMillisecond);
                    }
                }

                /** @var AmqpTopic $topic */
                $topic = $this->declareForPush($exchangeName);

                $producer->send($topic, $message);

                return $message->getCorrelationId();
            } catch (Throwable $exception) {
                $this->reportConnectionError('pushRaw', $exception);
            }

            return null;
        };

        try {
            return call_user_func($pushRawFunc);
        } catch (Throwable $exception) {
            if ($this->producerOptions['retry_on_connect_error'] ?? false) {
                return $this->tryAgainIfLostConnection($pushRawFunc, $exception);
            }

            throw $exception;
        }
    }

    /**
     * {@inheritdoc}
     *
     * @param DateInterval|DateTimeInterface|int $delay
     * @param object|string $job
     * @param string $data
     * @param null $queue
     * @return mixed|string|null
     * @throws Throwable
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        if (!is_object($job)) {
            throw new InvalidPayloadException(
                'Job must be a object'
            );
        }

        $options = [
            'delay' => $this->secondsUntil($delay),
            'routing_key' => $job->topic ?? null,
            'exchange_name' => $job->exchange ?? null,
        ];

        return $this->pushRaw($this->createPayload($job, $data), $queue ?: $job->queue, $options);
    }

    /**
     * {@inheritdoc}
     *
     * @param null $queueName
     * @param bool $once
     * @return Job|RabbitMQJob|null
     * @throws Throwable
     */
    public function pop($queueName = null, $once = false)
    {
        $queueName = $this->getQueueName($queueName);
        $popFunc = function () use ($queueName) {
            try {
                /** @var AmqpQueue $queue */
                $queue = $this->declareForPop($queueName);

                if (isset($this->consumers[$queueName])) {
                    $consumer = $this->consumers[$queueName];
                } else {
                    $consumer = $this->context->createConsumer($queue);
                    if (count($this->consumers) < 20) {
                        $this->consumers[$queueName] = $consumer;
                    }
                }

                if ($message = $consumer->receiveNoWait()) {
                    if (is_null($this->config['delay_strategy'])) {
                        if (!is_null($delayTo = $message->getProperty('delay_to'))) {
                            $remainDelay = intval($delayTo) - time();
                            if ($remainDelay > 0) {
                                $consumer->acknowledge($message);

                                $exchangeName = $message->getExchangeName();
                                if ($exchangeName) {
                                    //Remove delay suffix
                                    $exchangeName = substr($exchangeName, 0, -6);
                                    if ($exchangeNamePrefix = $this->getExchangeNamePrefix()) {
                                        $exchangeName = substr($exchangeName, strlen($exchangeNamePrefix) + 1);
                                    }
                                }

                                $routingKey = $message->getRoutingKey();
                                if ($routingKey) {
                                    if ($routingKeyPrefix = $this->getRoutingKeyPrefix()) {
                                        $routingKey = substr($routingKey, strlen($routingKeyPrefix) + 1);
                                    }
                                }

                                $job = new \VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\Job(
                                    $message->getBody(),
                                    $queueName,
                                    $routingKey,
                                    $exchangeName
                                );

                                $this->later($remainDelay, $job, $job->data, $job->queue);
                                return null;
                            }
                        }
                    }

                    return new RabbitMQJob($this->container, $this, $consumer, $message, $this->config);
                } else {
                    usleep(100000);
                }
            } catch (Throwable $exception) {
                $this->reportConnectionError('pop', $exception);
            }

            return null;
        };

        try {
            return call_user_func($popFunc);
        } catch (Throwable $exception) {
            return $this->tryAgainIfLostConnection($popFunc, $exception, $once);
        }
    }

    /**
     * @param $callback
     * @param Throwable $exception
     * @param bool $once
     * @param int $wait
     * @return mixed
     * @throws Throwable
     */
    protected function tryAgainIfLostConnection($callback, Throwable $exception, $once = true, $wait = 5)
    {
        if ($this->causedByLostConnection($exception) ||
            Str::contains($exception->getMessage(), [
                'Socket error: could not connect to host.',
                'Library error: a socket error occurred',
                'Broken pipe or closed connection',
                'Error sending data. Connection timed out.',
            ])
        ) {
            if ((!($this->context instanceof \Enqueue\AmqpExt\AmqpContext)) && (!($this->context instanceof \Enqueue\AmqpLib\AmqpContext))) {
                throw $exception;
            }

            while (true) {
                try {
                    /** @var RabbitMQConnector $connector */
                    $connector = $this->container->make(RabbitMQConnector::class);
                    $this->context = $connector->connect($this->config)->getContext();
                    $this->consumers = [];
                    $this->producer = null;
                    return call_user_func($callback);
                } catch (Throwable $retryException) {
                    if ($once) {
                        throw $retryException;
                    }

                    if (!$this->causedByLostConnection($retryException) &&
                        !Str::contains($retryException->getMessage(), ['Socket error: could not connect to host.', 'Library error: a socket error occurred'])
                    ) {
                        throw $retryException;
                    }
                }

                sleep($wait);
            }
        }

        throw $exception;
    }

    /**
     * Retrieves the correlation id, or a unique id.
     *
     * @return string
     */
    public function getCorrelationId(): string
    {
        return $this->correlationId ?: uniqid('', true);
    }

    /**
     * Sets the correlation id for a message to be published.
     *
     * @param string $id
     *
     * @return void
     */
    public function setCorrelationId(string $id): void
    {
        $this->correlationId = $id;
    }

    /**
     * @return AmqpContext
     */
    public function getContext(): AmqpContext
    {
        return $this->context;
    }

    /**
     * Create or fetch existed Exchange
     *
     * @param $exchangeName
     * @return AmqpTopic|mixed
     */
    protected function createExchange($exchangeName)
    {
        //Create or fetch existed Exchange
        if (isset($this->exchanges[$exchangeName])) {
            $topic = $this->exchanges[$exchangeName];
        } else {
            $topic = $this->context->createTopic($this->getExchangeNameWithPrefix($exchangeName));
            if (isset($this->exchangeOptions['optionsMapping'][$exchangeName]['type'])) {
                $topic->setType($this->exchangeOptions['optionsMapping'][$exchangeName]['type']);
            } else {
                $topic->setType($this->exchangeOptions['type']);
            }
            if (isset($this->exchangeOptions['optionsMapping'][$exchangeName]['arguments'])) {
                $topic->setArguments($this->exchangeOptions['optionsMapping'][$exchangeName]['arguments']);
            } else {
                $topic->setArguments($this->exchangeOptions['arguments']);
            }
            if ($this->exchangeOptions['passive'] ||
                (isset($this->exchangeOptions['optionsMapping'][$exchangeName]['passive']) &&
                    $this->exchangeOptions['optionsMapping'][$exchangeName]['passive'])
            ) {
                $topic->addFlag(AmqpTopic::FLAG_PASSIVE);
            }
            if ($this->exchangeOptions['durable'] ||
                (isset($this->exchangeOptions['optionsMapping'][$exchangeName]['durable']) &&
                    $this->exchangeOptions['optionsMapping'][$exchangeName]['durable'])
            ) {
                $topic->addFlag(AmqpTopic::FLAG_DURABLE);
            }
            if ($this->exchangeOptions['auto_delete'] ||
                (isset($this->exchangeOptions['optionsMapping'][$exchangeName]['auto_delete']) &&
                    $this->exchangeOptions['optionsMapping'][$exchangeName]['auto_delete'])
            ) {
                $topic->addFlag(AmqpTopic::FLAG_AUTODELETE);
            }

            if (count($this->exchanges) < 20) {
                $this->exchanges[$exchangeName] = $topic;
            }
        }

        if (!in_array($exchangeName, $this->declaredExchanges, true)) {
            if (isset($this->exchangeOptions['declareMapping'][$exchangeName])) {
                $isDeclareExchange = boolval($this->exchangeOptions['declareMapping'][$exchangeName]);
            } else {
                $isDeclareExchange = $this->exchangeOptions['declare'];
            }

            if ($isDeclareExchange) {
                $this->context->declareTopic($topic);

                $this->declaredExchanges[] = $exchangeName;
            }
        }

        return $topic;
    }

    /**
     * @param string $exchangeName
     * @return AmqpTopic|mixed
     */
    protected function declareForPush(string $exchangeName)
    {
        return $this->createExchange($exchangeName);
    }

    /**
     * @param $queueName
     * @return AmqpQueue|mixed
     */
    protected function declareForPop($queueName)
    {
        $queueName = $this->getQueueName($queueName);
        $queueNameWithPrefix = $this->getQueueNameWithPrefix($queueName);

        //Create or fetch existed Queue
        if (isset($this->queues[$queueName])) {
            $queue = $this->queues[$queueName];
        } else {
            $queue = $this->context->createQueue($queueNameWithPrefix);
            if (isset($this->queueOptions['optionsMapping'][$queueName]['arguments'])) {
                $queue->setArguments($this->queueOptions['optionsMapping'][$queueName]['arguments']);
            } else {
                $queue->setArguments($this->queueOptions['arguments']);
            }
            if ($this->queueOptions['passive'] ||
                (isset($this->queueOptions['optionsMapping'][$queueName]['passive']) &&
                    $this->queueOptions['optionsMapping'][$queueName]['passive'])
            ) {
                $queue->addFlag(AmqpQueue::FLAG_PASSIVE);
            }
            if ($this->queueOptions['durable'] ||
                (isset($this->queueOptions['optionsMapping'][$queueName]['durable']) &&
                    $this->queueOptions['optionsMapping'][$queueName]['durable'])
            ) {
                $queue->addFlag(AmqpQueue::FLAG_DURABLE);
            }
            if ($this->queueOptions['exclusive'] ||
                (isset($this->queueOptions['optionsMapping'][$queueName]['exclusive']) &&
                    $this->queueOptions['optionsMapping'][$queueName]['exclusive'])
            ) {
                $queue->addFlag(AmqpQueue::FLAG_EXCLUSIVE);
            }
            if ($this->queueOptions['auto_delete'] ||
                (isset($this->queueOptions['optionsMapping'][$queueName]['auto_delete']) &&
                    $this->queueOptions['optionsMapping'][$queueName]['auto_delete'])
            ) {
                $queue->addFlag(AmqpQueue::FLAG_AUTODELETE);
            }
            if (count($this->queues) < 20) {
                $this->queues[$queueName] = $queue;
            }
        }
        if (!in_array($queueName, $this->declaredQueues, true)) {
            if (isset($this->queueOptions['declareMapping'][$queueName])) {
                $isDeclareQueue = boolval($this->queueOptions['declareMapping'][$queueName]);
            } else {
                $isDeclareQueue = $this->queueOptions['declare'];
            }

            if ($isDeclareQueue) {
                $this->context->declareQueue($queue);

                $this->declaredQueues[] = $queueName;
            }
        }

        $topicExMapping = [];
        if (isset($this->cQueueTopicExMapping[$queueName])) {
            $topicExMapping = $this->cQueueTopicExMapping[$queueName];
        }
        if (count($topicExMapping) <= 0) {
            $topicExMapping[] = [];
        }
        foreach ($topicExMapping as $mapping) {
            $routingKey = $mapping['topic'] ?? $this->formatDefaultRoutingKey($queueName);
            if (!is_array($routingKey)) {
                $routingKey = [$routingKey];
            }
            $exchangeName = [$mapping['exchange'] ?? ($this->exchangeOptions['name'] ?: $this->formatDefaultExchangeName($queueName))];

            if (!empty($mapping['delay'])) {
                array_push($exchangeName, $exchangeName[0] . '-delay');
            }

            foreach ($exchangeName as $exName) {
                //Bind Queue To Exchange
                foreach ($routingKey as $key) {
                    if (!isset($this->binded[$queueName][$exName][$key])) {
                        if (isset($this->queueOptions['bindMapping'][$queueName][$exName][$key])) {
                            $isBindQueue = boolval($this->queueOptions['bindMapping'][$queueName][$exName][$key]);
                        } else {
                            $isBindQueue = $this->queueOptions['bind'];
                        }
                        if ($isBindQueue) {
                            $topic = $this->createExchange($exName);
                            $this->context->bind(new AmqpBind($queue, $topic, $this->getRoutingKeyWithPrefix($key)));
                            $this->binded[$queueName][$exName][$key] = true;
                        }
                    }
                }
            }
        }

        return $queue;
    }

    /**
     * @param null $queueName
     * @return mixed
     */
    protected function getQueueName($queueName = null)
    {
        return $queueName ?: $this->queueName;
    }

    /**
     * @return mixed|null
     */
    protected function getQueueNamePrefix()
    {
        return $this->config['queue_name_prefix'] ?? null;
    }

    /**
     * @param $queueName
     * @return string
     */
    protected function getQueueNameWithPrefix($queueName)
    {
        if ($queueNamePrefix = $this->getQueueNamePrefix()) {
            $queueName = $queueNamePrefix . '-' . $queueName;
        }

        return $queueName;
    }

    /**
     * @return mixed|null
     */
    protected function getExchangeNamePrefix()
    {
        return $this->exchangeOptions['exchange_name_prefix'] ?? null;
    }

    /**
     * @param $exchangeName
     * @return string
     */
    protected function getExchangeNameWithPrefix($exchangeName)
    {
        if ($exchangeNamePrefix = $this->getExchangeNamePrefix()) {
            $exchangeName = $exchangeNamePrefix . '-' . $exchangeName;
        }

        return $exchangeName;
    }

    /**
     * @return mixed|null
     */
    protected function getRoutingKeyPrefix()
    {
        return $this->exchangeOptions['routing_key_prefix'] ?? null;
    }

    /**
     * @param $routingKey
     * @return string
     */
    protected function getRoutingKeyWithPrefix($routingKey)
    {
        if ($routingKeyPrefix = $this->getRoutingKeyPrefix()) {
            $routingKey = $routingKeyPrefix . '.' . $routingKey;
        }

        return $routingKey;
    }

    /**
     * @param string $action
     * @param Throwable $e
     * @throws Exception
     */
    protected function reportConnectionError($action, Throwable $e)
    {
        /** @var LoggerInterface $logger */
        $logger = $this->container['log'];

        $logger->error('AMQP error while attempting '.$action.': '.$e->getMessage());

        // If it's set to false, throw an error rather than waiting
        if ($this->sleepOnError === false) {
            throw new RuntimeException('Error writing data to the connection with RabbitMQ', null, $e);
        }

        // Sleep so that we don't flood the log file
        sleep($this->sleepOnError);
    }

    /**
     * @param $routingKey
     * @return string
     */
    protected function formatDefaultRoutingKey($routingKey)
    {
        return strtoupper(str_replace('-', '_', $routingKey)) . '_key_routing';
    }

    /**
     * @param $exchangeName
     * @return string
     */
    protected function formatDefaultExchangeName($exchangeName)
    {
        return strtoupper(str_replace('-', '_', $exchangeName)) . '_exchange';
    }
}
